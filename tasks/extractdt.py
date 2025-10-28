"""ExtractDT."""

import json
import os

import pandas
from grib2sqlite import logger as sqlite_logger
from grib2sqlite import parse_grib_file

from deode.datetime_utils import as_datetime, as_timedelta
from deode.logs import LogDefaults, logger
from deode.tasks.base import Task
import subprocess
from datetime import datetime
import tarfile


class ExtractDT(Task):
    """Extract sqlite point files."""

    def __init__(self, config):
        """Construct ExtractDT object.
        Will normally write to the same SQLite file as model forecast.
        But "model_name" is set to "DT".

        Args:
            config (deode.ParsedConfig): Configuration

        Raises:
            FileNotFoundError: Required file not fount
        """
        Task.__init__(self, config, __class__.__name__)
        self.unix_group = self.platform.get_platform_value("unix_group")

        # path to sfcdata on disk

        # Get the times from config.toml
        self.basetime = as_datetime(self.config["general.times.basetime"])
        dt_date = self.basetime.strftime("%Y%m%d")

        self.minstep = 0
        self.maxstep = int(as_timedelta(config["general.times.forecast_range"]).total_seconds()//3600)
        self.steplist = [ str(i) for i in range(self.minstep,self.maxstep + 1) ]
        self.dt_path = self.platform.substitute(
            config["extract_dt.dt_grib_path"],
            basetime=self.basetime,
        )

        logger.info("RETRIEVAL DATE: {}", self.basetime.strftime("%Y%m%d"))
        logger.info("DT_PATH: {}", self.dt_path)

        self.archive = self.platform.get_system_value("archive")

        self.sqlite_path = self.platform.substitute(
            self.config["extractsqlite.sqlite_path"]
        )
        self.sqlite_template = self.platform.substitute(
            self.config["extractsqlite.sqlite_template"]
        )

        # Get & read param/station list (for deterministic case) from config toml file
        # Stop if any of them not found
        det_list = self.config["extractsqlite"]["parameter_list_default_det"]
        for entry in det_list:
            if "station_list_default.csv" in entry["location_file"]:
                self.stationfile_sfc = self.platform.substitute(entry["location_file"])
                paramfile_sfc = self.platform.substitute(entry["param_file"])
            elif "temp_list_default.csv" in entry["location_file"]:
                self.stationfile_ua = self.platform.substitute(entry["location_file"])
                paramfile_ua = self.platform.substitute(entry["param_file"])
        if not os.path.isfile(self.stationfile_ua):
            raise FileNotFoundError(f" missing {self.stationfile_ua}")
        logger.info("Station list ua: {}", self.stationfile_ua)
        if not os.path.isfile(self.stationfile_sfc):
            raise FileNotFoundError(f" missing {self.stationfile_sfc}")
        logger.info("Station list sfc: {}", self.stationfile_sfc)
        if not os.path.isfile(paramfile_ua):
            raise FileNotFoundError(f" missing {paramfile_ua}")
        logger.info("Parameter list ua: {}", paramfile_ua)
        if not os.path.isfile(paramfile_sfc):
            raise FileNotFoundError(f" missing {paramfile_sfc}")
        logger.info("Parameter list sfc: {}", paramfile_sfc)

        with open(paramfile_sfc) as pf_sfc:
            self.parameter_list_sfc = json.load(pf_sfc)
            pf_sfc.close()
        with open(paramfile_ua) as pf_ua:
            self.parameter_list_ua = json.load(pf_ua)
            pf_ua.close()
        self.output_settings = self.config["general.output_settings"]
        self.model_name = self.config["extractsqlite.sqlite_model_name"]

    def execute(self):
        """Execute ExtractSQLite on all files."""

        # Determine log file path
        log_file_name = self.config["extractsqlite"].get("log_file")
        log_file_path = os.path.join(self.sqlite_path, log_file_name) if log_file_name else None
        paramtypes=self.config["extract_dt.paramtypes"]

        for tag in paramtypes:
            flist = [f"{tag}_{i}.grib1" for i in self.steplist]
            # Choose parameter list based on tag
            if tag == "sfc":
                logger.info("reading sfc param list}")
                param_list = self.parameter_list_sfc
                station_list = pandas.read_csv(self.stationfile_sfc, skipinitialspace=True)
            elif tag == "ua":
                logger.info("reading sfc param list}")
                param_list = self.parameter_list_ua
                station_list = pandas.read_csv(self.stationfile_ua, skipinitialspace=True)
            else:
                logger.warning("Unknown tag: {}, skipping...", tag)
                continue
            for ff in flist:
                infile = os.path.join(self.dt_path, ff)
                # Log to standard logger
                logger.info("SQLITE EXTRACTION: {}", infile)
                # Append log message to the specified log file if defined
                # But first, ensure the directory structure exists
                os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
                if log_file_path:
                    with open(log_file_path, "a") as log_file:
                        log_file.write(f"SQLITE EXTRACTION: {infile}\n")
                
                if not os.path.isfile(infile):
                    logger.warning("File not found, skipping: {}", infile)
                    if log_file_path:
                        with open(log_file_path, "a") as log_file:
                            log_file.write(f"File not found, skipping: {infile}\n")
                    continue  # Skip to the next file
                
                loglevel = self.config.get("general.loglevel", LogDefaults.LEVEL).upper()
                sqlite_logger.setLevel(loglevel)
                parse_grib_file(
                    infile=infile,
                    param_list=param_list,
                    station_list=station_list,
                    sqlite_template=self.sqlite_path + "/" + self.sqlite_template,
                    model_name=self.model_name,
                    weights=None,
                )

        if self.model_name == "IFS":  # Extra block to download hlam's sqlite data for IFSENS
            # Extract YYYY and MM from basetime
            yyyy = self.basetime.strftime("%Y")
            mm = self.basetime.strftime("%m")
            logger.info("Start retrieving of IFSENS FCTABLES from hlam: Year {}, Month {}", yyyy, mm)
            # Build the extraction path by replacing IFS → IFSENS
            sqlite_extract_path = self.sqlite_path.replace(self.model_name, "IFSENS")
            logger.info("Output path: {}", sqlite_extract_path)
            os.makedirs(sqlite_extract_path, exist_ok=True)

            # Path in the EC filesystem
            remote_dir = f"ec:/hlam/harp_bologna/FCTABLE/IFSENS/{yyyy}/{mm}/"

            # 1) Get list of .tar.gz files via 'els'
            tar_files = []
            try:
                result = subprocess.run(
                    ["els", remote_dir],
                    check=True,
                    capture_output=True,
                    text=True
                )
                tar_files = [f for f in result.stdout.splitlines() if f.endswith(".tar.gz")]
            except subprocess.CalledProcessError as e:
                logger.warning("Failed to list files in %s: %s", remote_dir, e.stderr.strip())

            # 2) Download & uncompress each file (only if tar_files found)
            if tar_files:
                for tar_file in tar_files:
                    remote_file_path = os.path.join(remote_dir, tar_file)
                    local_file_path = os.path.join(sqlite_extract_path, tar_file)

                    # Download using 'ecp'
                    subprocess.run(
                        ["ecp", remote_file_path, local_file_path],
                        check=True
                    )

                    # Extract contents
                    with tarfile.open(local_file_path, "r:gz") as tar:
                        tar.extractall(path=sqlite_extract_path)

                    # Remove .tar.gz file
                    os.remove(local_file_path)
            else:
                logger.info("No .tar.gz files to download from %s. Skipping extraction.", remote_dir)


