"""ExtractDT."""

import json
import os

import pandas
from grib2sqlite import logger as sqlite_logger
from grib2sqlite import parse_grib_file

from deode.datetime_utils import as_datetime, as_timedelta
from deode.logs import LogDefaults, logger
from deode.tasks.base import Task


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
        self.stationfile = self.platform.substitute(self.config["extractsqlite.station_list"])
        if not os.path.isfile(self.stationfile):
            raise FileNotFoundError(f" missing {self.stationfile}")
        logger.info("Station list: {}", self.stationfile)
        paramfile = self.platform.substitute(self.config["extractsqlite.parameter_list"])
        if not os.path.isfile(paramfile):
            raise FileNotFoundError(f" missing {paramfile}")
        logger.info("Parameter list: {}", paramfile)
        with open(paramfile) as pf:
            self.parameter_list = json.load(pf)
            pf.close()
        self.output_settings = self.config["general.output_settings"]
        self.model_name = self.config["extractsqlite.sqlite_model_name"]

    def execute(self):
        """Execute ExtractSQLite on all files."""
        station_list = pandas.read_csv(self.stationfile, skipinitialspace=True)

        # Determine log file path
        log_file_name = self.config["extractsqlite"].get("log_file")
        log_file_path = os.path.join(self.sqlite_path, log_file_name) if log_file_name else None
        
        for tag in ["sfc", "ua"]:
            flist = [f"{tag}_{i}.grib1" for i in self.steplist]
            for ff in flist:
                infile = os.path.join(self.dt_path, ff)
                
                # Log to standard logger
                logger.info("SQLITE EXTRACTION: {}", infile)
                
                # Append log message to the specified log file if defined
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
                    param_list=self.parameter_list,
                    station_list=station_list,
                    sqlite_template=self.sqlite_path + "/" + self.sqlite_template,
                    model_name=self.model_name,
                    weights=None,
                )
