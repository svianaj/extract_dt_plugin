"""RetrieveDt."""

import os
import shutil
from deode.datetime_utils import as_datetime, as_timedelta
from deode.logs import logger
from deode.os_utils import deodemakedirs
from deode.tasks.base import Task
from deode.tasks.batch import BatchJob
import numpy as np
import re
from eccodes import *

class RetrieveDT(Task):
    """RetrieveDT task."""

    def __init__(self, config):
        """Construct RetrieveDT task object.
        This tasks retrieves DT data. Not for LBC creation but for direct use.
        So we may want very different fields, like pressure level data.
        This may be via MARS or POLYTOPE.

        Args:
            config (deode.ParsedConfig): Configuration
        Raises:
            ValueError: No data for this date.
        """
        Task.__init__(self, config, __class__.__name__)

        self.method = self.config["extract_dt.method"] # "mars" or "polytope"
        self.unix_group = self.platform.get_platform_value("unix_group")

        # path to sfcdata on disk

        # Get the times from config.toml
        self.basetime = as_datetime(self.config["general.times.basetime"])
        self.minstep = 0
        self.maxstep = int(as_timedelta(config["general.times.forecast_range"]).total_seconds()//3600)
        self.steplist = [ str(i) for i in range(self.minstep,self.maxstep + 1) ]
        self.max_try = int(config["scheduler.ecfvars.ecf_tries"])
        self.tryno = int(os.environ.get("ECF_TRYNO"))
        self.continue_on_fail = config.get("extract_dt.continue_on_fail", False)

        self.dt_path = self.platform.substitute(
            config["extract_dt.dt_grib_path"],
            basetime=self.basetime,
        )
        logger.info("DT PATH: {}", self.dt_path)
        logger.info("MIN/MAX STEP: {} {}", self.minstep, self.maxstep)
        logger.info("BASETIME: {}", self.basetime)

    def create_request(self, tag = "sfc"):
        allsteps = "/".join(self.steplist)
        method = self.config["extract_dt.method"]
        request = {
                "type":"FC",
                "step":allsteps,
                "stream":"OPER",
                "time":"00",
                "date":self.basetime.strftime("%Y%m%d"),
                "target":f"\"{tag}_[STEP].grib1\"",
                "process":"LOCAL",
        }
        if method == "mars":
            request["class"] = self.config["extract_dt.class_mars"]
            request["expver"] = self.config["extract_dt.expver_mars"]
        elif method == "polytope":
            request["dataset"] = "extremes-dt"
            request["class"] = self.config["extract_dt.class_polytope"]
            request["expver"] = self.config["extract_dt.expver_polytope"]

        if tag == "sfc":
            request["param"] = self.config["extract_dt.param_sfc"]
            request["levtype"] = "SFC"
            request["grid"] = self.config["extract_dt.grid_sfc"]
        elif tag == "ua":
            request["param"] = self.config["extract_dt.param_ua"]
            request["levtype"] = self.config["extract_dt.levtype_ua"]
            request["levelist"] = self.config["extract_dt.levelist_ua"]
            request["grid"] = self.config["extract_dt.grid_ua"]

        return request

    def execute(self):
        """Run task.

        Define run sequence.

        Raises:
            RuntimeError: If there is an issue with the work folder.
        """

        if self.tryno >= self.max_try and self.continue_on_fail:
            logger.error("ECF_TRYNO = {}, ECF_TRIES = {}", self.tryno, self.max_try)
            logger.error("Max number of re-try exceeded. Skipping this day!")
            return

        if not os.path.exists(self.dt_path):
            deodemakedirs(self.dt_path, unixgroup=self.unix_group)

        tryno = int(os.environ.get("ECF_TRYNO"))
        paramtypes=self.config["extract_dt.paramtypes"]
        for tag in paramtypes:
            # TODO: check whether files exist
            request = self.create_request(tag)

            if self.method == "mars":
                logger.info("Sending request to MARS client")                
                self.doreq_mars(request, tag)
            else:
                logger.info("Sending request to polytope client")                       
                self.doreq_polytope(request, tag)

            # move files to "semi-permanent"
            flist = [ f"{tag}_{i}.grib1" for i in self.steplist ]

            for gf in flist:
                if not os.path.exists(gf):
                    raise RuntimeError(f"Expected file not found: {gf}")
                if os.path.getsize(gf) == 0:
                    raise RuntimeError(f"Retrieved file is empty: {gf}")    
                logger.info("MOVING {}", gf)
                shutil.move(gf, os.path.join(self.dt_path, gf))

            # After moving, add cumulative lightning field for surface files
            if tag == "sfc":
                self.add_cumulative_litota1(self.dt_path, flist)


    def add_cumulative_litota1(self, path, file_list):
        """
        Compute cumulative lightning density from sfc_*.grib1 files.

        Parameters
        ----------
        path : str
            Path where GRIB files are stored.
        file_list : list
            List of filenames (sfc_i.grib1), must include i=1..max_forecast_range.
        """

        # Sort files numerically by forecast step
        def extract_step(fname):
            m = re.search(r"sfc_(\d+)\.grib1", fname)
            return int(m.group(1)) if m else -1

        file_list_sorted = sorted(file_list, key=extract_step)
        logger.info("Processing {} GRIB files from path: {}", len(file_list_sorted), path)

        cumulative = None

        for idx, fname in enumerate(file_list_sorted, start=1):
            fpath = os.path.join(path, fname)
            tmp_out = fpath + ".tmp"

            logger.info("Reading file {}/{}: {}", idx, len(file_list_sorted), fname)

            with open(fpath, "rb") as fin, open(tmp_out, "wb") as fout:
                while True:
                    gid = codes_grib_new_from_file(fin)
                    if gid is None:
                        break

                    shortName = codes_get(gid, "shortName")
                    if shortName == "litota1":
                        values = codes_get_values(gid)
                        if cumulative is None:
                            cumulative = np.copy(values)
                            logger.info("Initialized cumulative field from {}", fname)
                        else:
                            cumulative += values
                            logger.info("Updated cumulative field with {}", fname)

                        # Make a new message with the cumulative values
                        new_gid = codes_clone(gid)
                        codes_set_values(new_gid, cumulative)

                        # Update the stepRange to reflect accumulation
                        step = extract_step(fname)
                        stepRange = f"0-{step}"
                        codes_set(new_gid, "step", step)        # step = forecast hour, e.g., 2, 3, 6
                        codes_set(new_gid, "shortName", "litoti")
                        codes_set(new_gid, "stepType", "accum")
                        # Force shortName to remain litota1
                        #codes_set(new_gid, "shortName", "litota1")
                        # Optionally, define the stepType as accumulation
                        #codes_set(new_gid, "stepType", "accum")

                        logger.info("Writing cumulative litota1 with stepRange {}", stepRange)

                        # Write the new field
                        codes_write(new_gid, fout)
                        codes_release(new_gid)

                    # Also write the original field
                    codes_write(gid, fout)
                    codes_release(gid)

            # Replace the original file with updated one
            os.replace(tmp_out, fpath)
            logger.info("Updated file {}", fname)

        logger.info("Finished processing all {} files.", len(file_list_sorted))

    def doreq_mars(self, request, tag):
        logger.info("MARS REQUEST: {}", request)
        self.write_mars_req(request, f"{tag}.req", "retrieve")
        mars_bin = self.get_binary("mars")
        import subprocess
        try:
         subprocess.run(["srun", f"{mars_bin}", f"{tag}.req"], check=True)
        except subprocess.CalledProcessError as e:
         print(f"Error: MARS request failed with exit code {e.returncode}")
         print(f"Command Output: {e.output}")
         # Handle the failure gracefully (e.g., retry, log error, etc.)

    def doreq_polytope(self, request, tag):
        logger.info("POLYTOPE REQUEST: {}", request)
        from polytope.api import Client
        client = Client(address="polytope.lumi.apps.dte.destination-earth.eu")
        # dt_collection = "destination-earth"  # DESP account
        dt_collection = "ecmwf-destination-earth"  # ECMWF account
        files = client.retrieve(dt_collection, request)
        # NOTE: this will retrieve global data in "raw" form.
        # And ALL IN ONE FILE ?!!!
        # we need earthkit to interpolate to station locations.

    @staticmethod
    def write_mars_req(request, filename, method):
        """Write a request for MARS.

        Args:
            request:    dict object
            filename:   request file name
            method: selected method, retrieve or read
        """

        with open(filename, "w") as f:
            f.write(str(method.upper()) + ",\n")
            keylist = list(request.keys())

            for key in keylist:
                row_str = f"  {key.upper()} = {request[key]}"
                # put a comma at the end of every line, except the last one
                if key != keylist[-1]:
                    row_str += ","
                f.write(row_str + "\n")

            f.close()
 
    def check_file_exists(self, steps, path, file_name):
        """Check if which mars file already exist."""
        base_list = []
        for step in steps:
            step1 = int(step)
            step_str = str(step1) if path == "" else f"{step1:02d}"
            mars_file_check = os.path.join(path, f"{file_name}+{step_str}")
            if not os.path.exists(mars_file_check):
                base_list.append(step)
                logger.info("Missing file:{}", mars_file_check)

        base = "/".join(base_list)
        return base

