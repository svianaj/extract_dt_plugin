"""RetrieveDt."""

import os
import shutil

from deode.datetime_utils import as_datetime, as_timedelta
from deode.logs import logger
from deode.os_utils import deodemakedirs
from deode.tasks.base import Task
from deode.tasks.batch import BatchJob


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

        self.dt_path = self.platform.substitute(
            config["extract_dt.dt_grib_path"],
            basetime=self.basetime,
        )
        logger.info("DT PATH: {}", self.dt_path)
        logger.info("MIN/MAX STEP: {} {}", self.minstep, self.maxstep)

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

        if not os.path.exists(self.dt_path):
            deodemakedirs(self.dt_path, unixgroup=self.unix_group)
        
        for tag in [ "sfc", "ua" ]:
            # TODO: check whether files exist
            request = self.create_request(tag)

            if self.method == "mars":
                self.doreq_mars(request, tag)
            else:
                self.doreq_polytope(request, tag)

            # move files to "semi-permanent"
            flist = [ f"{tag}_{i}.grib1" for i in self.steplist ]
            for gf in flist:
                logger.info("MOVING {}", gf)
                shutil.move(gf, os.path.join(self.dt_path, gf))

    def doreq_mars(self, request, tag):
        logger.info("MARS REQUEST: {}", request)
        self.write_mars_req(request, f"{tag}.req", "retrieve")
        batch = BatchJob(os.environ, wrapper=self.wrapper)
        mars_bin = self.get_binary("mars")
        batch.run(f"{mars_bin} {tag}.req")

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

