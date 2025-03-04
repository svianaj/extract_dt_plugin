"""DT extraction suites."""

from pathlib import Path

from ecflow import RepeatDate, Trigger

from deode.datetime_utils import as_datetime, as_timedelta
from deode.logs import logger
from deode.os_utils import deodemakedirs
from deode.suites.base import (
    EcflowSuiteFamily,
    EcflowSuiteTask,
    EcflowSuiteTrigger,
    EcflowSuiteTriggers,
    SuiteDefinition,
)


class DtExtractSuiteDefinition(SuiteDefinition):
    """Definition of DT extraction suite."""

    def __init__(
        self,
        config,
        dry_run=False,
    ):
        """Construct the definition.

        Args:
            config (deode.ParsedConfig): Configuration file
            dry_run (bool, optional): Dry run not using ecflow. Defaults to False.

        """
        SuiteDefinition.__init__(self, config, dry_run=dry_run)

        self.config = config
        self.name = config.get("general.case", "DT_extract")

        unix_group = self.platform.get_platform_value("unix_group")
        deodemakedirs(self.joboutdir, unixgroup=unix_group)
        # Set the input template path
        input_template = self.platform.substitute("@DEODE_HOME@/templates/ecflow/default.py")

        start_date = int(as_datetime(config["general.times.start"]).strftime("%Y%m%d"))
        end_date = int(as_datetime(config["general.times.end"]).strftime("%Y%m%d"))

        loop_date = "YMD"
        # We want retrieval to begin with a delay, to make sure MARS data is available.
        # Use _JULIAN for looking at differences > 1 day.
        # Internal ecflow time variables are "HHMM" integers.
        delay = as_timedelta(config["extract_dt.delay"])
        delay_time = 100*(delay.seconds // 3600) + (delay.seconds//60)%60
        time_trigger = (
                f"(:TIME ge {delay_time} AND :ECF_JULIAN - :{loop_date}_JULIAN ge {delay.days} )" +
                " OR " +
                f"(:ECF_JULIAN - :{loop_date}_JULIAN gt {delay.days})"
                )

        day_family = DailyLoopFamily(
            name = "DT_loop",
            parent = self.suite,
            ecf_files = self.ecf_files,
            input_template = input_template,
            task_settings = self.task_settings,
            config = config,
            start_date = start_date,
            end_date = end_date,
            loop_date = loop_date,
        )
        #we could add the trigger to the loop family or to the first task...
        #day_family.ecf_node.add_trigger(time_trigger)

        dt_data = EcflowSuiteTask(
            name = "RetrieveDT",
            parent = day_family,
            config = config,
            ecf_files = self.ecf_files,
            input_template = input_template,
            task_settings = self.task_settings,
        )
        # NOTE: Currently, the "deode" trigger class only accepts triggers
        # of the style "node == finished".
        # So a "time-based" trigger must be added differently.
        dt_data.ecf_node.add_trigger(time_trigger)

        dt_extract = EcflowSuiteTask(
            name = "ExtractDT",
            parent = day_family,
            config = config,
            ecf_files = self.ecf_files,
            input_template = input_template,
            task_settings = self.task_settings,
            trigger = dt_data,
        )

        # TODO: cleanup task for removing older DT grib files?


class DailyLoopFamily(EcflowSuiteFamily):
    """Class for a basic Ecflow family with a date loop."""
    def __init__(
        self,
        name,
        parent,
        ecf_files,
        input_template,
        task_settings,
        config,
        start_date,
        end_date,
        loop_date = "YMD",
        variables = None,
        ):
        """Class initialization."""
        EcflowSuiteFamily.__init__(
            self,
            name,
            parent,
            ecf_files,
        )
        self.ecf_node.add(RepeatDate(loop_date, start_date, end_date))
        var = {"BASETIME":self.date_basher(loop_date)}
        self.ecf_node += var

    @staticmethod 
    def date_basher(basename, time="00:00:00", micro="%"):
        """Export an ECFlow date to a correct ISO format using evaluated bash strings."""
        # TODO: add "time" argument(s)
        # This routine uses the fact that if the loop variable is e.g. YMD
        # ECFlow automatically provies YMD_YYYY, YMD_MM, YMD_DD macro's
        # BUT: the day and month variables may by single digit numbers
        #      so we need to use "printf" commands to format them correctly.
        # ALTERNATIVES:
        #   YYYY=$(echo {micro}{basename}{micro} | cut -c 1-4)" etc.
        #   f"$\{{micro}{basename}{micro}:0:4\}-\{{micro}{basename}{micro}:4:2\}-\{{micro}{basename}{micro}:6:2\}"
#        if micro == "%":
#            fmt = "%%02d"
#        else:
#            fmt = "%02d"
        result = (
             f"$(echo {micro}{basename}{micro} | cut -c 1-4)-" +
             f"$(echo {micro}{basename}{micro} | cut -c 5-6)-" +
             f"$(echo {micro}{basename}{micro} | cut -c 7-8)" +
#             f"{micro}{basename}_YYYY{micro}" + "-" +
#             f"$(printf {fmt} {micro}{basename}_MM{micro})" + "-" +
#             f"$(printf {fmt} {micro}{basename}_DD{micro})" +
             f"T{time}Z"
             )
        return result

