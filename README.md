# extract_dt plugin
Plugin interface to run SQLite extraction for the global Digital Twin (either via MARS or POLYTOPE).

# Description:
This plugin needs a config.toml file from a deode run as input. Relevant configuration of the verification paths, etc. must be updated in the 
file extract_dt_plugin.toml.
To create an extraction suite from a config.toml and the extract_dt_plugin.toml file, create a file -i.e. called configuration- in the Deode-Workflow home directory, with the following content:

> --config-file
>   /path/to/config.toml
>   /path/to/extract_dt_plugin.toml

Then create and launch the extraction suite with these commands: 

> poetry shell
> deode case ?configuration -o extract_dt_suite.toml
> deode start suite --config-file extract_dt_suite.tom

If everything went fine, a new suite will appear in your ecflow_ui,
named *EXTRACT_DT* with a family *DT_cycle* that loops over the and tasks to retrieve the DT files from MARS or polytope (*RetrieveDT*) and extract point data in SQLite format, suitable for harp verification (*ExtractDT*).

# Configuration:

Most of the mars (polytope) specific config are in the [extract_dt] section.

