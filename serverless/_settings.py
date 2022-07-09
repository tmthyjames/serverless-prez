"""Module for loading packaged/local settings and additional configuration."""
from pathlib import Path

from dynaconf import Dynaconf

from serverless.utils import get_project_root

SETTINGS_FILE = "settings.yaml"

# `envvar_prefix` = Export envvars with `export DYNACONF_FOO=bar`
# `settings_files` = Load these files in the order specified
# `load_dotenv` = Load variables from .env file
settings = Dynaconf(
    envvar_prefix="SERVERLESS",
    root_path=Path.cwd(),
    settings_files=[
        SETTINGS_FILE,
    ],
    load_dotenv=True,
    dotenv_path=Path.cwd().joinpath(".env"),
)

settings.root_path = get_project_root()
