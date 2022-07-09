import os
import sys
from pathlib import Path
import shutil

import pandas as pd
import numpy as np

from serverless import settings
from serverless.utils import download_url


to_path = Path(
    settings.root_path/settings.data.raw_path/settings.etl.census.businesspatterns.raw_file
)

def get_county_business_patterns_data(to_path_raw, post_prep_filename):

    download_url(
        settings.etl.census.businesspatterns.url,
        str(to_path_raw)
    )

    shutil.unpack_archive(to_path_raw, to_path_raw.parent)

    prep_county_business_patterns_data(
        to_path_raw.parent/'cbp19co.txt', post_prep_filename
    )

def prep_county_business_patterns_data(from_path, to_filename):
    to_filename.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(from_path, dtype={'fipstate': str, 'fipscty': str})
    df.to_parquet(to_filename)


def main():
    get_county_business_patterns_data(
        to_path, settings.root_path/settings.data.processed_path/settings.etl.census.businesspatterns.processed_file
    )
