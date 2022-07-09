from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
import shutil
import json

############ TO IMPORT PROJECT PYTHON MODULES ############
# import sys
# sys.path.insert(0, '../src/')
# from src.data import data
# from src.data import get_data
# from src.pipeline.to_s3 import processed_files, transfer_to_s3
# from src.pipeline import Hera, sql_create_from_dataframe
# from src.helpers.states import get_state_enum


to_path = Path('../data/raw/tiger/mapping/ZIP_COUNTY_122021.xlsx')

def get_tiger_mapping_data(to_path_raw, post_prep_filename, unzip=True):

    data.download_url(
        'https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_122021.xlsx',
        str(to_path_raw)
    )
    if unzip:
        shutil.unpack_archive(to_path_raw, to_path_raw.parent)
    prep_tiger_mapping_data(to_path_raw.parent/'ZIP_COUNTY_122021.xlsx', post_prep_filename)

def prep_tiger_mapping_data(from_path, to_filename):
    df = pd.read_excel(from_path, dtype={'zip': str, 'county': str})
    df['statefp'] = df['usps_zip_pref_state'].str.lower()
    df['city'] = df['usps_zip_pref_city'].str.lower()
    df['statefps'] = df['county'].str.slice(0, 2)
    df['ctyfps'] = df['county'].str.slice(2, 5)
    df = df.drop(columns=['usps_zip_pref_city', 'usps_zip_pref_state'])
    df.to_json(to_filename, orient='records')
    data.partition_zipcode_to_county_mapping('../data/raw/tiger/mapping/data.json', ['statefp'], 'tiger', 'mapping')


def main():
    get_tiger_mapping_data(to_path, '../data/raw/tiger/mapping/data.json', unzip=False)
