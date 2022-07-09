import os
import shutil
import re
import json
from pathlib import Path

import requests
import pandas as pd
import geopandas as gpd

from serverless import settings
from serverless.utils import download_url


def prep_zipcode_to_county_mapping(inpath, outpath, partition):
    df = pd.read_excel(inpath, engine='openpyxl', dtype={'zip': str, 'county': str})
    df['statefp'] = df['county'].apply(lambda x: x[:2])
    df['countyfp'] = df['county'].apply(lambda x: x[2:])
    df['state'] = df['usps_zip_pref_state'].str.lower()
    df['city'] = df['usps_zip_pref_city'].str.lower()
    df.to_parquet(outpath, index=False, partition_cols=partition)

def shp_to_esri_enclosed_json_input(gdf, geom_type='esriGeometryPolygon'):

    shp_format = json.loads(gdf.to_json())
    esri_enclosed_json_inputs = {}
    esri_enclosed_json_inputs['displayFieldName'] = ''
    esri_enclosed_json_inputs['fieldAliases'] = {k:k for (k,v) in shp_format['features'][0]['properties'].items()}
    esri_enclosed_json_inputs['geometryType'] = geom_type
    esri_enclosed_json_inputs['spatialReference'] = {'wkid': 4269}

    if geom_type == 'esriGeometryPolygon':
        geom_key = 'rings'
    elif geom_type == 'esriGeometryPolyline':
        geom_key = 'paths'

    for jsonfeat in shp_format['features']:

        if jsonfeat['geometry']:

            if 'features' not in esri_enclosed_json_inputs:
                esri_enclosed_json_inputs['features'] = []

            jsonfeat['geometry'] = {
                geom_key: jsonfeat['geometry']['coordinates']
            }

            efeat = {
                'attributes': jsonfeat['properties']
            }

            efeat['geometry'] = jsonfeat['geometry']
            esri_enclosed_json_inputs['features'].append(efeat)

    return esri_enclosed_json_inputs

def partition_geo_data(path, partitions, processed_path, input_df=None, geom_type='esriGeometryPolygon'):

    gdf = input_df if input_df is not None else gpd.read_file(path).to_crs(4269).convert_dtypes()

    if partitions:

        grouped = gdf.groupby(partitions, as_index=False)

        for name, group in grouped:
            if isinstance(name, str):
                name = [name]
            filters = list(zip(partitions, name))
            fileid = ''.join([i[1] for i in filters])
            loc = '/'.join([f'{f[0]}={f[1]}'.lower() for f in filters])

            esri_format = shp_to_esri_enclosed_json_input(group, geom_type=geom_type)

            to_path = Path(processed_path/loc)
            to_path.mkdir(parents=True, exist_ok=True)

            with open(to_path/f'{fileid}.geojson', 'w') as file:
                json.dump(esri_format, file)

def tiger():
    zip_extension = '.zip'
    shp_extension = '.shp'
    excel_extensions = ['.xls', '.xlsx']

    for folder, subfolder in settings.etl.tiger.items():

        raw_path = settings.root_path/settings.data.raw_path/subfolder.file_path
        interim_path = settings.root_path/settings.data.interim_path/subfolder.file_path
        processed_path = settings.root_path/settings.data.processed_path/subfolder.file_path

        raw_path.mkdir(parents=True, exist_ok=True)
        processed_path.mkdir(parents=True, exist_ok=True)
        interim_path.mkdir(parents=True, exist_ok=True)

        url = subfolder.url

        for file in subfolder.files:
            raw_file_path = raw_path/file
            processed_file_path = processed_path/file
            interim_file_path = interim_path/file
            raw_file_exists = os.path.isfile(raw_file_path)
            existing_interim_files = [re.sub('\..*', '', i.name) for i in interim_path.iterdir()]
            processed_file_exists = os.path.isfile(processed_file_path)

            file_name_wo_zip = file.replace(zip_extension, '')

            if not raw_file_exists:
                print(f'DOWNLOADING: from {url+file} to {raw_file_path}')
                download_url(url+file, raw_file_path)
                if (zip_extension in file) and (file_name_wo_zip not in existing_interim_files):
                    print(f'UNZIPPING: from {raw_file_path} to {interim_file_path.parent}')
                    shutil.unpack_archive(raw_file_path, interim_file_path.parent)

                if raw_file_path.suffix == zip_extension:
                    interim_file = interim_file_path.parent/(raw_file_path.stem+shp_extension)
                    interim_file_exists = os.path.isfile(interim_file)
                    # if not interim_file_exists:
                    print(f'PARTITIONING: from {interim_file} to {processed_path}')
                    partition_geo_data(interim_file, subfolder.partition_by, processed_path)

            elif raw_file_path.suffix in excel_extensions:
                interim_file = interim_file_path.parent/'data.parquet'
                interim_file_exists = os.path.isfile(interim_file)
                if not interim_file_exists:
                    print(F'PREPPING: from {raw_file_path} to {interim_file}')
                    prep_zipcode_to_county_mapping(raw_file_path, processed_file_path.parent, subfolder.partition_by)

        print(f'FINISHED POPULATING RAW, INTERIM, AND PROCESSED DIRECTORIES for {processed_path}')

    return None

def main():
    tiger()
