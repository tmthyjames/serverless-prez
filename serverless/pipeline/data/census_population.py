import os
import sys
from pathlib import Path
import shutil

import pandas as pd

from serverless import settings
from serverless.utils import download_url


def get_population_data(post_prep_filename):

    population_20_url = settings.etl.census.population.url
    population_estimates_url = settings.etl.census.population.pop_estimates.url
    st_cty_ref_url = settings.etl.census.st_cty_ref.url

    pop20 = pd.read_excel(
        settings.etl.census.population.URL,
        header=4,
        nrows=3280,
        dtype={
            'Federal Information Processing Standards (FIPS) Code': str
        },
        usecols=[
            'Federal Information Processing Standards (FIPS) Code',
            'State',
            'Area name',
            'Rural-Urban Continuum Code 2013',
            'Population 1990',
            'Population 2000',
            'Population 2010',
            'Population 2020',
            'Population 2021'
        ]
    ).rename(
        columns={
            'Federal Information Processing Standards (FIPS) Code': 'FIPStxt',
            'Rural-Urban Continuum Code 2013': 'Rural_urban_continuum_code_2013'
        }
    )

    download_url(
        population_20_url,
        settings.root_path/settings.data.raw_path/settings.etl.census.population.raw_file
    )

    pop20.columns = [c.replace(' ', '_').replace('-', '_') for c in pop20.columns]

    pop_est = pd.read_excel(
        population_estimates_url,
        header=3, nrows=3142
    )

    download_url(
        population_estimates_url,
        settings.root_path/settings.data.raw_path/settings.etl.census.population.pop_estimates.raw_file
    )

    st_cty_ref = pd.read_csv(
        st_cty_ref_url,
        encoding = "ISO-8859-1", dtype={'cty': str, 'st': str}
    )

    download_url(
        st_cty_ref_url,
        settings.root_path/settings.data.raw_path/settings.etl.census.st_cty_ref.raw_file
    )

    pop_est['geoarea'] = pop_est['Unnamed: 0'].apply(lambda x: x.replace('.', ''))
    pop_est = pop_est.drop(columns=['Unnamed: 0'])
    pop_est = pop_est.merge(st_cty_ref, left_on='geoarea', right_on='ctyname').drop(columns=['geoarea'])
    pop_est.columns = [f'PopulationEstimate_{col}' if isinstance(col, int) else col for col in pop_est.columns]
    pop_est.columns = [col.replace(' ', '') for col in pop_est.columns]

    pop_est['FIPStxt'] = pop_est['st'] + pop_est['cty']
    pop_est = pop20[['FIPStxt', 'Population_2020', 'Rural_urban_continuum_code_2013']].merge(pop_est, on='FIPStxt')

    post_prep_filename.parent.mkdir(parents=True, exist_ok=True)
    pop_est.to_parquet(post_prep_filename)


def main():
    get_population_data(
        settings.root_path/settings.data.processed_path/settings.etl.census.population.processed_file
    )
