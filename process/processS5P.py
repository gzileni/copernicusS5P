#!/usr/bin/python3

import sys
import pathlib
import os
import dask

import xarray as xr
import geopandas
import matplotlib.pyplot as plt
import glob
import pandas as pd

from re import sub
from dotenv import load_dotenv

import dask_geopandas

dotenv_path = pathlib.Path('../.env')
load_dotenv(dotenv_path=dotenv_path)

def processNETCD(dataset, path):
    pr_files = glob.glob(path + '/*')
    pr_files.sort()
    with dask.config.set(**{'array.slicing.split_large_chunks': False}):
        dset = xr.open_mfdataset(pr_files, engine="netcdf4",
                                group="PRODUCT",
                                decode_times=True,
                                decode_timedelta=True,
                                decode_coords=True,
                                parallel=True)
        
        df = dset.to_dask_dataframe()
        df = df.loc[df["qa_value"] >= 0.75]
        print(df)
        path_hdf = os.getenv('HDFS_PATH') + '/copernicus'
        dask.dataframe.to_parquet(
            df, path_hdf, append=True, engine='fastparquet', compression='gzip')
        print('finish.')
        
def process(dataset, path):
    dset = xr.open_dataset(path, 
                            engine="netcdf4",
                            group="PRODUCT",
                            cache=True,
                            inline_array=True)
    df = dset.to_dask_dataframe()
    df = df.loc[df["qa_value"] >= 0.75]
    
    ddf = df.set_geometry(
        dask_geopandas.points_from_xy(df, 'latitude', 'longitude')
    )
    
    print(ddf.head(5))
    path_parquet = path + '.parquet'
    ddf.to_parquet(path_parquet)
    
    print('finish.')
    # check integrity files
#    print(dataset + ' ---> reading rows ---> ' + str(len(datas)))
#    
#    if (datas is not None):
#        df = datas.to_dataframe()
        # select quality data qa_value >= 0.5
#        df = df.where(df.qa_value >= 0.5, inplace=True)
        
#        if (df is not None):
#            if (len(df) > 0):
#                print(df.head(5))
#                print('--- created geometries ---> ' + str(len(df)))
#                datag = geopandas.GeoSeries.from_xy(
#                        df.latitude, 
#                        df.longitude,
#                        crs="EPSG:4326") 
#                print(datag.head(5))
#                datag.to_file(dataset + '.geojson', driver='GeoJSON')
                
#                fig, ax2 = plt.subplots(figsize=(30, 18))
#                datag.plot(edgecolor='black', ax=ax2)
#                plt.xlim(650000, 750000)
#                plt.ylim(220000, 290000)
#                fig.savefig(dataset + '.eps', format='eps')
            # latitude, longitude, delta_time, time_utc, qa_value, formaldehyde_tropospheric_vertical_column, formaldehyde_tropospheric_vertical_column_precision

def to_snake_case(s):
    return '_'.join(
        sub('([A-Z][a-z]+)', r' \1',
            sub('([A-Z]+)', r' \1',
                s.replace('-', ' '))).split()).lower()

def getPathDataset():
    path = None
    l = len(sys.argv)
    location = ''
    pollution = ''
    if (l > 1):
        location = sys.argv[1]  
        pollution = str(sys.argv[2])
    else:
        location = os.getenv('LOCATION')
        pollution = os.getenv('POLLUTION')
        
    location = to_snake_case(location)
    currDir = str(os.getcwd())
    path = currDir + os.path.join(str('/../datasets'),
                        location,
                        pollution)
    return path

def browseDatasets(path):
    for root, dirs, files in os.walk(path):
        for dataset in files:
            process(dataset, os.path.join(path, dataset))
            # processNETCD(dataset, path)
                
def main():
    path = getPathDataset()
    print('path: ' + path)
    browseDatasets(path)
    
main()