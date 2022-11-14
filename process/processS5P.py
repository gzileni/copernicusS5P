#!/usr/bin/python3

import os
import pathlib
from multiprocessing import Process, freeze_support
from re import sub

import dask
import dask_geopandas
import geopandas
import matplotlib.pyplot as plt
import nctoolkit as nc
import numpy as np
import shapely.geometry

from dask.distributed import Client
from dotenv import load_dotenv
from progress.spinner import Spinner
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

dotenv_path = pathlib.Path('../.env')
load_dotenv(dotenv_path=dotenv_path)

url = "postgresql://" + os.getenv('POSTGRES_USER') + ":" + os.getenv('POSTGRES_PASSWORD') + "@" + \
    os.getenv('PGHOST') + ":" + \
    str(os.getenv('PGPORT')) + "/" + os.getenv('POSTGRES_DB')
engine = create_engine(url)

# ------------------------------------
def process(dataset, path):
    
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        
        pathData = getPathDataset('/../data/process')

        spinner = Spinner('Processing ---> ' + dataset + ' ')
        spinner.next()
        
        chunksSize = 100
        chunks = {
            "time": chunksSize,
            "latitude": chunksSize,
            "longitude": chunksSize
        }
        
        try:
            dset = nc.open_data(path)
            #dset = xr.open_dataset(path,
            #                    engine="netcdf4",
            #                    group="PRODUCT",
            #                    cache=True,
            #                    inline_array=True,
            #                    chunks=chunks)
            print(dset.head(5))
            print('\nDim: ' + str(dset.nbytes * (2 ** -30)) + ' GB')
            spinner.next()
        except:
            os.remove(path)
            spinner.next()
            return False
        
        # -----------------------------------------------------
        # creare geo dataset from bbox
        print('\ncreating bbox dataframe ... ')
        bbox_coordinates = str(os.getenv('BBOX')).split(',')
        x = np.array(bbox_coordinates)
        y = x.astype(np.float64)
        p1 = shapely.geometry.box(*y, ccw=True)
        gp_bbox_poly = geopandas.GeoDataFrame(
            geometry=geopandas.GeoSeries([p1]), crs='EPSG:4326')
        dgp_bbox_poly = dask_geopandas.from_geopandas(
            gp_bbox_poly, chunksize=1000)
        spinner.next()
        
        # -----------------------------------------------------
        print('\nfiltering data ...')
        dset = dset.crop(lon=[7, 8], lat=[44, 45])
        dset = dset.where(dset.qa_value >= 0.75)
        spinner.next()
        
        # -----------------------------------------------------
        print('\ncreating dataframe ... ')
        # df = dset.to_dask_dataframe()
        df = dset.ds.to_dataframe()
        print('dataframe pandas: ' + df.head(5))
        spinner.next()

        # -----------------------------------------------------
        # create geo dask dataframe
        print('\ncreating geographic dataframe ... ')
        ddf = dask_geopandas.GeoDataFrame(dask_geopandas.points_from_xy(
            df, 'latitude', 'longitude')).set_crs('EPSG:4326')
        spinner.next()
        
        # -----------------------------------------------------
        # intersect geo dataframe 
        print('\nintersection dataframes ... ')
        ddf_intersect = dask_geopandas.GeoDataFrame.sjoin(
            ddf, dgp_bbox_poly).to_crs('EPSG:4326')
        spinner.next()
        
        if (len(ddf_intersect) > 0):
            
            # -----------------------------------------------------
            print('\ncreating parquet files ... ')
            path_parquet = os.path.join(pathData, dataset) + '.parquet'
            ddf_intersect.to_parquet(path_parquet)
            spinner.next()
            
            # -----------------------------------------------------
            print('\nsave dataframe data to parquet file ... ')
            df = geopandas.read_parquet(path_parquet).to_crs('EPSG:4326')
            spinner.next()
            
            # -----------------------------------------------------
            # updated postgis
            print('\nsave dataframe data to db ... ')
            df.to_postgis(os.getenv('POLLUTION').lower(),
                                    engine,
                                    if_exists="append",
                                    chunksize=10000)    
        else:
            # delete dataset
            print('\ndeleting dataset ...')
            os.remove(path)
        
        spinner.next()
        spinner.finish()
        return True
    
def to_snake_case(s):
    return '_'.join(
        sub('([A-Z][a-z]+)', r' \1',
            sub('([A-Z]+)', r' \1',
                s.replace('-', ' '))).split()).lower()

# get path dataset
def getPathDataset(root):
    path = None
    location = os.getenv('LOCATION')
    pollution = os.getenv('POLLUTION')        
    location = to_snake_case(location)
    currDir = str(os.getcwd())
    
    # create directory
    path = currDir + os.path.join(str(root))
    if (not os.path.isdir(path)):
        os.mkdir(path)
        
    path = os.path.join(path, location)
    if (not os.path.isdir(path)):
        os.mkdir(path)
        
    path = os.path.join(path, pollution)
    if (not os.path.isdir(path)):
        os.mkdir(path)
    
    return path

# browse all directory
def main():
    
    scheduler = str(os.getenv('DASK_SCHEDULER')) + \
        ':' + str(os.getenv('DASK_SCHEDULER_PORT'))

    client = Client(scheduler)
    client
    
    pathDataset = getPathDataset('/../datasets')
    
    for root, dirs, files in os.walk(pathDataset):
        for dataset in files:
            ncdDataSet = os.path.join(pathDataset, dataset)
            if not (process(dataset, ncdDataSet)):
                print(dataset + ' ERROR!')

if __name__ == '__main__':
    freeze_support()
    main()