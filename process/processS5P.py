#!/usr/bin/python3

import sys
import pathlib
import os

import xarray as xr
import geopandas
import matplotlib.pyplot as plt
import shapely.geometry
import numpy as np

from re import sub
from dotenv import load_dotenv

import dask_geopandas

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

dotenv_path = pathlib.Path('../.env')
load_dotenv(dotenv_path=dotenv_path)

url = "postgresql://" + os.getenv('POSTGRES_USER') + ":" + os.getenv('POSTGRES_PASSWORD') + "@" + \
    os.getenv('PGHOST') + ":" + \
    str(os.getenv('PGPORT')) + "/" + os.getenv('POSTGRES_DB')
engine = create_engine(url)
  
def process(dataset, path):
    print('open dataset by ---> ' + path)
    dset = xr.open_dataset(path, 
                            engine="netcdf4",
                            group="PRODUCT",
                            cache=True,
                            inline_array=True)
    
    # -----------------------------------------------------
    # filter value 
    # print('filtering value by quality datas ...')

    print('creating dataframe ... ')
    df = dset.to_dask_dataframe()
    df = df.loc[df["qa_value"] >= 0.75]
    
    # -----------------------------------------------------
    # create geo dask dataframe
    print('creating geographic dataframe ... ')
    ddf = df.set_geometry(dask_geopandas.points_from_xy(
        df, 'latitude', 'longitude')).set_crs('EPSG:4326')
    
    # -----------------------------------------------------
    # creare geo dataset from bbox
    print('creating bbox dataframe ... ')
    bbox_coordinates = str(os.getenv('BBOX')).split(',')
    x = np.array(bbox_coordinates)
    y = x.astype(np.float64)
    p1 = shapely.geometry.box(*y, ccw=True)
    gp_bbox_poly = geopandas.GeoDataFrame(
    geometry=geopandas.GeoSeries([p1]), crs='EPSG:4326')
    dgp_bbox_poly = dask_geopandas.from_geopandas(gp_bbox_poly, chunksize=1000)
    
    # -----------------------------------------------------
    # intersect geo dataframe 
    print('intersection dataframes ... ')
    ddf_intersect = dask_geopandas.GeoDataFrame.sjoin(
        ddf, dgp_bbox_poly).to_crs('EPSG:4326')
    
    if (len(ddf_intersect) > 0):
        
        # save data to postgis, image and parquet file 
        print(ddf_intersect.head(5))
        
        print('creating parquet files ... ')
        path_parquet = os.path.join(pathData, dataset) + '.parquet'
        ddf_intersect.to_parquet(path_parquet)
        
        print('save dataframe data to db ... ')
        df = geopandas.read_parquet(path_parquet).to_crs('EPSG:4326')

        # updated postgis
        df.to_postgis(os.getenv('POLLUTION').lower(),
                                 engine,
                                 if_exists="append",
                                 chunksize=10000)
        
    else:
        # delete dataset
        print('deleting dataset ...')
        os.remove(path)
        
    print('finish.')
    
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
    for root, dirs, files in os.walk(pathDataset):
        for dataset in files:
            process(dataset, os.path.join(pathDataset, dataset))

pathDataset = getPathDataset('/../datasets')
pathData = getPathDataset('/../data/process')
main()