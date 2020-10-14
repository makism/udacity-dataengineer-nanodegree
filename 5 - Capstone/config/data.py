""" Dataset variables

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>

import os
import config as cfg
import sys
sys.path.append("../common/")
sys.path.append("common/")
import aws_dwh


# Whether or not to fill in missing records/timeseries.
# As we are working with timeseries somem records are missing,
# i.e. observations for specific date; we can insert these
# missing records with empty and zero values.
# This will result in a vastly bigger dataset!
DATESET_FILLIN_DT=False

# Whether or not to impute missing values
DATASET_IMPUTE=True

# Whether or not to sample the datasets.
DATASET_SAMPLE=True

# The fraction of the original dataset to sample.
DATASET_SAMPLE_FRAC=0.05

# Store locally or to S3 buckets.
DATASET_STORE="local"

import sys
if sys.version_info[0] == 3 and sys.version_info[1] >= 7 :
    import enum
    class DatastoreLocation(enum.Enum):
        Local = 1
        S3 = 2

def dataset_wildfiles(pipeline="LoadAndSample"):
    """Returns the complete filename for the (initially preprocessed and/or sampled) dataset "Wildfires".
    
    Parameters
    ----------
    pipeline: string, default `LoadAndSample`
        If set to `LoadAndSample`, it returns the filename as produced from the "Load and Sample" pipeline, otherwise it returns the filename from the pipeline "ETL".
        
    Returns
    -------
    fqn_name: string
        A fully-qualified filename either for local or S3 usage.
    """
    fname = "wildfires.csv"
    if pipeline == "LoadAndSample":
        fname = "sample_wildfires.csv"
    
    dwh = aws_dwh.parse_dwh()
    
    if DATASET_STORE == "local":
        return f"{cfg.ARTIFACTS}/{fname}"
    else:
        return f"{dwh['s3']['bucket-1']['FQN']}/{fname}"
    
def dataset_air_quality(pipeline="LoadAndSample"):
    """Returns the complete filename for the (initially preprocessed and/or sampled) dataset "Air Quality".
    
    Parameters
    ----------
    pipeline: string, default `LoadAndSample`
        If set to `LoadAndSample`, it returns the filename as produced from the "Load and Sample" pipeline, otherwise it returns the filename from the pipeline "ETL".
        
    Returns
    -------
    fqn_name: string
        A fully-qualified filename either for local or S3 usage.
    """
    fname = "air_quality"
    if pipeline == "LoadAndSample":
        fname = "sample_air_quality"
    
    dwh = aws_dwh.parse_dwh()
    
    if DATASET_STORE == "local":
        return f"{cfg.ARTIFACTS}/{fname}"
    else:
        return f"{dwh['s3']['bucket-1']['FQN']}/{fname}"
    
def dataset_us_drougts(pipeline="LoadAndSample"):
    """Returns the complete filename for the (initially preprocessed and/or sampled) dataset "US Droughts".
    
    Parameters
    ----------
    pipeline: string, default `LoadAndSample`
        If set to `LoadAndSample`, it returns the filename as produced from the "Load and Sample" pipeline, otherwise it returns the filename from the pipeline "ETL".
        
    Returns
    -------
    fqn_name: string
        A fully-qualified filename either for local or S3 usage.
    """
    fname = "us_droughts"
    if pipeline == "LoadAndSample":
        fname = "sample_us_droughts"
    
    dwh = aws_dwh.parse_dwh()
    
    if DATASET_STORE == "local":
        return f"{cfg.ARTIFACTS}/{fname}"
    else:
        return f"{dwh['s3']['bucket-1']['FQN']}/{fname}"
    
def dataset_global_temps(pipeline="LoadAndSample"):
    """Returns the complete filename for the (initially preprocessed and/or sampled) dataset "Global Temperatures".
    
    Parameters
    ----------
    pipeline: string, default `LoadAndSample`
        If set to `LoadAndSample`, it returns the filename as produced from the "Load and Sample" pipeline, otherwise it returns the filename from the pipeline "ETL".
        
    Returns
    -------
    fqn_name: string
        A fully-qualified filename either for local or S3 usage.
    """
    fname = "global_temperatures"
    if pipeline == "LoadAndSample":
        fname = "sample_global_temperatures"
    
    dwh = aws_dwh.parse_dwh()
    
    if DATASET_STORE == "local":
        return f"{cfg.ARTIFACTS}/{fname}"
    else:
        return f"{dwh['s3']['bucket-1']['FQN']}/{fname}"