""" Helper functions for AWS

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com

import tqdm
import os
import sys
import boto3
import json
import sys
sys.path.append("../config")
import config as cfg
import logger


def parse_dwh(dwh_json=None):
    """Load a JSON file containing AWS settings and credentials.
    
    Parameters
    ----------
    dwh_json: string
        A complete filename to the JSON file.
    
    Returns
    -------
    dwh: dict
        A dictionary containing the parse values from the JSON file.
    """
    if dwh_json is None:
        dwh_json = f"{cfg.IAC}/dwh.json"
    
    dwh = None

    try:
        with open(f"{dwh_json}", "r") as fp:
            dwh = json.load(fp)
    except Exception as err:
        print(err)
        
    return dwh


def upload_to_s3(source, dwh):
    """ Upload recursively a directory to a S3 bucket.
    
    Parameters
    ----------
    source: string
        Source directory
        
    dwh: dict
        A dictionary containing the AWS credentials and DWH configuration as generated from the function `parse_dwh`. 
    """
    log_handler = logger.get_logger(logger_name="upload_to_s3")
    
    client = boto3.client(
        's3',
        region_name=dwh["aws"]["region"],
        aws_access_key_id=dwh["aws"]["access_key_id"],
        aws_secret_access_key=dwh["aws"]["secret_access_key"]
    )
    
    s3_root = os.path.basename(source)
    
    for root, dirs, files in tqdm.tqdm(os.walk(source)):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, source)
            s3_path = os.path.join(f"{s3_root}/", relative_path)
            
            log_handler.info(f"Upload \"{local_path}\" to \"{s3_path}\".")
            
            try:
                client.upload_file(local_path, dwh['s3']['bucket-1']['name'], s3_path)
            except Exception as err:
                log_handler.info(err)
