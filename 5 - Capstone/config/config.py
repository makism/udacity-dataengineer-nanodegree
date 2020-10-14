""" Project variables; mostly related to directory setup.

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>

import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA = os.path.abspath(os.path.join(ROOT, "raw_data"))
ARTIFACTS = os.path.abspath(os.path.join(ROOT, "artifacts"))
IAC = os.path.abspath(os.path.join(ROOT, "IaC"))
TEMP = "/tmp/"
REDSHIFT_JSON_PATHS = os.path.abspath(os.path.join(ROOT, "json_paths"))

APP_DEV=False
