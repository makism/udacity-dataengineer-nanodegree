### Intro

In this directory you will find a few help python scripts and programs developed for Infrastructure-as-Code (IaC).

### Setup

Copy the template file `dwh.json.tpl` as `dwh.json` and fill in your AWS credentials and settings.
This step is imperative for the rest of the scripts and pipelines to operate properly!

### IaC

1. `aws_create_cluster.py`

Helper program to allocate all the required resources on AWS. These include:

* S3 buckets
* Redshift cluster
* EMR cluster

of course, through the provided command-line arguments these resources can be released.

Please run `python aws_create_cluster.py --help` for the complete list of available options.

2. `airflow_create_connections.py`

Creates the necessary connections in Airflow for AWS and Redshift. Please note that Airflow must be running for any changes to take place.

Just run `python airflow_create_connections.py` and refresh your Airflow admin page to see the changes.

3. `IaC_dev.ipynb`

A notebook used during development cycle; also, a playground for experimenting with AWS/boto3 :-)
