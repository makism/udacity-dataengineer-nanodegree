
# Description

The purpose of this project is to build the proper pipelines to ingest data from the given S3 bucket into our Redshift cluster.

Our DAG looks like the following.

```
                                                                - load_user_dimension_table -
                                                               /                             \
                   - stage_events_to_redshift -               -   load_song_dimension_table   -
                  /                            \             /                                 \
  start_operator -                              - songplays -                                   - run_quality_checks - end_operator
                  \                            /             \                                 /
                   - stage_songs_to_redshift  -               -   load_artist_dimension_table -
                                                               \                             /
                                                                - load_time_dimension_table -

```

## Staging

The main entrypoint of our pipeline. Here we ingest the JSON files from the S3 bucket, into the staging tables. It is worth noting that the S3 key is parameterized, so as we can ingest data for a specific year and month combination. We run two staging task in parallel.

## Fact Tables

The plugin `LoadFactOperator` handle the insertion of data into the facts tables. It is pretty similar to the `LoadDimensionOperator`, but with the extended parameter `append`.
If it is set to `False`, the given table will be `TRUCATE`d before loading new data.

## Dimension Tables

The plugin `LoadDimensionOperator` is used to load all the dimensional tables. The most important parameters are
`table`, and `sql_query`.

| Parameter | Description |
|-----------|-------------|
| table     | the table name in which the data will be loaded |
| sql_query | the sql query; reference to a class variable from `SqlQueries`. |

## Quality Check

The quality checks are run at the very end of the pipeline. In the task definition, we just define for which tables we want to run the checks. Consider the following snippet:
```python
run_quality_checks = DataQualityOperator(
    ...
    tables=["staging_songs", "staging_events", "songplays", "users", "songs", "artists", "time"]
)
```

Internally, the plugin, references another class, `SqlDataQualityQueries`, that defines the following structure for each table:

```python
checks = {
  "schema_name": {
    "test": "SELECT COUNT(*) FROM {} WHERE field IS NULL",
    "expected": 0
  },
}
```
Some check may define a "not_expected" operator!

So, we iterate over the given tables, and execute each of these simple checks.


# Extra files

Besides the default files found in the template workspace, the following ones were created for different purposes.

| file                          | description |
|-------------------------------|-------------|
| dags/create_schemas_dag.py    | A simple DAG to create the appropriate schemas, and if they exist just `TRUNCATE` them. |
| airflow_create_connections.py | Creates programmatically the connections in Airflow |
| aws_create_cluster.py         | Infrastructre as Code for AWS |
| logger.py                     | The standard Python logger |
| test_redshift.py              | A test script for Redshift |
| test_s3.py                    | A test script for S3       |
| plugins/helpers/sql_data_quality_queries.py | A simple class to hold queries and their expected results for each table. |
| plugins/helpers/sql_create_queries.py       | A class which holds the DDL for the schemas. |
