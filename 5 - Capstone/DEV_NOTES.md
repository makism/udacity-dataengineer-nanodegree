Dev Notes
---------

# Apache Spark

- After consecutive writes directly in S3 (`s3a://`) using the Parquet format, Apache Spark throws errors (which can be traced back to AWS SDK) related to ACL and privileges. This doesn't make any sense because the first write calls work perfectly. A work around, that worked at least one time, was to make the S3 bucket completely public.

- When writting a dataframe into a json from Spark, it seems that Spark ignores completely the fields with nulls and nans. This is a huge problem when ingesting the jsons into Redshift. A quick way to fix it is to `df.na.fill("")`.


# Amazon Redshift and S3

- The Parquet support in Redshift does not work out of the box. I stumbled upon lots of errors related to CRC checksums.

- Parquest and partition keys when `COPY`ing complains; couldn't solve this issue.

- The quickest schema type for staging tables is by far the `VARCHAR`. Later, when filling the dimension and/or facts table do conversion/casting.

- I noticed that If a table's columns and a JSON's fields do not match (in order they appear), these fields will be skipped when ingesting.

- JSON paths are really needed for the jsons to be properly ingested!
