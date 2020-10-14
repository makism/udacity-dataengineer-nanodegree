""" Preprocessing functions

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>

import pyspark.sql.functions as fn
import pyspark.sql.types as t

def count_duplicates(df):
    """ A simple query to count duplicate rows in a dataframe.
    
    Parameters
    ----------
    df: spark.sql.Dataframe
        Input dataframe
    
    Returns
    -------
    total_duplicates: int
        The number of duplicate rows (if any).
    """
    total_duplicates = df.groupBy(df.columns)\
                        .count()\
                        .where(fn.col('count') > 1)\
                        .select(fn.sum('count').alias("total_duplicates"))\
                        .collect()[0]['total_duplicates']
    return total_duplicates


def missing_fields_perc(df, threshold=0.0):
    """ Find columns, and compute the percentage from all the rows with missing values (NaNs and NULLs).
    
    Parameters
    ----------
    df: spark.sql.Dataframe
        Input dataframe
        
    threshold: float or None
        A given threshold which is used to decide if a column is considered empty/missing.
    
    Returns
    -------
    cols: array-list
        A list containing the comlumns' names with missing values
        
    missing_df: spark.sql.Dataframe
        A dataframe with the column names and their percentage of missing values
    """
    num_fields = filter(lambda col:
                        True if isinstance(col.dataType, t.FloatType) or isinstance(col.dataType, t.IntegerType) or isinstance(col.dataType, t.DoubleType) 
                        else False,
                        df.schema
    )
    
    num_fields_names = list(map(lambda col: col.name, num_fields))

    amount_missing_df = df.select([
        (fn.count(fn.when(fn.isnan(c) | fn.col(c).isNull(), c)) / fn.count(fn.lit(1))).alias(f"{c}_perc_missing")
        for c in num_fields_names
    ])

    if threshold is not None:
        complete_cols = [k for k, v in amount_missing_df.collect()[0].asDict().items() if v <= threshold]
        amount_missing_df = amount_missing_df.drop(*complete_cols)
    else:
        complete_cols = [k for k, v in amount_missing_df.collect()[0].asDict().items()]
    
    return complete_cols, amount_missing_df