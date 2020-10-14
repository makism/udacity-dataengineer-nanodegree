""" Redshift SQL Queries for Error reporting

We have organized the different SQL queries in multiple clases, and
unified them under one class for readability and maintenance purposes.

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>


class RedshiftErrors:
    """SQL queries for error handling."""
    
    Errors = {
        "STL_LOAD_ERRORS": """
            SELECT
                *
            FROM
                stl_load_errors
            WHERE
                filename = 's3://{}/{}/{}';
        """
    }
