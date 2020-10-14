""" Redshift User Defined Functions (UDFs)

We have organized the different SQL queries in multiple clases, and
unified them under one class for readability and maintenance purposes.

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>


class RedshiftUDFs:
    """Helper UDFs."""
    
    UDFs = {
        "f_my_cast": """
            -- Cast, if able the given column to integer.
            CREATE OR REPLACE FUNCTION f_my_cast (VARCHAR)
                RETURNS INTEGER
            VOLATILE
            AS $$
                SELECT
                    CASE  
                        WHEN $1 = '' THEN -1 
                        ELSE CAST(CAST($1 AS FLOAT) AS INTEGER)
            END $$ LANGUAGE sql;
        """
    }
