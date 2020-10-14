""" Redshift Drop SQL Queries

We have organized the different SQL queries in multiple clases, and
unified them under one class for readability and maintenance purposes.

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>


class RedshiftDrop:
    """Drop table statements."""

    Drop = {
        "stage_wildfires": """
            DROP TABLE IF EXISTS public.stage_wildfires;
        """,
        "stage_airquality": """
            DROP TABLE IF EXISTS public.stage_airquality;
        """,
        "stage_temperatures": """
            DROP TABLE IF EXISTS public.stage_temperatures;
        """,
        "stage_droughts": """
            DROP TABLE IF EXISTS public.stage_droughts;
        """,
        "states_abbrv": """
            DROP TABLE IF EXISTS public.states_abbrv;
        """,
        "states_counties": """
            DROP TABLE IF EXISTS public.states_counties CASCADE;
        """,
        "fips": """
            DROP TABLE IF EXISTS public.fips CASCADE;
        """,
        "wildfires": """
            DROP TABLE IF EXISTS public.wildfires;
        """,
        "droughts": """
            DROP TABLE IF EXISTS public.droughts;
        """,
        "temperatures": """
            DROP TABLE IF EXISTS public.temperatures;
        """
    }
