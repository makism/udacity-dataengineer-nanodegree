""" Redshift Stage Queries

We have organized the different SQL queries in multiple clases, and
unified them under one class for readability and maintenance purposes.

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>


class RedshiftStage:
    """Stage statements."""

    Stage = {
        "stage_wildfires": """
            COPY public.stage_wildfires FROM 's3://{}/wildfires/{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            JSON 's3://{}/json_paths/wildfires.json' COMPUPDATE OFF
            REGION '{}';
        """,
        "stage_airquality": """
            COPY public.stage_airquality FROM 's3://{}/airquality/{}'
            ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}'
            JSON 's3://{}/json_paths/airquality.json' COMPUPDATE OFF
            REGION '{}';
        """,
        "stage_droughts": """
            COPY public.stage_droughts FROM 's3://{}/droughts/{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            JSON 's3://{}/json_paths/droughts.json' COMPUPDATE OFF
            REGION '{}';
        """,
        "stage_temperatures": """
            COPY public.stage_temperatures FROM 's3://{}/temperatures/{}'
            ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}'
            JSON 's3://{}/json_paths/temperatures.json' COMPUPDATE OFF
            REGION '{}';
        """
    }
