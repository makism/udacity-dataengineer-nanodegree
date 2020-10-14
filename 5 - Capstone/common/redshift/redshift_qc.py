""" Redshift Quality Checks

We have organized the different SQL queries in multiple clases, and
unified them under one class for readability and maintenance purposes.

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>


class RedshiftQualityChecks:
    """Queries for quality check."""

    QualityChecks = {
        "stage_wildfires": {
            "query": """
                SELECT COUNT(*) FROM stage_wildfires;
            """,
            "not_expected": 0
        },
        "stage_airquality": {
            "query": """
                SELECT COUNT(*) FROM stage_airquality;
            """,
            "not_expected": 0
        },
        "stage_temperatures": {
            "query": """
                SELECT COUNT(*) FROM stage_temperatures;
            """,
            "not_expected": 0
        },
        "stage_droughts": {
            "query": """
                SELECT COUNT(*) FROM stage_droughts;
            """,
            "not_expected": 0
        },
        "wildfires": {
            "query": """
                SELECT COUNT(*) FROM wildfires;
            """,
            "not_expected": 0
        },
        "temperatures": {
            "query": """
                SELECT COUNT(*) FROM fips;
            """,
            "not_expected": 0
        },
        "fips": {
            "query": """
                SELECT COUNT(*) FROM fips;
            """,
            "not_expected": 0
        },
        "states_abbrv": {
            "query": """
                SELECT DISTINCT COUNT(*) FROM states_abbrv;
            """,
            "expected": 52
        },
        "states_counties": {
            "query": """
                SELECT COUNT(*) FROM states_counties;
            """,
            "not_expected": 0
        },
        "droughts": {
            "query": """
                SELECT COUNT(*) FROM droughts;
            """,
            "not_expected": 0
        },
        "airquality": {
            "query": """
                SELECT COUNT(*) FROM airquality;
            """,
            "not_expected": 0
        },
        "annual_reports": {
            "query": """
                SELECT COUNT(*) FROM annual_reports;
            """,
            "not_expected": 0
        }
    }
