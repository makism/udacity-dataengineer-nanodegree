""" Redshift Create Queries

We have organized the different SQL queries in multiple clases, and
unified them under one class for readability and maintenance purposes.

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>


class RedshiftCreate:
    """Create table statements."""

    Create = {
        "stage_airquality": """
            CREATE TABLE IF NOT EXISTS public.stage_airquality (
                state_code          VARCHAR,
                county_code         VARCHAR,
                site_num            VARCHAR,
                parameter_code      INTEGER,
                poc                 INTEGER,
                latitude            FLOAT,
                longitude           FLOAT,
                datum               VARCHAR,
                parameter_name      VARCHAR,
                sample_duration     VARCHAR,
                pollutant_standard  VARCHAR, 
                date_local          VARCHAR,
                units_of_measure    VARCHAR,
                event_type          VARCHAR,
                observation_count   INTEGER,
                observation_percent INTEGER,
                arithmetic_mean     FLOAT,
                first_max_value     FLOAT,
                first_max_hour      INTEGER,
                aqi                 INTEGER,
                method_code         INTEGER,
                method_name         VARCHAR,
                local_site_name     VARCHAR,
                address             VARCHAR,
                state_name          VARCHAR,
                county_name         VARCHAR,
                city_name           VARCHAR,
                cbsa_name           VARCHAR,
                date_of_last_change VARCHAR,
                part_year           INTEGER,
                part_month          INTEGER
            );
        """,
        "stage_wildfires": """
            CREATE TABLE IF NOT EXISTS public.stage_wildfires (
                OBJECTID                 VARCHAR(256),
                FOD_ID                   VARCHAR(256),
                FPA_ID                   VARCHAR(256),
                FIRE_YEAR                VARCHAR(256),
                DISCOVERY_DOY            VARCHAR(256),
                DISCOVERY_TIME           VARCHAR(256),
                STAT_CAUSE_CODE          VARCHAR(256),
                STAT_CAUSE_DESCR         VARCHAR(256),
                CONT_DOY                 VARCHAR(256),
                CONT_TIME                VARCHAR(256),
                FIRE_SIZE                VARCHAR(256),
                FIRE_SIZE_CLASS          VARCHAR(256),
                STATE                    VARCHAR(256),
                COUNTY                   VARCHAR(256),
                FIPS_CODE                VARCHAR(256),
                FIPS_NAME                VARCHAR(256),
                DISCOVERY_DATE_converted VARCHAR(256),
                CONT_DATE_converted      VARCHAR(256),
                part_year                VARCHAR(256),
                part_month               VARCHAR(256)
            );
        """,
        "stage_droughts": """
            CREATE TABLE IF NOT EXISTS public.stage_droughts (
                releaseDate    VARCHAR,
                FIPS           VARCHAR,
                county         VARCHAR,
                state          VARCHAR,
                NONE           FLOAT,
                D0             FLOAT,
                D1             FLOAT,
                D2             FLOAT,
                D3             FLOAT,
                D4             FLOAT,
                validStart     VARCHAR,
                validEnd       VARCHAR,
                county_cleaned VARCHAR,
                part_year      INTEGER,
                part_month     INTEGER
            );
        """,
        "stage_temperatures": """
            CREATE TABLE IF NOT EXISTS public.stage_temperatures (
                dt                            VARCHAR,
                AverageTemperature            FLOAT,
                AverageTemperatureUncertainty FLOAT,
                State                         VARCHAR,
                AverageTemperature_imputed    FLOAT,
                part_year                     INTEGER,
                part_month                    INTEGER
            );
        """,
        "wildfires": """
            CREATE TABLE IF NOT EXISTS wildfires(
                OBJECTID          VARCHAR PRIMARY KEY NOT NULL,
                FOD_ID            VARCHAR,
                FPA_ID            VARCHAR,
                FIRE_YEAR         INTEGER,
                DISCOVERY_DOY     INTEGER,
                DISCOVERY_TIME    INTEGER,
                STAT_CAUSE_CODE   INTEGER,
                STAT_CAUSE_DESCR  VARCHAR,
                CONT_DOY          INTEGER,
                CONT_TIME         INTEGER,
                FIRE_SIZE         FLOAT,
                FIRE_SIZE_CLASS   VARCHAR(1),
                STATE_COUNTY      INTEGER REFERENCES public.states_counties(index),
                FIPS_ID           INTEGER REFERENCES public.fips(index),
                DISCOVERY_DATE    DATE,
                CONT_DATE         DATE
            );
        """,
        "states_abbrv": """
            CREATE TABLE IF NOT EXISTS public.states_abbrv (
                index  INTEGER IDENTITY(0,1) PRIMARY KEY NOT NULL,
                state  VARCHAR,
                abbr   VARCHAR(2)
            );
        """,
        "states_counties": """
            CREATE TABLE IF NOT EXISTS public.states_counties (
                index  INTEGER IDENTITY(0,1) PRIMARY KEY NOT NULL,
                state  VARCHAR(2),
                county VARCHAR
            );
        """,
        "fips": """
            CREATE TABLE IF NOT EXISTS fips (
                index     INTEGER IDENTITY(0,1) PRIMARY KEY NOT NULL,
                fips_code INTEGER,
                fips_name VARCHAR
            );
        """,
        "temperatures": """
            CREATE TABLE IF NOT EXISTS temperatures(
                    dt                 DATE,
                    State              VARCHAR,
                    AverageTemperature FLOAT
            );
        """,
        "droughts": """
            CREATE TABLE IF NOT EXISTS droughts(
                releaseDate  DATE,
                FIPS_ID      INTEGER REFERENCES public.fips(index),
                STATE_COUNTY INTEGER REFERENCES public.states_counties(index),
                None         FLOAT,
                D0           FLOAT,
                D1           FLOAT,
                D2           FLOAT,
                D3           FLOAT,
                D4           FLOAT,
                validStart   DATE,
                validEnd     DATE
            );
        """,
        "airquality": """
            CREATE TABLE IF NOT EXISTS airquality(
                STATE_COUNTY      INTEGER REFERENCES public.states_counties(index),
                city_name         VARCHAR,
                aqi                 INTEGER,
                date_of_last_change DATE
            );
        """,
        "annual_reports": """
            CREATE TABLE IF NOT EXISTS annual_reports (
                state       VARCHAR,
                year        INTEGER,
                aqi         INTEGER,
                temperature FLOAT,
                wildfires   INTEGER,
                droughts    FLOAT
            );
        """
    }

