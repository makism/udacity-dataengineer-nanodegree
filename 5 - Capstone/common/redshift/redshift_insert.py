""" Redshift Insert Queries

We have organized the different SQL queries in multiple clases, and
unified them under one class for readability and maintenance purposes.

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>


class RedshiftInsert:
    """Insert statements."""

    Insert = {
        "fips1": """
            INSERT INTO public.fips (fips_code, fips_name)
            SELECT
                DISTINCT CAST(FIPS_CODE AS INTEGER),
                FIPS_NAME
            FROM
                public.stage_wildfires;
        """,
        "fips2": """
            INSERT INTO public.fips (fips_code, fips_name)
            SELECT
                DISTINCT CAST(fips AS INTEGER),
                CONCAT(CONCAT(COUNTY, '-'), STATE)
            FROM
                public.stage_droughts;
        """,
        "states_counties": """
            INSERT INTO public.states_counties (state, county)
            SELECT
                DISTINCT state,
                county
            FROM
                stage_wildfires;
        """,
        "temperatures": """
            INSERT INTO public.temperatures (dt, State, AverageTemperature)
            SELECT
                CAST(dt AS DATE),
                State,
                AverageTemperature_imputed
            FROM
                public.stage_temperatures;
        """,
        "wildfires": """
            INSERT INTO public.wildfires(
                OBJECTID, FOD_ID, FPA_ID, FIRE_YEAR, DISCOVERY_DOY, DISCOVERY_TIME, STAT_CAUSE_CODE, STAT_CAUSE_DESCR, CONT_DOY, CONT_TIME, FIRE_SIZE, FIRE_SIZE_CLASS, STATE_COUNTY, FIPS_ID, DISCOVERY_DATE, CONT_DATE
            )
            WITH
                FIPS_ID as (SELECT * FROM public.fips),
                STATES_COUNTIES_ID as (SELECT * FROM public.states_counties)
            SELECT 
                OBJECTID,
                FOD_ID,
                FPA_ID,
                CAST(FIRE_YEAR AS INTEGER),
                CAST(DISCOVERY_DOY AS INTEGER),
                f_my_cast(DISCOVERY_TIME), 
                CAST(STAT_CAUSE_CODE AS INTEGER),
                STAT_CAUSE_DESCR,
                f_my_cast(CONT_DOY), 
                f_my_cast(CONT_TIME),
                CAST(FIRE_SIZE AS FLOAT),
                FIRE_SIZE_CLASS,
                FIPS_ID.index,
                STATES_COUNTIES_ID.index,
                CAST(DISCOVERY_DATE_converted AS DATE),
                CAST(CONT_DATE_converted AS DATE)
            FROM
                public.stage_wildfires,
                FIPS_ID,
                STATES_COUNTIES_ID
            WHERE
                FIPS_ID.fips_code = CAST(stage_wildfires.FIPS_CODE AS INTEGER) AND 
                FIPS_ID.fips_name = stage_wildfires.FIPS_NAME AND
                STATES_COUNTIES_ID.state = stage_wildfires.state AND
                STATES_COUNTIES_ID.county = stage_wildfires.county;
        """,
        "droughts": """
            INSERT INTO public.droughts(
                releaseDate, FIPS_ID, STATE_COUNTY, None, D0, D1, D2, D3, D4, validStart, validEnd
            )
            WITH
                FIPS_ID as (SELECT * FROM public.fips),
                STATES_COUNTIES_ID as (SELECT * FROM public.states_counties)
            SELECT
                CAST(releaseDate AS DATE),
                FIPS_ID.index,
                STATES_COUNTIES_ID.index,
                NONE,
                D0,
                D1,
                D2,
                D3,
                D4,
                CAST(validStart AS DATE),
                CAST(validEnd AS DATE)
            FROM
                public.stage_droughts,
                FIPS_ID,
                STATES_COUNTIES_ID
            WHERE
                FIPS_ID.fips_code = CAST(stage_droughts.fips AS INTEGER) AND 
                FIPS_ID.fips_name = CONCAT(CONCAT(stage_droughts.COUNTY, '-'), stage_droughts.STATE) AND
                STATES_COUNTIES_ID.state = stage_droughts.state AND
                STATES_COUNTIES_ID.county = stage_droughts.county_cleaned;
        """,
        "airquality": """
            -- we need to resolve the "State" column from staging table "stage_airquality"
            -- into an abbreviation. that's where the table "states_abbrv" comes in!
            INSERT INTO public.airquality (
                STATE_COUNTY, city_name, aqi, date_of_last_change
            )
            WITH
                STATES_ABBRV AS (SELECT * FROM public.states_abbrv),
                STATES_COUNTIES_ID AS (SELECT * FROM public.states_counties)
            SELECT
              STATES_COUNTIES_ID.index,
              public.stage_airquality.city_name,
              public.stage_airquality.aqi,
              CAST(public.stage_airquality.date_of_last_change AS DATE)
            FROM
                public.stage_airquality,
                STATES_COUNTIES_ID,
                STATES_ABBRV
            WHERE
                STATES_ABBRV.state = public.stage_airquality.state_name AND
                STATES_COUNTIES_ID.state = 	STATES_ABBRV.abbr  AND
                STATES_COUNTIES_ID.county = public.stage_airquality.county_name;
        """,
        "states_abbrv": """
            INSERT INTO public.states_abbrv (state, abbr) VALUES
                ('Alabama', 'AL'),
                ('Alaska', 'AK'),
                ('Arizona', 'AZ'),
                ('Arkansas', 'AR'),
                ('California', 'CA'),
                ('Colorado', 'CO'),
                ('Connecticut', 'CT'),
                ('Delaware', 'DE'),
                ('Florida', 'FL'),
                ('Georgia', 'GA'),
                ('Hawaii', 'HI'),
                ('Idaho', 'ID'),
                ('Illinois', 'IL'),
                ('Indiana', 'IN'),
                ('Iowa', 'IA'),
                ('Kansas', 'KS'),
                ('Kentucky', 'KY'),
                ('Louisiana', 'LA'),
                ('Maine', 'ME'),
                ('Maryland', 'MD'),
                ('Massachusetts', 'MA'),
                ('Michigan', 'MI'),
                ('Minnesota', 'MN'),
                ('Mississippi', 'MS'),
                ('Missouri', 'MO'),
                ('Montana', 'MT'),
                ('Nebraska', 'NE'),
                ('Nevada', 'NV'),
                ('New Hampshire', 'NH'),
                ('New Jersey', 'NJ'),
                ('New Mexico', 'NM'),
                ('New York', 'NY'),
                ('North Carolina', 'NC'),
                ('North Dakota', 'ND'),
                ('Ohio', 'OH'),
                ('Oklahoma', 'OK'),
                ('Oregon', 'OR'),
                ('Pennsylvania', 'PA'),
                ('Rhode Island', 'RI'),
                ('South Carolina', 'SC'),
                ('South Dakota', 'SD'),
                ('Tennessee', 'TN'),
                ('Texas', 'TX'),
                ('Utah', 'UT'),
                ('Vermont', 'VT'),
                ('Virginia', 'VA'),
                ('Washington', 'WA'),
                ('West Virginia', 'WV'),
                ('Wisconsin', 'WI'),
                ('Wyoming', 'WY'),
                ('District of  Columbia', 'DC'),
                ('Marshall Islands', 'MH');
        """,
        "annual_reports": """
            INSERT INTO public.annual_reports (state, year, aqi, temperature, wildfires, droughts)
            WITH states_counties_index AS 
            (
                SELECT
                    * 
                FROM
                    states_counties 
            )
            ,
            states_abbrv AS 
            (
                SELECT
                    * 
                FROM
                    states_abbrv 
            )
            ,
            yearly_aqi AS
            (
                SELECT
                    SC_IDX.state,
                    DATE_PART_YEAR(AQI.date_of_last_change) AS year,
                    AVG(AQI.aqi) AS AVG_AQ 
                FROM
                    airquality AS AQI,
                    states_counties_index AS SC_IDX 
                WHERE
                    SC_IDX.index = AQI.STATE_COUNTY 
                GROUP BY
                    SC_IDX.state,
                    year 
            )
            ,
            yearly_temperatures AS 
            (
                SELECT
                    states_abbrv.abbr AS state_abbreviated,
                    DATE_PART_YEAR(dt) AS year,
                    AVG(averagetemperature) AS avg_temp 
                FROM
                    temperatures,
                    states_abbrv 
                WHERE
                    states_abbrv.state = temperatures.state 
                GROUP BY
                    state_abbreviated,
                    year 
                ORDER BY
                    state_abbreviated,
                    year 
            )
            ,
            yearly_wildfires AS 
            (
                SELECT
                    SC_IDX.state AS state,
                    FIRE_YEAR,
                    COUNT(*) AS total 
                FROM
                    wildfires AS WF,
                    states_counties_index AS SC_IDX 
                WHERE
                    SC_IDX.index = WF.STATE_COUNTY 
                GROUP BY
                    SC_IDX.state,
                    FIRE_YEAR 
                ORDER BY
                    SC_IDX.state ASC,
                    FIRE_YEAR ASC 
            )
            ,
            yearly_droughts AS 
            (
                SELECT
                    SC_IDX.state,
                    DATE_PART_YEAR(releasedate) AS year,
                    AVG(GREATEST(NONE, D0, D1, D2, D3, D4)) AS avg_drought_coeff 
                FROM
                    droughts,
                    states_counties_index AS SC_IDX 
                WHERE
                    SC_IDX.index = droughts.STATE_COUNTY 
                GROUP BY
                    SC_IDX.state,
                    year 
                ORDER BY
                    SC_IDX.state ASC,
                    year ASC 
            )
            SELECT
                yearly_aqi.state,
                yearly_aqi.year,
                yearly_aqi.avg_aq,
                yearly_temperatures.avg_temp,
                yearly_wildfires.total,
                yearly_droughts.avg_drought_coeff 
            FROM
                yearly_aqi 
                JOIN
                    yearly_temperatures 
                    ON yearly_aqi.state = yearly_temperatures.state_abbreviated 
                    AND yearly_aqi.year = yearly_temperatures.year 
                JOIN
                    yearly_wildfires 
                    ON yearly_aqi.state = yearly_wildfires.state 
                    AND yearly_aqi.year = yearly_wildfires.FIRE_YEAR 
                JOIN
                    yearly_droughts 
                    ON yearly_aqi.state = yearly_droughts.state 
                    AND yearly_aqi.year = yearly_droughts.year 
            ORDER BY
                yearly_aqi.state ASC,
                yearly_aqi.year ASC
        """
    }
