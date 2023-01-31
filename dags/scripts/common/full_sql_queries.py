# Create schema
stg_schema_create = """
    DROP SCHEMA IF EXISTS stg CASCADE;
    CREATE SCHEMA stg;
"""
prod_schema_create = """
    DROP SCHEMA IF EXISTS prod CASCADE;
    CREATE SCHEMA prod;
"""

# Create staging table
stg_dim_date_create = """
    CREATE TABLE stg.dim_date (
        stg_date_id             INT,
        stg_full_date           VARCHAR,
        stg_year                INT,
        stg_month               INT,
        stg_month_name          VARCHAR,
        stg_day_of_month        INT,
        stg_day_of_week         INT,
        stg_day_name            VARCHAR,
        stg_quarter             VARCHAR,
        stg_holiday_indicator   VARCHAR,
        stg_weekday_indicator   VARCHAR
    );
"""

stg_dim_time_create = """
    CREATE TABLE stg.dim_time (
        stg_full_time           VARCHAR,
        stg_hour                INT,
        stg_minute              INT,
        stg_daytime_indicator   VARCHAR
    );
"""

stg_dim_resolution_create = """
    CREATE TABLE stg.dim_resolution (
        stg_resolution          VARCHAR
    );
"""

stg_dim_police_district_create = """
    CREATE TABLE stg.dim_police_district (
        stg_police_district     VARCHAR
    );
"""

stg_dim_intersection_create = """
    CREATE TABLE stg.dim_intersection (
        stg_cnn                 INT,
        stg_intersection        VARCHAR,
        stg_latitude            DOUBLE PRECISION,
        stg_longitude           DOUBLE PRECISION
    );
"""

stg_dim_category_create = """
    CREATE TABLE stg.dim_category (
        stg_category_code       INT,
        stg_category            VARCHAR,
        stg_subcategory         VARCHAR,
        stg_description         VARCHAR
    );
"""

stg_fact_incident_create = """
    CREATE TABLE stg.fact_incident (
        stg_incident_date       VARCHAR,
        stg_incident_time       VARCHAR,
        stg_report_date         VARCHAR,
        stg_report_time         VARCHAR,
        stg_incident_id         BIGINT,
        stg_category_code       INT,
        stg_resolution          VARCHAR,
        stg_police_district     VARCHAR,
        stg_cnn                 INT
    );
"""
stg_tables_create = (
    stg_dim_date_create
    + stg_dim_time_create
    + stg_dim_resolution_create
    + stg_dim_police_district_create
    + stg_dim_intersection_create
    + stg_dim_category_create
    + stg_fact_incident_create
)

# Create data warehouse table
dim_date_create = """
    CREATE TABLE prod.dim_date (
        date_id                 INT,
        full_date               VARCHAR,
        year                    INT,
        month                   INT,
        month_name              VARCHAR,
        day_of_month            INT,
        day_of_week             INT,
        day_name                VARCHAR,
        quarter                 VARCHAR,
        holiday_indicator       VARCHAR,
        weekday_indicator       VARCHAR
    ) DISTSTYLE ALL;
"""

dim_time_create = """
    CREATE TABLE prod.dim_time (
        time_id                 BIGINT IDENTITY(1,1),
        full_time               VARCHAR,
        hour                    INT,
        minute                  INT,
        daytime_indicator       VARCHAR
    ) DISTSTYLE ALL;
"""

dim_resolution_create = """
    CREATE TABLE prod.dim_resolution (
        resolution_id           BIGINT IDENTITY(1,1),
        resolution              VARCHAR
    ) DISTSTYLE ALL;
"""

dim_police_district_create = """
    CREATE TABLE prod.dim_police_district (
        police_district_id      BIGINT IDENTITY(1,1),
        police_district         VARCHAR
    ) DISTSTYLE ALL;
"""

dim_intersection_create = """
    CREATE TABLE prod.dim_intersection (
        intersection_id         BIGINT IDENTITY(1,1),
        cnn                     INT,
        intersection            VARCHAR,
        latitude                NUMERIC,
        longitude               NUMERIC,
        track_hash              BIGINT,
        eff_date                DATE
                                DEFAULT '1970-01-01'::DATE,
        exp_date                DATE
                                DEFAULT '2999-12-31'::DATE,
        current_flag            SMALLINT DEFAULT 1
    ) DISTSTYLE ALL;
"""

dim_category_create = """
    CREATE TABLE prod.dim_category (
        category_id             BIGINT IDENTITY(1,1),
        category_code           INT,
        category                VARCHAR,
        subcategory             VARCHAR,
        description             VARCHAR,
        track_hash              BIGINT,
        eff_date                DATE
                                DEFAULT '1970-01-01'::DATE,
        exp_date                DATE
                                DEFAULT '2999-12-31'::DATE,
        current_flag            SMALLINT DEFAULT 1
    ) DISTSTYLE ALL;
"""
br_category_group_create = """
    CREATE TABLE prod.br_category_group (
        category_group_id       INT,
        category_id             INT
    ) DISTSTYLE ALL;
"""

dim_category_group_create = """
    CREATE TABLE prod.dim_category_group (
        category_group_id INT
    ) DISTSTYLE ALL;
"""

fact_incident_create = """
    CREATE TABLE prod.fact_incident (
        category_group_id       INT,
        resolution_id           INT,
        incident_date           INT,
        incident_time           INT,
        report_date             INT,
        report_time             INT,
        intersection_id         INT,
        police_district_id      INT,
        incident_id             INT
    );
"""

prod_tables_create = (
    dim_date_create
    + dim_time_create
    + dim_resolution_create
    + dim_police_district_create
    + dim_intersection_create
    + dim_category_create
    + fact_incident_create
    + br_category_group_create
    + dim_category_group_create
)

# Full load data from staging tables to product tables
full_load_dim_date = """
    INSERT INTO prod.dim_date
    SELECT * FROM stg.dim_date;
"""

full_load_dim_time = """
    INSERT INTO prod.dim_time(full_time, hour, minute, daytime_indicator)
    SELECT * FROM stg.dim_time;
"""

full_load_dim_resolution = """
    INSERT INTO prod.dim_resolution(resolution)
    SELECT * FROM stg.dim_resolution;
"""

full_load_dim_police = """
    INSERT INTO prod.dim_police_district(police_district)
    SELECT * FROM stg.dim_police_district;
"""

full_load_dim_intersection = """
    INSERT INTO prod.dim_intersection(cnn, intersection, latitude, longitude, track_hash)
    SELECT stg_cnn, stg_intersection, stg_latitude, stg_longitude, FNV_HASH(stg_intersection) 
    FROM stg.dim_intersection;
"""

full_load_dim_category = """
    INSERT INTO prod.dim_category(category_code, category, subcategory, description, track_hash)
    SELECT stg_category_code, stg_category, stg_subcategory, stg_description, 
            FNV_HASH(stg_category, FNV_HASH(stg_subcategory))
    FROM stg.dim_category;
"""

dim_tables_full_insert = (
    full_load_dim_date
    + full_load_dim_time
    + full_load_dim_resolution
    + full_load_dim_police
    + full_load_dim_intersection
    + full_load_dim_category
)

# Populate bridge category table
temp_category_code_group = """
    SELECT ROW_NUMBER() OVER(ORDER BY category_code_group) AS group_id, category_code_group
    INTO tmp_br_category_group
    FROM (
        SELECT DISTINCT category_code_group
        FROM (
            SELECT LISTAGG(stg_category_code, ',')
            WITHIN GROUP (ORDER BY stg_category_code) AS category_code_group
            FROM stg.fact_incident
            GROUP BY stg_incident_id
        ) cate_list_1
    ) cate_list_2
    ;
"""

full_load_br_category_group = """
    INSERT INTO prod.br_category_group (category_group_id, category_id)
    SELECT tmp_br.group_id, dc.category_id AS category_id
    FROM (
        SELECT group_id, tmp_c::INT AS category_code
        FROM (
            SELECT group_id, split_to_array(category_code_group) AS category_code_array
            FROM tmp_br_category_group
        ) tmp
        LEFT JOIN tmp.category_code_array tmp_c ON TRUE
    ) tmp_br
    JOIN prod.dim_category dc ON tmp_br.category_code = dc.category_code
    ;
"""

full_load_dim_category_group = """
    INSERT INTO prod.dim_category_group
    SELECT DISTINCT category_group_id FROM prod.br_category_group
"""

category_group_table_full_insert = (
    temp_category_code_group
    + full_load_br_category_group
    + full_load_dim_category_group
)
# Populate fact table
full_load_fact_incident = """
    INSERT INTO prod.fact_incident
    WITH stg_fact_category_group AS (
        SELECT tmp_fact_cate.incident_id, tmp_br_cate.group_id
        FROM (
            SELECT stg_incident_id AS incident_id, LISTAGG(stg_category_code, ',')
            WITHIN GROUP (ORDER BY stg_category_code) AS category_code_group
            FROM stg.fact_incident
            GROUP BY stg_incident_id
        ) tmp_fact_cate
        JOIN tmp_br_category_group tmp_br_cate
        ON tmp_fact_cate.category_code_group = tmp_br_cate.category_code_group
    )
    SELECT DISTINCT *
    FROM (
        SELECT
            stg_fact_category_group.group_id AS category_group_id,
            p_r.resolution_id AS resolution_id, 
            p_d1.date_id AS incident_date, 
            p_t1.time_id AS incident_time, 
            p_d2.date_id AS report_date, 
            p_t2.time_id AS report_time, 
            p_i.intersection_id AS intersection_id, 
            p_pd.police_district_id AS police_district_id,
            stg_incident_id AS incident_id
        FROM stg.fact_incident s_f
        JOIN stg_fact_category_group ON s_f.stg_incident_id = stg_fact_category_group.incident_id
        JOIN prod.dim_resolution p_r ON s_f.stg_resolution = p_r.resolution
        JOIN prod.dim_date p_d1 ON s_f.stg_incident_date = p_d1.full_date
        JOIN prod.dim_time p_t1 ON s_f.stg_incident_time = p_t1.full_time
        JOIN prod.dim_date p_d2 ON s_f.stg_report_date = p_d2.full_date
        JOIN prod.dim_time p_t2 ON s_f.stg_report_time = p_t2.full_time
        JOIN prod.dim_intersection p_i ON s_f.stg_cnn = p_i.cnn
        JOIN prod.dim_police_district p_pd ON s_f.stg_police_district = p_pd.police_district
    ) tmp_fact;

    DROP TABLE tmp_br_category_group;
"""
