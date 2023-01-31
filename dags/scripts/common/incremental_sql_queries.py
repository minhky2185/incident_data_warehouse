incremental_load_dim_date = """
    INSERT INTO prod.dim_date
    SELECT *
    FROM (
        SELECT s_d.*
        FROM stg.dim_date s_d
        LEFT JOIN prod.dim_date p_d ON s_d.stg_date_id = p_d.date_id
        WHERE p_d.date_id IS NULL
    ) stg_date;
"""

incremental_load_dim_time = """
    INSERT INTO prod.dim_time(full_time, hour, minute, daytime_indicator)
    SELECT *
    FROM (
        SELECT s_t.*
        FROM stg.dim_time s_t
        LEFT JOIN prod.dim_time p_t ON s_t.stg_full_time = p_t.full_time
        WHERE p_t.full_time IS NULL
    ) stg_time;
"""

incremental_load_dim_resolution = """
    INSERT INTO prod.dim_resolution(resolution)
    SELECT *
    FROM (
        SELECT s_r.stg_resolution
        FROM stg.dim_resolution s_r
        LEFT JOIN prod.dim_resolution p_r 
        ON s_r.stg_resolution = p_r.resolution
        WHERE p_r.resolution IS NULL  
    ) stg_resoution;
"""

incremental_load_dim_police_district = """
    INSERT INTO prod.dim_police_district(police_district)
    SELECT *
    FROM (
        SELECT s_pd.stg_police_district
        FROM stg.dim_police_district s_pd
        LEFT JOIN prod.dim_police_district p_pd
        ON s_pd.stg_police_district = p_pd.police_district
        WHERE p_pd.police_district IS NULL
    ) stg_police_district;
"""

incremental_load_dim_intersection = """
    SELECT stg_i_1.*,
            CASE WHEN p_i.cnn IS NULL THEN 1 ELSE 0 END new_row,
            CASE WHEN p_i.cnn IS NOT NULL
                AND stg_i_1.stg_track_hash <> p_i.track_hash THEN 1 ELSE 0 END change_row
    INTO stg_i_2
    FROM (
        SELECT stg_cnn, stg_intersection, stg_latitude, stg_longitude,
                FNV_HASH(stg_intersection) AS stg_track_hash
        FROM stg.dim_intersection
    ) stg_i_1
    LEFT JOIN prod.dim_intersection p_i 
    ON stg_i_1.stg_cnn = p_i.cnn;


    UPDATE prod.dim_intersection
    SET exp_date = CURRENT_DATE,
        current_flag = 0
    FROM stg_i_2
    WHERE cnn = stg_cnn AND change_row = 1 AND current_flag = 1;


    INSERT INTO prod.dim_intersection(cnn, intersection, latitude, longitude, 
                                        track_hash, eff_date)
    SELECT stg_cnn, stg_intersection, stg_latitude, stg_longitude, stg_track_hash,
            CURRENT_DATE AS eff_date
    FROM stg_i_2
    WHERE new_row = 1 OR change_row = 1;


    UPDATE prod.dim_intersection
    SET latitude = stg_latitude,
        longitude = stg_longitude
    FROM stg_i_2
    WHERE cnn = stg_cnn 
    AND exp_date = '2999-12-31'
    AND new_row = 0 AND change_row = 0;

    DROP TABLE stg_i_2;    
"""

incremental_load_dim_category = """
    SELECT stg_c_1.*,
            CASE WHEN p_c.category_code IS NULL THEN 1 ELSE 0 END new_row,
            CASE WHEN p_c.category_code IS NOT NULL
                AND stg_c_1.stg_track_hash <> p_c.track_hash THEN 1 ELSE 0 END change_row
    INTO stg_c_2
    FROM (
        SELECT stg_category_code, stg_category, stg_subcategory, stg_description,
                FNV_HASH(stg_category, FNV_HASH(stg_subcategory)) as stg_track_hash
        FROM stg.dim_category
    ) stg_c_1
    LEFT JOIN prod.dim_category p_c ON stg_c_1.stg_category_code = p_c.category_code;


    UPDATE prod.dim_category
    SET exp_date = CURRENT_DATE,
        current_flag = 0
    FROM stg_c_2
    WHERE category_code = stg_category_code AND change_row = 1 AND current_flag = 1;


    INSERT INTO prod.dim_category(category_code, category, subcategory, description,
                                track_hash, eff_date)
    SELECT stg_category_code, stg_category, stg_subcategory, stg_description, stg_track_hash,
                                CURRENT_DATE AS eff_date
    FROM stg_c_2
    WHERE new_row = 1 OR change_row = 1;


    UPDATE prod.dim_category
    SET description = stg_description
    FROM stg_c_2
    WHERE category_code = stg_category_code 
    AND exp_date = '2999-12-31'
    AND new_row = 0 AND change_row = 0;

    DROP TABLE stg_c_2;    
"""

dim_tables_incremental_insert = (
    incremental_load_dim_date
    + incremental_load_dim_time
    + incremental_load_dim_resolution
    + incremental_load_dim_police_district
    + incremental_load_dim_intersection
    + incremental_load_dim_category
)

# Incremental insert for bridge category table
tmp_compare = """
    SELECT LISTAGG(category_id, ',')
            WITHIN GROUP (ORDER BY category_id) AS category_id_group
    INTO tmp_compare    
    FROM prod.br_category_group
    GROUP BY category_group_id;
"""

tmp_br_cate = """
    SELECT ROW_NUMBER() OVER(ORDER BY category_id_group) AS group_id, category_id_group
    INTO tmp_br_cate
    FROM (
        SELECT DISTINCT category_id_group
        FROM (
            SELECT LISTAGG(category_id, ',')
            WITHIN GROUP (ORDER BY category_id) AS category_id_group
            FROM (
                SELECT s_f.stg_incident_id, p_c.category_id
                FROM stg.fact_incident s_f
                JOIN prod.dim_category p_c 
                ON s_f.stg_category_code = p_c.category_code AND p_c.current_flag = 1
            ) tmp_cate_1
            GROUP BY stg_incident_id
        ) tmp_cate_2
    ) tmp_cate_3;
"""


incremental_load_bridge_category = """
    INSERT INTO prod.br_category_group (category_group_id, category_id)
    WITH tmp_insert_bridge AS (
        SELECT (tmp_max.max_id + tmp_new_group.group_id) AS group_id, tmp_new_group.category_id_group
        FROM (
            SELECT tmp_br_cate.group_id, tmp_br_cate.category_id_group
            FROM tmp_br_cate
            JOIN tmp_compare ON tmp_br_cate.category_id_group = tmp_compare.category_id_group
            WHERE tmp_compare.category_id_group IS NULL
        ) tmp_new_group
        CROSS JOIN (
            SELECT MAX(category_group_id) AS max_id FROM prod.br_category_group
        ) tmp_max
    )
    SELECT group_id, tmp_id::INT AS category_id
    FROM (
        SELECT group_id, split_to_array(category_id_group) AS category_id_array
        FROM tmp_insert_bridge
    ) tmp_cate_4
    LEFT JOIN tmp_cate_4.category_id_array tmp_id ON TRUE;
"""

incremental_load_dim_category_group = """
    INSERT INTO prod.dim_category_group
    SELECT DISTINCT category_group_id
    FROM prod.br_category_group
    WHERE category_group_id > (
        SELECT MAX (category_group_id) FROM prod.dim_category_group
    );

    DROP TABLE tmp_compare;
    DROP TABLE tmp_br_cate;
"""

category_group_table_incremental_insert = (
    tmp_compare
    + tmp_br_cate
    + incremental_load_bridge_category
    + incremental_load_dim_category_group
)

# Incremental insert for fact incident
temp_table_for_compare = """
    SELECT category_group_id, LISTAGG(category_id, ',')
    WITHIN GROUP (ORDER BY category_id) AS category_id_group
    INTO tmp_compare_fact
    FROM prod.br_category_group
    GROUP BY category_group_id;
"""

populate_fact_incident = """
    INSERT INTO prod.fact_incident
    WITH tmp_fact_cate_group_id AS (
        SELECT tmp_fact_cate_id_group.stg_incident_id AS incident_id, tmp_compare_fact.category_group_id
        FROM (
            SELECT stg_incident_id, LISTAGG(category_id, ',')
            WITHIN GROUP (ORDER BY category_id) AS category_id_group
            FROM (
                SELECT s_f.stg_incident_id, p_c.category_id
                FROM stg.fact_incident s_f
                JOIN prod.dim_category p_c
                ON s_f.stg_category_code = p_c.category_code AND p_c.current_flag = 1
            ) tmp_fact_cate_id
            GROUP BY stg_incident_id 
        ) tmp_fact_cate_id_group
        JOIN tmp_compare_fact 
        ON tmp_fact_cate_id_group.category_id_group = tmp_compare_fact.category_id_group
    )
    SELECT DISTINCT *
    FROM (
        SELECT
            tmp_fact_cate_group_id.category_group_id,
            p_r.resolution_id AS resolution_id, 
            p_d1.date_id AS incident_date, 
            p_t1.time_id AS incident_time, 
            p_d2.date_id AS report_date, 
            p_t2.time_id AS report_time, 
            p_i.intersection_id AS intersection_id, 
            p_pd.police_district_id AS police_district_id,
            stg_incident_id AS incident_id
        FROM stg.fact_incident s_f
        JOIN tmp_fact_cate_group_id ON s_f.stg_incident_id = tmp_fact_cate_group_id.incident_id
        JOIN prod.dim_resolution p_r ON s_f.stg_resolution = p_r.resolution
        JOIN prod.dim_date p_d1 ON s_f.stg_incident_date = p_d1.full_date
        JOIN prod.dim_time p_t1 ON s_f.stg_incident_time = p_t1.full_time
        JOIN prod.dim_date p_d2 ON s_f.stg_report_date = p_d2.full_date
        JOIN prod.dim_time p_t2 ON s_f.stg_report_time = p_t2.full_time
        JOIN prod.dim_intersection p_i ON s_f.stg_cnn = p_i.cnn AND p_i.current_flag = 1
        JOIN prod.dim_police_district p_pd ON s_f.stg_police_district = p_pd.police_district
    ) tmp_fact;

    DROP TABLE tmp_compare_fact;
"""

incremental_load_fact_incident = temp_table_for_compare + populate_fact_incident
