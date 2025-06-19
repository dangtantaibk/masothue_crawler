CREATE TABLE mst_province(
    id SERIAL,
    name VARCHAR NOT NULL,
    slug VARCHAR NOT NULL
);

CREATE TABLE mst_district(
    id SERIAL,
    name VARCHAR NOT NULL,
    slug VARCHAR NOT NULL,
    province_id int4 NOT NULL,
    province_name VARCHAR NOT NULL
);

CREATE TABLE mst_career(
    id SERIAL,
    code VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    slug VARCHAR NOT NULL
);

CREATE TABLE mst_company(
    id SERIAL,
    name VARCHAR NOT NULL,
    name_short VARCHAR,
    name_global VARCHAR,
    tax VARCHAR NOT NULL,
    address VARCHAR,
    district_id int4,
    province_id int4,
    representative VARCHAR,
    phone VARCHAR,
    active_date timestamp,
    manage_by VARCHAR,
    category VARCHAR,
    status VARCHAR,
    last_update timestamp,
    career_code TEXT,
    career_name TEXT,
    slug VARCHAR NOT NULL
);