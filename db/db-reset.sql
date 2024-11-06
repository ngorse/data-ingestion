\c postgres
DROP DATABASE IF EXISTS inventory;
CREATE DATABASE inventory;
\c inventory

CREATE TABLE brand (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE product (
    id SERIAL PRIMARY KEY,
    id_brand INTEGER NOT NULL REFERENCES brand(id) ON DELETE CASCADE,
    product_id TEXT NOT NULL
);

CREATE TABLE csv_brand (
    id SERIAL PRIMARY KEY,
    id_product INTEGER NOT NULL REFERENCES product(id) ON DELETE CASCADE,
    csv_line INTEGER NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE variant (
    id SERIAL PRIMARY KEY,
    id_product INTEGER NOT NULL REFERENCES product(id) ON DELETE CASCADE,
    variant_id TEXT NOT NULL,
    age_group TEXT,
    gender_male BOOLEAN,
    gender_female BOOLEAN,
    gender_unisex BOOLEAN,
    size_type TEXT DEFAULT 'regular'
);

CREATE TABLE csv_age_group (
    id SERIAL PRIMARY KEY,
    id_variant INTEGER NOT NULL REFERENCES variant(id) ON DELETE CASCADE,
    csv_line INTEGER NOT NULL,
    age_group TEXT NOT NULL
);

CREATE TABLE csv_gender (
    id SERIAL PRIMARY KEY,
    id_variant INTEGER NOT NULL REFERENCES variant(id) ON DELETE CASCADE,
    csv_line INTEGER NOT NULL,
    gender TEXT NOT NULL
);


CREATE TABLE localized_meta (
    id SERIAL PRIMARY KEY,
    id_variant INTEGER NOT NULL REFERENCES variant(id) ON DELETE CASCADE,
    csv_line INTEGER NOT NULL,
    locale TEXT,
    size_label TEXT,
    product_name TEXT,
    color TEXT,
    product_type TEXT
);

CREATE TABLE warnings (
    id SERIAL PRIMARY KEY,
    csv_line int,
    warning TEXT,
    description TEXT
);

