-- Connect to the default database (e.g., `postgres`) to be able to drop and create the target database.
\c postgres

-- Drop the database if it exists
DROP DATABASE IF EXISTS inventory;

-- Create the database
CREATE DATABASE inventory;

-- Connect to the newly created database
\c inventory

-- Create the brand table
CREATE TABLE brand (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- Create the product table with a foreign key to brand
CREATE TABLE product (
    id SERIAL PRIMARY KEY,
    id_brand INTEGER NOT NULL REFERENCES brand(id) ON DELETE CASCADE,
    product_id TEXT NOT NULL
);

-- Create the brand table
CREATE TABLE csv_brand (
    id SERIAL PRIMARY KEY,
    id_product INTEGER NOT NULL REFERENCES product(id) ON DELETE CASCADE,
    csv_line INTEGER NOT NULL,
    name TEXT NOT NULL
);

---- Create the variant table with a foreign key to product
--CREATE TABLE variant (
--    id SERIAL PRIMARY KEY,
--    id_product INTEGER NOT NULL REFERENCES product(id) ON DELETE CASCADE,
--    csv_line INTEGER NOT NULL,
--    variant_id TEXT NOT NULL,
--    age_group TEXT,  -- Flag if there are inconsistencies during ingestion
--    gender TEXT,     -- Flag if there are inconsistencies during ingestion
--    size_type TEXT DEFAULT 'regular',  -- Default value since it is the same across the database
--    UNIQUE (variant_id)  -- Ensure each variant_id is unique
--);
--
---- Create the localized_meta table with a foreign key to variant
--CREATE TABLE localized_meta (
--    id SERIAL PRIMARY KEY,
--    id_variant INTEGER NOT NULL REFERENCES variant(id) ON DELETE CASCADE,
--    csv_line INTEGER NOT NULL,
--    size_label TEXT,
--    product_name TEXT,
--    color TEXT,
--    product_type TEXT
--);
--
---- Create a warnings table to store flags for issues
--CREATE TABLE warnings (
--    id SERIAL PRIMARY KEY,
--    id_variant INTEGER NOT NULL REFERENCES variant(id) ON DELETE CASCADE,
--    warning_type TEXT,  -- e.g., 'size_label', 'age_group', 'gender'
--    description TEXT     -- Additional information about the issue
--);
--
