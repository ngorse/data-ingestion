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
    csv_line INTEGER NOT NULL,
    name TEXT NOT NULL
);

-- Create the product table with a foreign key to brand
CREATE TABLE product (
    id SERIAL PRIMARY KEY,
    id_brand INTEGER NOT NULL REFERENCES brand(id) ON DELETE CASCADE,
    csv_line INTEGER NOT NULL,
    product_id TEXT NOT NULL
);

-- Create the variant table with a foreign key to product
CREATE TABLE variant (
    id SERIAL PRIMARY KEY,
    id_product INTEGER NOT NULL REFERENCES product(id) ON DELETE CASCADE,
    csv_line INTEGER NOT NULL,
    variant_id TEXT NOT NULL,
    age_group TEXT,
    gender TEXT,
    size_type TEXT
);

-- Create the localized_meta table with a foreign key to variant
CREATE TABLE localized_meta (
    id SERIAL PRIMARY KEY,
    id_variant INTEGER NOT NULL REFERENCES variant(id) ON DELETE CASCADE,
    csv_line INTEGER NOT NULL,
    size_label TEXT,
    product_name TEXT,
    color TEXT,
    product_type TEXT
);