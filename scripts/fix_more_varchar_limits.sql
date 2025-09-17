-- Fix additional VARCHAR field limits
-- Convert category and country fields to TEXT to handle long values

-- Convert all proprietorship category fields to TEXT
ALTER TABLE land_registry_data 
    ALTER COLUMN proprietorship_1_category TYPE TEXT,
    ALTER COLUMN proprietorship_2_category TYPE TEXT,
    ALTER COLUMN proprietorship_3_category TYPE TEXT,
    ALTER COLUMN proprietorship_4_category TYPE TEXT;

-- Convert all country incorporated fields to TEXT
ALTER TABLE land_registry_data 
    ALTER COLUMN country_1_incorporated TYPE TEXT,
    ALTER COLUMN country_2_incorporated TYPE TEXT,
    ALTER COLUMN country_3_incorporated TYPE TEXT,
    ALTER COLUMN country_4_incorporated TYPE TEXT;

-- Also convert some other fields that might have long values
ALTER TABLE land_registry_data 
    ALTER COLUMN district TYPE TEXT,
    ALTER COLUMN county TYPE TEXT,
    ALTER COLUMN region TYPE TEXT;