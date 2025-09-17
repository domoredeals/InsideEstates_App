-- Fix VARCHAR field limits by converting to TEXT fields
-- This allows unlimited length for address and name fields

-- Convert all proprietor name fields to TEXT
ALTER TABLE land_registry_data 
    ALTER COLUMN proprietor_1_name TYPE TEXT,
    ALTER COLUMN proprietor_2_name TYPE TEXT,
    ALTER COLUMN proprietor_3_name TYPE TEXT,
    ALTER COLUMN proprietor_4_name TYPE TEXT;

-- Convert all address fields to TEXT
ALTER TABLE land_registry_data 
    ALTER COLUMN property_address TYPE TEXT,
    ALTER COLUMN proprietor_1_address_1 TYPE TEXT,
    ALTER COLUMN proprietor_1_address_2 TYPE TEXT,
    ALTER COLUMN proprietor_1_address_3 TYPE TEXT,
    ALTER COLUMN proprietor_2_address_1 TYPE TEXT,
    ALTER COLUMN proprietor_2_address_2 TYPE TEXT,
    ALTER COLUMN proprietor_2_address_3 TYPE TEXT,
    ALTER COLUMN proprietor_3_address_1 TYPE TEXT,
    ALTER COLUMN proprietor_3_address_2 TYPE TEXT,
    ALTER COLUMN proprietor_3_address_3 TYPE TEXT,
    ALTER COLUMN proprietor_4_address_1 TYPE TEXT,
    ALTER COLUMN proprietor_4_address_2 TYPE TEXT,
    ALTER COLUMN proprietor_4_address_3 TYPE TEXT;