-- Single table schema for Land Registry CCOD/OCOD data
-- All property and proprietor data in one denormalized table

-- Drop existing table if needed
DROP TABLE IF EXISTS land_registry_data CASCADE;

-- Create the single comprehensive table
CREATE TABLE land_registry_data (
    id BIGSERIAL PRIMARY KEY,
    
    -- Property fields
    title_number VARCHAR(20) NOT NULL,
    tenure VARCHAR(50),
    property_address TEXT,
    district TEXT,
    county TEXT,
    region TEXT,
    postcode VARCHAR(10),
    multiple_address_indicator CHAR(1),
    additional_proprietor_indicator CHAR(1),
    price_paid NUMERIC(12,2),
    date_proprietor_added DATE,
    change_indicator CHAR(1),
    change_date DATE,
    
    -- Metadata fields
    dataset_type VARCHAR(10) NOT NULL, -- 'CCOD' or 'OCOD'
    update_type VARCHAR(10) NOT NULL,  -- 'FULL' or 'COU'
    file_month DATE NOT NULL,          -- Which monthly file this came from
    source_filename VARCHAR(255),      -- Original filename for data lineage
    
    -- Proprietor 1 fields
    proprietor_1_name TEXT,
    company_1_reg_no VARCHAR(50),
    proprietorship_1_category TEXT,
    country_1_incorporated TEXT,   -- Only populated for OCOD
    proprietor_1_address_1 TEXT,
    proprietor_1_address_2 TEXT,
    proprietor_1_address_3 TEXT,
    
    -- Proprietor 2 fields
    proprietor_2_name TEXT,
    company_2_reg_no VARCHAR(50),
    proprietorship_2_category TEXT,
    country_2_incorporated TEXT,   -- Only populated for OCOD
    proprietor_2_address_1 TEXT,
    proprietor_2_address_2 TEXT,
    proprietor_2_address_3 TEXT,
    
    -- Proprietor 3 fields
    proprietor_3_name TEXT,
    company_3_reg_no VARCHAR(50),
    proprietorship_3_category TEXT,
    country_3_incorporated TEXT,   -- Only populated for OCOD
    proprietor_3_address_1 TEXT,
    proprietor_3_address_2 TEXT,
    proprietor_3_address_3 TEXT,
    
    -- Proprietor 4 fields
    proprietor_4_name TEXT,
    company_4_reg_no VARCHAR(50),
    proprietorship_4_category TEXT,
    country_4_incorporated TEXT,   -- Only populated for OCOD
    proprietor_4_address_1 TEXT,
    proprietor_4_address_2 TEXT,
    proprietor_4_address_3 TEXT,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure we don't duplicate the same property from the same file
    CONSTRAINT uk_title_file UNIQUE(title_number, file_month)
);

-- Create indexes for performance
CREATE INDEX idx_lr_title_number ON land_registry_data(title_number);
CREATE INDEX idx_lr_postcode ON land_registry_data(postcode);
CREATE INDEX idx_lr_district ON land_registry_data(district);
CREATE INDEX idx_lr_county ON land_registry_data(county);
CREATE INDEX idx_lr_region ON land_registry_data(region);
CREATE INDEX idx_lr_dataset_type ON land_registry_data(dataset_type);
CREATE INDEX idx_lr_file_month ON land_registry_data(file_month);
CREATE INDEX idx_lr_update_type ON land_registry_data(update_type);
CREATE INDEX idx_lr_source_filename ON land_registry_data(source_filename);
CREATE INDEX idx_lr_price_paid ON land_registry_data(price_paid) WHERE price_paid IS NOT NULL;

-- Indexes on company registration numbers
CREATE INDEX idx_lr_company_1_reg ON land_registry_data(company_1_reg_no) WHERE company_1_reg_no IS NOT NULL;
CREATE INDEX idx_lr_company_2_reg ON land_registry_data(company_2_reg_no) WHERE company_2_reg_no IS NOT NULL;
CREATE INDEX idx_lr_company_3_reg ON land_registry_data(company_3_reg_no) WHERE company_3_reg_no IS NOT NULL;
CREATE INDEX idx_lr_company_4_reg ON land_registry_data(company_4_reg_no) WHERE company_4_reg_no IS NOT NULL;

-- Indexes on proprietor names for text search
CREATE INDEX idx_lr_prop_1_name ON land_registry_data(proprietor_1_name) WHERE proprietor_1_name IS NOT NULL;
CREATE INDEX idx_lr_prop_2_name ON land_registry_data(proprietor_2_name) WHERE proprietor_2_name IS NOT NULL;
CREATE INDEX idx_lr_prop_3_name ON land_registry_data(proprietor_3_name) WHERE proprietor_3_name IS NOT NULL;
CREATE INDEX idx_lr_prop_4_name ON land_registry_data(proprietor_4_name) WHERE proprietor_4_name IS NOT NULL;

-- Trigger to update timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_land_registry_updated_at 
    BEFORE UPDATE ON land_registry_data
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Sample queries to use with this table:

-- Find all properties owned by a specific company:
-- SELECT * FROM land_registry_data 
-- WHERE company_1_reg_no = 'COMPANY123' 
--    OR company_2_reg_no = 'COMPANY123'
--    OR company_3_reg_no = 'COMPANY123' 
--    OR company_4_reg_no = 'COMPANY123';

-- Find latest data for a property (most recent file_month):
-- SELECT DISTINCT ON (title_number) *
-- FROM land_registry_data
-- WHERE title_number = 'TITLE123'
-- ORDER BY title_number, file_month DESC;

-- Count properties by region:
-- SELECT region, COUNT(DISTINCT title_number) as property_count
-- FROM land_registry_data
-- GROUP BY region
-- ORDER BY property_count DESC;