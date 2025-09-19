-- Add additional columns to ch_scrape_charges table for detailed charge information

-- Add columns for transaction details
ALTER TABLE ch_scrape_charges 
ADD COLUMN IF NOT EXISTS transaction_filed TEXT,
ADD COLUMN IF NOT EXISTS registration_type TEXT,
ADD COLUMN IF NOT EXISTS amount_secured TEXT,
ADD COLUMN IF NOT EXISTS short_particulars TEXT,
ADD COLUMN IF NOT EXISTS contains_negative_pledge BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS contains_floating_charge BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS floating_charge_covers_all BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS contains_fixed_charge BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS fixed_charge_description TEXT,
ADD COLUMN IF NOT EXISTS charge_link TEXT;

-- Add table for additional transactions against charges
CREATE TABLE IF NOT EXISTS ch_scrape_charge_transactions (
    id SERIAL PRIMARY KEY,
    company_number TEXT NOT NULL,
    charge_id TEXT NOT NULL,
    transaction_type TEXT,
    transaction_date DATE,
    delivered_date DATE,
    view_download_link TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (company_number, charge_id) REFERENCES ch_scrape_charges(company_number, charge_id)
);

-- Add index for performance
CREATE INDEX IF NOT EXISTS idx_charge_transactions_charge ON ch_scrape_charge_transactions(company_number, charge_id);