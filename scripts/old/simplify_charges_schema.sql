-- Simplify schema by adding additional transaction fields to ch_scrape_charges table

-- Add columns for satisfaction transaction details
ALTER TABLE ch_scrape_charges 
ADD COLUMN IF NOT EXISTS satisfaction_type TEXT,
ADD COLUMN IF NOT EXISTS satisfaction_delivered_date DATE;

-- Migrate any existing data from transactions table
UPDATE ch_scrape_charges c
SET satisfaction_type = t.transaction_type,
    satisfaction_delivered_date = t.delivered_date
FROM ch_scrape_charge_transactions t
WHERE c.company_number = t.company_number 
AND c.charge_id = t.charge_id
AND t.transaction_type LIKE '%satisfaction%';

-- Drop the transactions table as it's no longer needed
DROP TABLE IF EXISTS ch_scrape_charge_transactions;