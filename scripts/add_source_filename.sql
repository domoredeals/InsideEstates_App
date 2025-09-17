-- Add source_filename column to track which file each record came from
ALTER TABLE land_registry_data 
ADD COLUMN source_filename VARCHAR(255);

-- Create an index on source_filename for faster queries
CREATE INDEX idx_lr_source_filename ON land_registry_data(source_filename);