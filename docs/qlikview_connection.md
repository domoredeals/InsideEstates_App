# Connecting QlikView to PostgreSQL Database

## Prerequisites

1. **PostgreSQL ODBC Driver** - Install on the machine running QlikView
   - Download from: https://www.postgresql.org/ftp/odbc/versions/msi/
   - Use 64-bit version if QlikView is 64-bit

2. **Database Connection Details**
   - Host: localhost (or your server IP)
   - Port: 5432
   - Database: insideestates_app
   - Username: insideestates_user
   - Password: InsideEstates2024!

## Method 1: ODBC Connection (Recommended)

### Step 1: Configure ODBC Data Source
1. Open **ODBC Data Sources** (Windows)
   - Press Windows Key + R
   - Type: `odbcad32` (for 64-bit) or `C:\Windows\SysWOW64\odbcad32.exe` (for 32-bit)
   
2. Click **Add** → Select **PostgreSQL Unicode**

3. Configure connection:
   ```
   Data Source: InsideEstates_PostgreSQL
   Database: insideestates_app
   Server: localhost
   Port: 5432
   User Name: insideestates_user
   Password: InsideEstates2024!
   ```

4. Click **Test** to verify connection

### Step 2: Connect in QlikView
1. Open QlikView
2. Go to **Edit Script** (Ctrl+E)
3. Click **Connect** button
4. Select **ODBC** → Choose **InsideEstates_PostgreSQL**
5. Test connection

### Step 3: Load Data
```sql
// Example QlikView load script
Properties:
SQL SELECT 
    title_number,
    tenure,
    property_address,
    district,
    county,
    region,
    postcode,
    price_paid,
    dataset_type,
    change_date
FROM properties
WHERE dataset_type = 'CCOD'
LIMIT 10000;  // Start with limited data for testing

// Load company statistics
CompanyStats:
SQL SELECT 
    company_name,
    company_registration_no,
    property_count,
    total_value_owned
FROM companies
WHERE property_count > 10
ORDER BY property_count DESC;
```

## Method 2: Direct PostgreSQL Connection

In QlikView script editor:
```sql
OLEDB CONNECT TO 'Provider=PostgreSQL OLE DB Provider;Data Source=localhost;Initial Catalog=insideestates_app;User Id=insideestates_user;Password=InsideEstates2024!;';

// Or using connection string
LIB CONNECT TO 'postgresql://insideestates_user:InsideEstates2024!@localhost:5432/insideestates_app';
```

## Optimized Views for QlikView

Create these views in PostgreSQL for better QlikView performance:

```sql
-- Summary view for properties
CREATE OR REPLACE VIEW qv_property_summary AS
SELECT 
    p.title_number,
    p.tenure,
    p.property_address,
    p.postcode,
    p.district,
    p.county,
    p.region,
    p.price_paid,
    p.dataset_type,
    p.change_date,
    EXTRACT(YEAR FROM p.change_date) as change_year,
    EXTRACT(MONTH FROM p.change_date) as change_month,
    CASE 
        WHEN p.price_paid IS NOT NULL THEN 
            CASE 
                WHEN p.price_paid < 250000 THEN 'Under £250k'
                WHEN p.price_paid < 500000 THEN '£250k-£500k'
                WHEN p.price_paid < 1000000 THEN '£500k-£1m'
                WHEN p.price_paid < 5000000 THEN '£1m-£5m'
                ELSE 'Over £5m'
            END
        ELSE 'No Price'
    END as price_band
FROM properties p;

-- Monthly aggregates for performance
CREATE OR REPLACE VIEW qv_monthly_stats AS
SELECT 
    dataset_type,
    file_month,
    COUNT(*) as property_count,
    COUNT(price_paid) as properties_with_price,
    AVG(price_paid) as avg_price,
    SUM(price_paid) as total_value
FROM properties
GROUP BY dataset_type, file_month;

-- Geographic aggregates
CREATE OR REPLACE VIEW qv_geographic_stats AS
SELECT 
    dataset_type,
    region,
    county,
    COUNT(*) as property_count,
    AVG(price_paid) as avg_price,
    COUNT(DISTINCT CASE WHEN postcode != '' THEN postcode END) as unique_postcodes
FROM properties
GROUP BY dataset_type, region, county;
```

## Performance Tips for QlikView

1. **Use WHERE clauses** to limit data during development
2. **Create indexes** on commonly filtered fields
3. **Use incremental loads** for updates
4. **Consider creating aggregate tables** for large datasets
5. **Use QVD files** for better performance

## Sample QlikView Load Script

```sql
// Set variables
SET ThousandSep=',';
SET DecimalSep='.';
SET MoneyThousandSep=',';
SET MoneyDecimalSep='.';
SET MoneyFormat='£#,##0;(£#,##0)';

// Connect to database
ODBC CONNECT TO 'InsideEstates_PostgreSQL';

// Load dimension data
Regions:
LOAD DISTINCT
    region as Region,
    region as RegionName;
SQL SELECT DISTINCT region 
FROM properties 
WHERE region != '';

// Load fact data with limits for testing
PropertyFacts:
LOAD
    title_number as TitleNumber,
    tenure as Tenure,
    property_address as Address,
    postcode as Postcode,
    district as District,
    county as County,
    region as Region,
    price_paid as PricePaid,
    dataset_type as DatasetType,
    Date(change_date) as ChangeDate,
    Year(change_date) as Year,
    Month(change_date) as Month;
SQL SELECT * FROM qv_property_summary
WHERE change_date >= '2024-01-01'
LIMIT 100000;

// Store as QVD for performance
STORE PropertyFacts INTO [PropertyFacts.qvd] (qvd);
```

## Troubleshooting

1. **Connection Issues**
   - Ensure PostgreSQL is running
   - Check firewall allows port 5432
   - Verify credentials in .env file

2. **Performance Issues**
   - Start with smaller datasets
   - Use views instead of complex queries
   - Create appropriate indexes
   - Consider using QVDs

3. **ODBC Driver Issues**
   - Match driver architecture (32/64 bit) with QlikView
   - Reinstall PostgreSQL ODBC driver if needed
   - Check Windows Event Log for errors

## Security Considerations

1. Create read-only PostgreSQL user for QlikView:
```sql
CREATE USER qlikview_reader WITH PASSWORD 'ReadOnly2024!';
GRANT CONNECT ON DATABASE insideestates_app TO qlikview_reader;
GRANT USAGE ON SCHEMA public TO qlikview_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO qlikview_reader;
```

2. Use this read-only account in QlikView connections
3. Never store passwords in QlikView scripts - use ODBC DSN instead