# PostgreSQL ODBC Driver Setup for Windows

## Download PostgreSQL ODBC Driver

### Official Download Link
**PostgreSQL ODBC Driver**: https://www.postgresql.org/ftp/odbc/versions/

### Direct Links (as of 2024):
- **64-bit Windows**: https://ftp.postgresql.org/pub/odbc/versions/msi/psqlodbc_16_00_0000-x64.zip
- **32-bit Windows**: https://ftp.postgresql.org/pub/odbc/versions/msi/psqlodbc_16_00_0000-x86.zip

### Alternative Download Sources:
1. **Stack Builder** (if you have PostgreSQL installed on Windows)
   - Run Stack Builder from PostgreSQL program group
   - Select "Database Drivers" → "psqlODBC"

2. **Direct from psqlODBC site**: 
   - https://odbc.postgresql.org/

## Installation Steps

1. **Download the MSI installer** for your system (64-bit or 32-bit)

2. **Run the installer** as Administrator

3. **Follow installation wizard** - default settings are fine

4. **Verify installation** by opening ODBC Data Sources:
   - 64-bit: Run `odbcad32` from Start Menu
   - 32-bit: Run `C:\Windows\SysWOW64\odbcad32.exe`
   - You should see "PostgreSQL ANSI" and "PostgreSQL Unicode" in the drivers list

## Configure ODBC Data Source

1. **Open ODBC Data Source Administrator** (as above)

2. **Go to System DSN or User DSN tab**

3. **Click Add** and select "PostgreSQL Unicode(x64)" or similar

4. **Configure with these settings**:
   ```
   Data Source: InsideEstates_PostgreSQL
   Description: InsideEstates Property Database
   Database: insideestates_app
   Server: localhost (or your server IP)
   Port: 5432
   User Name: insideestates_user
   Password: InsideEstates2024!
   ```

5. **Click "Test"** to verify connection

6. **Additional Options** (if needed):
   - SSL Mode: prefer
   - Show System Tables: unchecked
   - Row Versioning: checked (for better QlikView compatibility)

## QlikView Connection String

Once ODBC is configured, use in QlikView:

```sql
// Using DSN
ODBC CONNECT TO 'DSN=InsideEstates_PostgreSQL;';

// Or using direct connection string
ODBC CONNECT TO 'Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=insideestates_app;Uid=insideestates_user;Pwd=InsideEstates2024!;';
```

## Troubleshooting

### "Architecture mismatch" error
- Ensure ODBC driver bit version matches QlikView (both 32-bit or both 64-bit)

### "Connection refused" error  
- Check PostgreSQL is running: `sudo systemctl status postgresql`
- Check Windows Firewall allows port 5432
- Verify PostgreSQL accepts connections from your IP in pg_hba.conf

### "Authentication failed" error
- Verify username and password are correct
- Check user exists in PostgreSQL
- Ensure database name is correct

### Performance issues
- Use the Unicode driver (not ANSI) for better performance
- Enable connection pooling in ODBC settings
- Use QlikView's incremental load features