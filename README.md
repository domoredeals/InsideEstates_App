# InsideEstates App - PostgreSQL Version

First live version of InsideEstates using PostgreSQL database.

## Project Structure
```
InsideEstates_App/
├── app/              # Flask application code
├── config/           # Configuration files
├── db/               # Database migration files
├── scripts/          # Setup and utility scripts
├── static/           # Static assets (CSS, JS, images)
├── templates/        # HTML templates
├── .env.example      # Environment variables template
└── requirements.txt  # Python dependencies
```

## Setup Instructions

1. **Create Python virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

4. **Initialize PostgreSQL database:**
   ```bash
   # As PostgreSQL superuser:
   sudo -u postgres psql -f scripts/init_database.sql
   
   # Or using Python script:
   python scripts/setup_database.py
   ```

5. **Optimize PostgreSQL for performance:**
   ```bash
   # Apply performance optimizations (128GB RAM system)
   python scripts/optimize_postgresql.py
   
   # Include tuning config in postgresql.conf:
   sudo echo 'include_if_exists = /home/adc/Projects/InsideEstates_App/config/postgresql_tuning.conf' >> /etc/postgresql/*/main/postgresql.conf
   
   # Restart PostgreSQL to apply all settings:
   sudo systemctl restart postgresql
   ```

6. **Load data efficiently:**
   ```bash
   # Use bulk loader for large datasets
   python scripts/bulk_loader.py
   
   # For manual bulk loading, use settings from:
   psql -d insideestates_app -f scripts/bulk_load_settings.sql
   ```

## Database Schema

To be defined based on data sources.

## Next Steps

1. Define data sources and requirements
2. Design database schema based on data structure
3. Create tables and indexes
4. Create data loading/migration scripts
5. Build Flask application with routes and templates
6. Implement search and analytics features
7. Deploy to production environment