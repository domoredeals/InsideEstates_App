"""
Bright Data (Luminati) Proxy Configuration
==========================================

To use the proxy-enabled scraper, you need a Bright Data account.
Sign up at: https://brightdata.com

Once you have an account:
1. Create a new proxy zone (recommended: Datacenter proxies)
2. Get your credentials from the dashboard
3. Update the values below with your actual credentials
"""

# Your Bright Data credentials
PROXY_USERNAME = "your_username_here"  # e.g., "brd-customer-hl_997fefd5-zone-datacenter"
PROXY_PASSWORD = "your_password_here"  # Your zone password
PROXY_PORT = "22225"  # Usually 22225 for HTTP proxy

# Example configuration (DO NOT USE - these are just examples):
# PROXY_USERNAME = "brd-customer-hl_997fefd5-zone-ch30oct22"
# PROXY_PASSWORD = "kikhwzt80akq"
# PROXY_PORT = "22225"

# Advanced settings (optional)
PROXY_COUNTRY = None  # Set to country code (e.g., "gb") to use specific country IPs
SESSION_TIMEOUT = 300  # Session timeout in seconds
MAX_RETRIES = 3  # Maximum retries per request

# Proxy types available in Bright Data:
# - Datacenter: Fast and affordable, good for most scraping
# - Residential: Real user IPs, best for avoiding detection
# - ISP: Static IPs, good balance of speed and legitimacy
# - Mobile: Mobile carrier IPs, highest legitimacy but expensive