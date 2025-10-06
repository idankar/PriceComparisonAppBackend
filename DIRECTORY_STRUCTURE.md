# Price Comparison App - Directory Structure

## 📁 Project Organization

### 🚀 01_data_scraping_pipeline/
**Primary data collection and ETL pipeline** - EASILY ACCESSIBLE FOR QUICK USE
- ETL scripts for all retailers (Shufersal, Rami Levy, Victory, etc.)
- Unified pharma ETL pipeline
- API data collectors
- Scraping utilities and helpers

### 🌐 02_backend_api/
**Flask API and server components**
- `backend_app.py` - Main Flask application
- API endpoints for product search and location-based queries
- Server configuration

### 🗄️ 03_database/
**Database schemas and management**
- Schema definitions
- Migration scripts
- Database optimization queries
- Maintenance utilities

### 🛠️ 04_utilities/
**Helper scripts and configuration files**
- Configuration JSONs (pharma keywords, insights config)
- Enrichment scripts
- Image processing utilities
- Price normalization tools (GPT-4 analyzer, normalization scripts)
- Brand classification
- Data quality fixes
- General helper functions

### 📍 05_geocoding/
**Location and geocoding services**
- Pharmacy geocoder with Google Maps & Nominatim
- Store location management
- GPS coordinate utilities

### 🔗 06_product_matching/
**Product matching and unification**
- Unified product matcher using OpenAI
- Cross-retailer product matching
- Matching algorithms and models

### 🧪 07_testing/
**Test scripts and validation**
- Unit tests
- Integration tests
- Data validation scripts

### 📊 08_logs/
**Application and process logs**
- Scraping logs
- Error logs
- Processing logs

### 🗃️ 09_archived_old_scripts/
**Deprecated and old files** (for reference only)
- Old scrapers
- Deprecated models
- Legacy code

### 🖼️ 10_product_images_archive/
**Product image backups**
- Retailer-specific image folders
- Historical image data

### 📤 11_data_exports/
**Data outputs and exports**
- JSON exports
- CSV files
- Processed datasets
- Raw scraper outputs
- GPT-4 analysis results
- Data quality audit reports

### 📚 12_documentation/
**Project documentation and reports**
- Technical reports and guides
- Architecture documentation
- Implementation handoff documents
- Feature blueprints
- Data quality reports
- API endpoint documentation
- ETL pipeline guides
- Database schema documentation
- Project status reports

## 🔑 Key Files in Root
- `README.md` - Main project documentation
- `requirements.txt` - Python dependencies
- `fresh_env/` - Virtual environment

## 💡 Quick Access Guide

### To run data scraping:
```bash
cd 01_data_scraping_pipeline/
# Run your preferred ETL script
```

### To start the API server:
```bash
cd 02_backend_api/
python backend_app.py
```

### To run product matching:
```bash
cd 06_product_matching/
python unified_product_matcher.py
```

## 📝 Notes
- The data scraping pipeline (folder 01) is kept easily accessible as requested
- All outdated and unnecessary files have been moved to archives
- Active development files are organized by function
- Large image folders have been consolidated to reduce clutter