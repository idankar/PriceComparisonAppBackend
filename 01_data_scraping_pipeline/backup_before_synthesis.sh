#!/bin/bash

# Be Pharm Database Backup Script
# Run this BEFORE executing the price synthesis script

set -e  # Exit on error

echo "=================================================="
echo "BE PHARM DATABASE BACKUP"
echo "=================================================="

# Set variables
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DB_NAME="price_comparison_app_v2"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"
BACKUP_DIR="./backups"

# Create backup directory if it doesn't exist
mkdir -p $BACKUP_DIR

# Export password to avoid prompt
export PGPASSWORD="***REMOVED***"

echo "Creating backups at $BACKUP_DIR"
echo ""

# 1. Full database backup
FULL_BACKUP="$BACKUP_DIR/full_db_backup_${TIMESTAMP}.sql"
echo "1. Creating full database backup..."
pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f "$FULL_BACKUP"
echo "   ✓ Full backup saved to: $FULL_BACKUP"

# 2. Be Pharm specific data backup
BE_PHARM_BACKUP="$BACKUP_DIR/be_pharm_data_${TIMESTAMP}.sql"
echo ""
echo "2. Creating Be Pharm specific backup..."
pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME \
    -t "retailer_products" \
    -t "prices" \
    -t "stores" \
    -t "products" \
    --inserts \
    -f "$BE_PHARM_BACKUP"
echo "   ✓ Be Pharm backup saved to: $BE_PHARM_BACKUP"

# 3. Compressed backup
COMPRESSED_BACKUP="$BACKUP_DIR/full_db_backup_${TIMESTAMP}.sql.gz"
echo ""
echo "3. Creating compressed backup..."
pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME | gzip > "$COMPRESSED_BACKUP"
echo "   ✓ Compressed backup saved to: $COMPRESSED_BACKUP"

# Get backup sizes
echo ""
echo "Backup sizes:"
ls -lh $BACKUP_DIR/*${TIMESTAMP}* | awk '{print "   - " $9 ": " $5}'

echo ""
echo "=================================================="
echo "BACKUP COMPLETE!"
echo "=================================================="
echo ""
echo "To restore from backup if needed:"
echo "  Full restore: psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME < $FULL_BACKUP"
echo ""
echo "Now safe to run synthesis:"
echo "  Dry run:     python 01_data_scraping_pipeline/be_pharm_price_synthesis.py --dry-run --sql-approach"
echo "  Production:  python 01_data_scraping_pipeline/be_pharm_price_synthesis.py --sql-approach"
echo ""