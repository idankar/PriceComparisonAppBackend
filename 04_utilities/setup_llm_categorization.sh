#!/bin/bash

echo "============================================================================="
echo "LLM Category Backfill - Setup Script"
echo "============================================================================="
echo ""

# Check if OpenAI package is installed
echo "📦 Checking OpenAI Python package..."
if python3 -c "import openai" 2>/dev/null; then
    VERSION=$(python3 -c "import openai; print(openai.__version__)")
    echo "✅ OpenAI package is installed (version: $VERSION)"
else
    echo "❌ OpenAI package not found"
    echo "📥 Installing OpenAI package..."
    pip3 install openai

    if [ $? -eq 0 ]; then
        echo "✅ OpenAI package installed successfully"
    else
        echo "❌ Failed to install OpenAI package"
        exit 1
    fi
fi

echo ""
echo "============================================================================="
echo "🔑 OpenAI API Key Setup"
echo "============================================================================="
echo ""

# Check if API key is already set
if [ -n "$OPENAI_API_KEY" ]; then
    echo "✅ OPENAI_API_KEY is already set in environment"
    echo "   Key: ${OPENAI_API_KEY:0:10}..."
else
    echo "❌ OPENAI_API_KEY is not set"
    echo ""
    echo "To get an OpenAI API key:"
    echo "1. Visit: https://platform.openai.com/api-keys"
    echo "2. Sign in or create an account"
    echo "3. Click 'Create new secret key'"
    echo "4. Copy the key (starts with 'sk-')"
    echo ""
    read -p "Do you have an OpenAI API key? (y/n): " HAS_KEY

    if [ "$HAS_KEY" = "y" ] || [ "$HAS_KEY" = "Y" ]; then
        read -p "Enter your OpenAI API key: " API_KEY

        # Add to .env file
        if [ -f .env ]; then
            echo "" >> .env
            echo "# OpenAI API Key for LLM Category Backfill" >> .env
            echo "OPENAI_API_KEY=$API_KEY" >> .env
            echo "✅ API key added to .env file"
        fi

        # Export for current session
        export OPENAI_API_KEY=$API_KEY
        echo "✅ API key set for current session"
        echo ""
        echo "⚠️  Note: To use in future sessions, either:"
        echo "   1. Source the .env file: source .env"
        echo "   2. Or export manually: export OPENAI_API_KEY='your-key'"
    else
        echo ""
        echo "⚠️  You'll need an OpenAI API key to run the categorization script."
        echo "   Get one from: https://platform.openai.com/api-keys"
        echo ""
        exit 1
    fi
fi

echo ""
echo "============================================================================="
echo "📊 Database Status Check"
echo "============================================================================="
echo ""

# Check database status
PGPASSWORD=025655358 /Library/PostgreSQL/17/bin/psql -h localhost -p 5432 -d price_comparison_app_v2 -U postgres -c "
SELECT
    COUNT(*) as total_active,
    COUNT(CASE WHEN category IS NOT NULL AND category <> '' THEN 1 END) as with_category,
    COUNT(CASE WHEN category IS NULL OR category = '' THEN 1 END) as without_category,
    ROUND(100.0 * COUNT(CASE WHEN category IS NOT NULL AND category <> '' THEN 1 END) / COUNT(*), 2) as coverage_pct
FROM canonical_products
WHERE is_active = TRUE;
" 2>/dev/null

echo ""
echo "============================================================================="
echo "✅ Setup Complete!"
echo "============================================================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Test with a small batch (DRY RUN):"
echo "   python3 04_utilities/llm_category_backfill.py --limit 10 --dry-run"
echo ""
echo "2. Process a small batch (REAL):"
echo "   python3 04_utilities/llm_category_backfill.py --limit 50"
echo ""
echo "3. Process all products:"
echo "   python3 04_utilities/llm_category_backfill.py"
echo ""
echo "For more information, see: 04_utilities/LLM_CATEGORY_BACKFILL_README.md"
echo ""
