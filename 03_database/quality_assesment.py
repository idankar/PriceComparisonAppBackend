import pandas as pd
from sqlalchemy import create_engine
import os
import openai
import json

# --- 1. Configuration ---
# --- Please fill in your database password ---
DB_PASSWORD = '***REMOVED***' # IMPORTANT: Replace with your actual password

# --- OpenAI API Key and Model ---
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "***REMOVED***-TSybPJY6Yt9JIJ4k066J06XvV_Vz1E0QasT8jEEx6tZw70bg9RRMZQ-3oBBSjT3BlbkFJ2sUCNgLmgep2y2wrGb39IeJsJiVeEyLqiI_ufaK30DByYW6hkcyDdCx-Gsa0W63EmLZmy-bI4A")
LLM_MODEL = "gpt-4o"

# --- Database Connection Details ---
DB_USER = 'postgres'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'price_comparison_app_v2'

# --- File Path ---
GOLDEN_FILE_PATH = '/Users/noa/Desktop/PriceComparisonApp/superpharm_products_enriched.jsonl'

# --- Analysis Parameters ---
SAMPLE_SIZE = 100 # How many products to sample from the database for the analysis

# --- Main Analysis Function ---
def get_ai_data_quality_report(df_candidate_sample, df_golden_sample):
    """Sends data samples to an LLM and asks for a quality analysis report."""
    
    # Convert dataframes to JSON strings for the prompt
    candidate_json = df_candidate_sample.to_json(orient='records', indent=2, force_ascii=False)
    golden_json = df_golden_sample.to_json(orient='records', indent=2, force_ascii=False)

    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
        prompt = f"""
        You are a Senior Data Quality Analyst with 30 years of experience, specializing in retail and e-commerce data.
        Your task is to provide a detailed analysis and give a final "green light" recommendation for a product dataset.

        I have two datasets:
        1. "Golden Standard Data": A clean, well-structured sample from the market leader, Super-Pharm.
        2. "Candidate Data": A sample from another retailer (Good Pharm) that needs to be compared against the golden standard.

        Here is the Golden Standard Data (from Super-Pharm):
        ```json
        {golden_json}
        ```

        Here is the Candidate Data (from Good Pharm):
        ```json
        {candidate_json}
        ```

        Please provide a report in Markdown format that covers the following points:

        1.  **Completeness:** Are there any obvious missing fields in the Candidate Data? How does its completeness compare to the Golden Standard?
        2.  **Clarity & Fuzziness:** Analyze the `name` and `brand` fields in the Candidate Data. Are the names clear and descriptive, or are they full of abbreviations, internal codes, or other noise compared to the Golden Standard? Provide 2-3 specific examples of "fuzzy" or unclear names.
        3.  **Uniformity:** Assess the consistency of the Candidate Data. Are the naming conventions, capitalization, and formatting for brand and name consistent? How does this uniformity compare to the Golden Standard?
        4.  **Actionable Recommendations:** Based on your analysis, what are the top 2-3 most critical actions that need to be taken to improve the Candidate Data's quality before it can be used for reliable price comparison?
        5.  **Final Verdict (The "Green Light"):** Conclude with a clear verdict. Is this Candidate Data ready for production use ("Green Light"), does it need minor, automatable cleanup ("Yellow Light"), or does it require significant manual review and cleaning ("Red Light")? Justify your verdict.
        """
        
        print("\n--- 🤖 Sending data to LLM for analysis... This may take a moment. ---")
        
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        
        return response.choices[0].message.content

    except Exception as e:
        return f"--- An error occurred while contacting the LLM: {e} ---"

# --- Main Script ---
def main():
    # 1. Connect to Database
    db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    try:
        engine = create_engine(db_url)
        print("✅ Database connection successful.")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    # 2. Load Golden Super-Pharm Data
    try:
        df_golden = pd.read_json(GOLDEN_FILE_PATH, lines=True)
        print(f"✅ Loaded {len(df_golden)} products from the Super-Pharm golden file.")
    except Exception as e:
        print(f"❌ Error loading golden file: {e}")
        return

    # 3. Load a RANDOM sample of Candidate Data from PostgreSQL (Good Pharm)
    try:
        # This query gets a random sample of products listed by Good Pharm
        # The column 'p.productname' is aliased as 'name' to match the rest of the script.
        query = f"""
        SELECT p.masterproductid, p.productname AS name, p.brand
        FROM products p
        JOIN retailerproductlistings l ON p.masterproductid = l.masterproductid
        WHERE l.retailerid = 97
        ORDER BY RANDOM()
        LIMIT {SAMPLE_SIZE};
        """
        df_candidate_sample = pd.read_sql_query(query, engine)
        print(f"✅ Loaded a random sample of {len(df_candidate_sample)} products for Good Pharm.")
    except Exception as e:
        print(f"❌ Error loading data from database: {e}")
        return

    # 4. Get a representative sample from the golden data for context
    df_golden_sample = df_golden.sample(n=SAMPLE_SIZE, random_state=1)

    # 5. Generate and print the AI-powered report
    report = get_ai_data_quality_report(df_candidate_sample, df_golden_sample)
    
    print("\n" + "="*50)
    print("    🤖 AI-Generated Data Quality Report 🤖")
    print("="*50 + "\n")
    print(report)

if __name__ == "__main__":
    main()
