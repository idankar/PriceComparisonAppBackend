Metriks App - Database Schema & Data Normalization Guide
1. Introduction
This document provides a comprehensive overview of the PostgreSQL database schema and the data processing workflows for the Metriks Price Comparison App. It is intended as a foundational reference for understanding data relationships, developing new features, and integrating frontend services.

The schema is designed to support multiple retailers, including both grocery and pharmacy chains, each with numerous physical or online stores. The core function is to track products and their prices over time, providing users with accurate price comparisons.

A key feature of our backend is a sophisticated data normalization pipeline that uses Large Language Models (LLMs) to clean and consolidate product data, ensuring that searches for a product like "Advil 200mg" yield comparable results from all retailers, regardless of how they individually name the item.

2. Entity Relationship Summary
The core data flow of the database is as follows:

Retailers and Stores: The top-level entity is a retailer. Each retailer can have multiple stores.

Master Product Catalog: The products table acts as the master catalog. After the normalization process, this table holds canonical, retailer-agnostic information for each unique product, identified by its masterproductid.

Listings: A retailerproductlisting links a canonical master product to a specific store, representing that product being sold at that location.

Price History: Each listing is associated with one or more entries in the prices table, allowing for a full price history of an item at a specific store.

3. Data Normalization Workflow
The raw product data scraped from retailers is messy, inconsistent, and contains many duplicates. To make it useful, we implement a multi-phase, LLM-powered normalization pipeline. The immediate goal of this pipeline is to consolidate the pharmacy product catalog.

3.1. Phase 1: Pharmacy Product Classification
Purpose: To filter the entire raw product database (approx. 135k items) and isolate only "pharmacy products" (health, beauty, baby, hygiene, etc.).

Process:

The pharmacy_classifier.py script performs an initial keyword-based filter to create a candidate set.

It uses an LLM (e.g., gpt-4o-mini) to classify each candidate as either "pharmacy" or "non-pharmacy".

Output: A pharmacy_only_masterproductids.csv file containing the master IDs and names of all products classified as pharmacy items.

3.2. Phase 2: Pharmacy Product Consolidation
Purpose: To identify and group identical pharmacy products into single canonical entities. For example, grouping "Dior Vernis Nail Polish #100" and "Dior Vernis Lacquer 100" into a single entity.

Process:

The data_consolidation_pharmacy.py script ingests the pharmacy-only products.

Attribute-First Filtering: It performs sophisticated feature extraction to parse attributes like size, units, SPF, and variant/shade codes. It then uses a series of strict, attribute-based rules to automatically reject dissimilar pairs (e.g., if SPF values or sizes differ).

LLM Verification: Pairs that are not obviously different but are not perfect matches (the "grey area") are sent to an advanced LLM (e.g., gpt-4o) with a strict prompt to determine if they are the exact same product.

Output: A duplicate_groups_pharmacy.csv file where each group_id represents a single canonical product and lists all the masterproductids that belong to it.

3.3. Phase 3: Database Implementation
Purpose: To update the live database with the consolidation results.

Process: This is a critical, multi-step database migration:

A temporary canonical_masterproductid column is added to the products table.

The duplicate_groups_pharmacy.csv file is used to update this column, mapping each product to its chosen canonical ID.

The retailerproductlistings table is updated to point to the new canonical IDs.

All non-canonical (duplicate) entries are deleted from the products table.

The temporary column is removed.
This process results in a clean, consolidated products table.

4. Frontend Integration & API Logic
This section outlines the intended use of the consolidated database for a frontend search engine.

Master Search Index: The consolidated products table is the primary resource for user searches. A frontend search for a product name should query the productname column of this table.

Search Flow:

A user types a search query (e.g., "Nurofen for kids").

The frontend sends this query to a backend API endpoint (e.g., /api/search?q=Nurofen%20for%20kids).

The API performs a LIKE or full-text search against the products.productname column to find matching canonical products.

The API retrieves the masterproductid for each search result.

Price Fetching Flow:

When a user clicks on a search result (a canonical product), the frontend requests all prices for that product's masterproductid.

The backend API uses the masterproductid to query the retailerproductlistings table to find all listingids associated with that product.

It then joins with the prices and stores tables to retrieve the current price, store name, and retailer for every location that sells the item.

The API returns a JSON object containing the canonical product details (productname, brand, image_url) and a list of all available prices and locations.

5. Table Schema Details
5.1. retailers
Stores top-level information about each retail chain.

Key Columns: retailerid (PK), retailername.

Retailers Tracked: Hazi Hinam, Mega/Carrefour, Rami Levy, Shufersal, Victory, Yohananof, Super-Pharm, Be Pharm, Good Pharm.

Note: A significant data cleanup task was performed to separate "Be Pharm" stores and products from the "Shufersal" retailerid into their own distinct retailer entry.

5.2. stores
Stores information about individual store branches or online channels.

Key Columns: storeid (PK), retailerid (FK), storename, address, city.

Store Types (storetype): 1 (Physical), 2 (Online).

5.3. products
The master product catalog. Post-normalization, this table contains clean, canonical products.

Key Columns: masterproductid (PK), barcode (Unique), productname, brand, isweighted, image_url (newly added).

5.4. categories
Stores product categories hierarchically.

Status: This table is currently empty and not in use for the MVP.

5.5. retailerproductlistings
Links a master product to a specific offering at a retailer's store.

Key Columns: listingid (PK), masterproductid (FK), retailerid (FK), storeid (FK), retaileritemcode.

5.6. prices
Stores current and historical price points for each listing.

Key Columns: priceid (PK), listingid (FK), price, unitprice, priceupdatetimestamp.

5.7. promotions & promotionproductlinks
Stores details about special offers and links them to product listings.

Status: This data is captured but may not be fully implemented in the MVP.