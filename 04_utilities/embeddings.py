import os
import json
import sqlite3
import torch
import torch.nn as nn
import numpy as np
from torchvision import transforms # This might not be explicitly needed if open_clip's preprocess handles it all
from PIL import Image
from tqdm import tqdm
import open_clip # For open_clip.create_model_and_transforms and model
from transformers import AutoModel, AutoTokenizer # For HeBERT

# --- CONFIGURATION ---

# Model Paths and Names (Update these if they differ)
PYTORCH_MODEL_PATH = "final_model_staged/hebert_clip_staged_final_best.pt"
VISION_MODEL_NAME = "ViT-L-14"
VISION_PRETRAINED_DATASET = "laion2b_s32b_b82k"
TEXT_MODEL_NAME = "avichr/heBERT"

# Input data
PRODUCT_JSONL_PATH = "shufersal_database.jsonl" # Updated name
ORIGINAL_IMAGES_DIR = "product_images/"

# Output
OUTPUT_DATABASE_DIR = "database/"
OPTIMIZED_IMAGES_SUBDIR = "product_images_optimized/"
SQLITE_DB_NAME = "products_mobile.sqlite"
TEXT_EMBEDDINGS_NAME = "product_text_embeddings.npy"
IMAGE_EMBEDDINGS_NAME = "product_image_embeddings.npy"

# Processing Parameters
EMBEDDING_DIM = 512 # Confirmed from training script
TEXT_MAX_LENGTH = 128 # From training script
OPTIMIZED_IMAGE_SIZE = (256, 256) # For display images
OPTIMIZED_IMAGE_QUALITY = 80 # For JPG

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# --- Re-define HeBERTCLIP Model (Copied from your training script for loading state_dict) ---
class HeBERTCLIP(nn.Module):
    def __init__(self, vision_model_instance, text_model_instance, projection_dim): # Modified to take instances
        super(HeBERTCLIP, self).__init__()
        self.vision_model = vision_model_instance
        self.text_model = text_model_instance

        vision_output_dim = self.vision_model.visual.output_dim
        text_output_dim = self.text_model.config.hidden_size

        self.vision_projection = nn.Linear(vision_output_dim, projection_dim)
        self.text_projection = nn.Linear(text_output_dim, projection_dim)

        # Weight initialization (important if training, but for loading, structure matters most)
        # nn.init.normal_(self.vision_projection.weight, std=0.02)
        # nn.init.normal_(self.text_projection.weight, std=0.02)
        # nn.init.zeros_(self.vision_projection.bias)
        # nn.init.zeros_(self.text_projection.bias)

    def encode_image(self, images):
        # Ensure images are on the correct device
        images = images.to(next(self.vision_model.parameters()).device) # Get device from model
        vision_features = self.vision_model.encode_image(images)
        projected_vision_features = self.vision_projection(vision_features)
        return projected_vision_features.to(torch.float32) # Ensure FP32

    def encode_text(self, text_input_ids, attention_mask=None):
        # Ensure inputs are on the correct device
        text_input_ids = text_input_ids.to(next(self.text_model.parameters()).device)
        if attention_mask is not None:
            attention_mask = attention_mask.to(next(self.text_model.parameters()).device)

        text_outputs = self.text_model(
            input_ids=text_input_ids,
            attention_mask=attention_mask,
            return_dict=True
        )
        last_hidden_states = text_outputs.last_hidden_state
        if attention_mask is not None:
            input_mask_expanded = attention_mask.unsqueeze(-1).expand(last_hidden_states.size()).float()
            sum_embeddings = torch.sum(last_hidden_states * input_mask_expanded, 1)
            sum_mask = input_mask_expanded.sum(1)
            sum_mask = torch.clamp(sum_mask, min=1e-9)
            mean_embeddings = sum_embeddings / sum_mask
        else: # Fallback if no attention mask (though tokenizer should provide it)
            mean_embeddings = torch.mean(last_hidden_states, dim=1)

        projected_text_features = self.text_projection(mean_embeddings)
        return projected_text_features.to(torch.float32) # Ensure FP32

# --- Helper Functions ---
def setup_output_directories():
    """Creates the necessary output directories."""
    os.makedirs(OUTPUT_DATABASE_DIR, exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DATABASE_DIR, OPTIMIZED_IMAGES_SUBDIR), exist_ok=True)
    print(f"Output directories ensured: {OUTPUT_DATABASE_DIR} and its subdirectories.")

def init_sqlite_db(db_path):
    """Initializes the SQLite database and creates tables."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT, -- This will correspond to embedding index
            product_id_internal TEXT UNIQUE NOT NULL,
            name_he TEXT,
            brand TEXT,
            amount TEXT,
            unit TEXT,
            price REAL,
            original_image_url TEXT,
            optimized_image_filename TEXT, -- Filename in OPTIMIZED_IMAGES_SUBDIR
            categories_json TEXT -- Store categories list as a JSON string
        )
    """)
    # Potentially add FTS5 table for text search on metadata later if needed
    # cursor.execute("""
    #     CREATE VIRTUAL TABLE IF NOT EXISTS products_fts USING fts5(
    #         name_he, brand, content='products', content_rowid='id'
    #     );
    # """)
    # # Trigger to keep FTS table in sync
    # cursor.execute("""
    #     CREATE TRIGGER IF NOT EXISTS products_ai AFTER INSERT ON products BEGIN
    #         INSERT INTO products_fts (rowid, name_he, brand)
    #         VALUES (new.id, new.name_he, new.brand);
    #     END;
    # """)
    # cursor.execute("""
    #     CREATE TRIGGER IF NOT EXISTS products_ad AFTER DELETE ON products BEGIN
    #         DELETE FROM products_fts WHERE rowid=old.id;
    #     END;
    # """)
    # cursor.execute("""
    #     CREATE TRIGGER IF NOT EXISTS products_au AFTER UPDATE ON products BEGIN
    #         UPDATE products_fts SET name_he=new.name_he, brand=new.brand WHERE rowid=new.id;
    #     END;
    # """)
    conn.commit()
    print(f"SQLite database initialized at {db_path}")
    return conn

def load_models_and_preprocessors():
    """Loads the vision model, text model, tokenizers, and preprocessors."""
    print(f"Loading vision model: {VISION_MODEL_NAME} ({VISION_PRETRAINED_DATASET})")
    # The vision_model_instance is the first element from open_clip.create_model_from_pretrained
    # The preprocess_image is the third element
    # We need the base model for HeBERTCLIP, and the preprocess separately.
    vision_model_instance, _, preprocess_image = open_clip.create_model_and_transforms(
        VISION_MODEL_NAME,
        pretrained=VISION_PRETRAINED_DATASET,
        device=DEVICE
    )
    # vision_model_instance = vision_model_instance.to(DEVICE) # ensure device

    print(f"Loading text model: {TEXT_MODEL_NAME}")
    text_model_instance = AutoModel.from_pretrained(TEXT_MODEL_NAME).to(DEVICE)
    tokenizer = AutoTokenizer.from_pretrained(TEXT_MODEL_NAME)

    print("Instantiating HeBERTCLIP composite model...")
    hebert_clip_model = HeBERTCLIP(vision_model_instance, text_model_instance, EMBEDDING_DIM)
    print(f"Loading trained weights from: {PYTORCH_MODEL_PATH}")
    hebert_clip_model.load_state_dict(torch.load(PYTORCH_MODEL_PATH, map_location=DEVICE))
    hebert_clip_model = hebert_clip_model.to(DEVICE)
    hebert_clip_model.eval() # Set to evaluation mode
    print("Models loaded and HeBERTCLIP instantiated successfully.")
    return hebert_clip_model, tokenizer, preprocess_image

def process_product(product_data, hebert_clip_model, tokenizer, image_preprocessor, db_conn, current_index):
    """Processes a single product: generates embeddings, optimizes image, stores metadata."""
    cursor = db_conn.cursor()

    # 1. Construct Text for Embedding
    name_he = product_data.get("name_he", "")
    brand = product_data.get("brand", "")
    categories = product_data.get("categories", [])
    last_category = categories[-1] if categories else ""

    text_parts = []
    if name_he: text_parts.append(name_he)
    if brand: text_parts.append(brand)
    if last_category: text_parts.append(last_category)
    text_to_embed = " ".join(text_parts) # Space separated as discussed

    # 2. Generate Text Embedding
    tokenized_text = tokenizer(
        text_to_embed,
        max_length=TEXT_MAX_LENGTH,
        padding="max_length", # Pad to max_length for consistent input shape
        truncation=True,
        return_tensors="pt"
    )
    input_ids = tokenized_text["input_ids"].to(DEVICE)
    attention_mask = tokenized_text["attention_mask"].to(DEVICE)

    with torch.no_grad():
        text_embedding = hebert_clip_model.encode_text(input_ids, attention_mask)
        text_embedding = text_embedding.squeeze().cpu().numpy() # (1, dim) -> (dim,)

    # 3. Load, Preprocess, and Embed Image
    image_embedding = np.zeros(EMBEDDING_DIM, dtype=np.float32) # Default if image fails
    optimized_image_filename_to_db = None

    original_image_path_suffix = product_data.get("image_filename") # Assuming your JSONL has 'image_filename' like 'shufersal_xxxx.jpg'
    if not original_image_path_suffix: # Fallback if 'image_filename' is not present, try to construct from product_id_internal
        product_id_internal = product_data.get("product_id_internal")
        if product_id_internal:
            original_image_path_suffix = f"{product_id_internal}.jpg" # Common pattern

    if original_image_path_suffix:
        original_image_full_path = os.path.join(ORIGINAL_IMAGES_DIR, original_image_path_suffix)
        try:
            pil_image = Image.open(original_image_full_path).convert("RGB")

            # Generate image embedding
            preprocessed_image_for_vit = image_preprocessor(pil_image).unsqueeze(0).to(DEVICE)
            with torch.no_grad():
                image_embedding_tensor = hebert_clip_model.encode_image(preprocessed_image_for_vit)
                image_embedding = image_embedding_tensor.squeeze().cpu().numpy() # (1, dim) -> (dim,)

            # Optimize and save display image
            optimized_image = pil_image.resize(OPTIMIZED_IMAGE_SIZE, Image.Resampling.LANCZOS)
            # Use a unique filename, e.g., based on product_id_internal or db id
            # Ensuring the filename is filesystem-safe
            safe_filename_base = product_data.get("product_id_internal", f"product_{current_index}")
            safe_filename_base = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in safe_filename_base)
            optimized_image_filename_to_db = f"{safe_filename_base}.jpg" # Save as JPG

            optimized_image_save_path = os.path.join(OUTPUT_DATABASE_DIR, OPTIMIZED_IMAGES_SUBDIR, optimized_image_filename_to_db)
            optimized_image.save(optimized_image_save_path, "JPEG", quality=OPTIMIZED_IMAGE_QUALITY)

        except FileNotFoundError:
            print(f"Warning: Original image not found for {product_data.get('product_id_internal')}: {original_image_full_path}")
        except Exception as e:
            print(f"Warning: Error processing image for {product_data.get('product_id_internal')}: {e}")
    else:
        print(f"Warning: No image filename/ID found for product: {product_data.get('name_he')}")


    # 4. Store Metadata in SQLite
    # Ensure all fields from JSONL are handled, defaulting to None (which becomes SQL NULL)
    sql_data = {
        "product_id_internal": product_data.get("product_id_internal"),
        "name_he": product_data.get("name_he"),
        "brand": product_data.get("brand"),
        "amount": product_data.get("amount"),
        "unit": product_data.get("unit"),
        "price": product_data.get("price"),
        "original_image_url": product_data.get("image_url"), # Assuming this field exists
        "optimized_image_filename": optimized_image_filename_to_db,
        "categories_json": json.dumps(product_data.get("categories", []), ensure_ascii=False) # Store list as JSON string
    }

    # Ensure correct order of columns as defined in CREATE TABLE
    columns = ['product_id_internal', 'name_he', 'brand', 'amount', 'unit', 'price', 'original_image_url', 'optimized_image_filename', 'categories_json']
    values = [sql_data.get(col) for col in columns]

    try:
        cursor.execute(f"""
            INSERT INTO products ({', '.join(columns)})
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, values)
        # The rowid of the inserted row (which is 'id' due to AUTOINCREMENT) will match current_index if we start index from 1 for product processing.
        # Or, more robustly, ensure current_index matches the expected rowid.
        # Since we process sequentially, the auto-incremented 'id' will naturally align with a 0-based index if the table is empty.
    except sqlite3.IntegrityError as e:
        print(f"Error inserting product {sql_data['product_id_internal']} into SQLite: {e}. Might be a duplicate product_id_internal.")
        # If it's a duplicate, we might want to skip adding its embeddings or handle updates
        return None, None # Skip this product's embeddings if DB insert fails

    return text_embedding, image_embedding
# --- Main Script Logic ---
def main():
    print("Starting database and embeddings generation process...")
    setup_output_directories()

    # Initialize SQLite DB
    db_path = os.path.join(OUTPUT_DATABASE_DIR, SQLITE_DB_NAME)
    db_conn = init_sqlite_db(db_path)

    # Load models
    hebert_clip_model, tokenizer, image_preprocessor = load_models_and_preprocessors()

    # Lists to hold all embeddings
    all_text_embeddings = []
    all_image_embeddings = []
    processed_product_ids_for_embeddings = [] # To keep track of which products have embeddings

    # Read PRODUCT_JSONL_PATH
    print(f"Reading products from {PRODUCT_JSONL_PATH}...")
    product_count = 0
    with open(PRODUCT_JSONL_PATH, 'r', encoding='utf-8') as f_jsonl:
        # First pass to count lines for tqdm
        num_lines = sum(1 for line in f_jsonl)
        f_jsonl.seek(0) # Reset file pointer to the beginning

        for i, line in enumerate(tqdm(f_jsonl, total=num_lines, desc="Processing products")):
            try:
                product_data = json.loads(line)
            except json.JSONDecodeError:
                print(f"Skipping malformed JSON line: {line.strip()}")
                continue

            # current_index is i (0-based) which will match the embedding list index
            # and also the SQLite 'id' if the table is fresh and insertions are sequential.
            text_emb, img_emb = process_product(product_data, hebert_clip_model, tokenizer, image_preprocessor, db_conn, i)

            if text_emb is not None and img_emb is not None:
                all_text_embeddings.append(text_emb)
                all_image_embeddings.append(img_emb)
                # We could store product_data.get("product_id_internal") in processed_product_ids_for_embeddings
                # to later ensure consistency, but if process_product returns embeddings,
                # it means DB insertion (implicitly) succeeded for a new row.
            product_count +=1

    # Commit any remaining database transactions
    db_conn.commit()
    db_conn.close()
    print(f"\nProcessed {product_count} products.")
    print(f"Generated {len(all_text_embeddings)} text embeddings and {len(all_image_embeddings)} image embeddings.")

    # Save embeddings as NumPy arrays
    if all_text_embeddings:
        np_text_embeddings = np.array(all_text_embeddings, dtype=np.float32)
        text_emb_path = os.path.join(OUTPUT_DATABASE_DIR, TEXT_EMBEDDINGS_NAME)
        np.save(text_emb_path, np_text_embeddings)
        print(f"Text embeddings saved to {text_emb_path} with shape {np_text_embeddings.shape}")
    else:
        print("No text embeddings were generated to save.")

    if all_image_embeddings:
        np_image_embeddings = np.array(all_image_embeddings, dtype=np.float32)
        img_emb_path = os.path.join(OUTPUT_DATABASE_DIR, IMAGE_EMBEDDINGS_NAME)
        np.save(img_emb_path, np_image_embeddings)
        print(f"Image embeddings saved to {img_emb_path} with shape {np_image_embeddings.shape}")
    else:
        print("No image embeddings were generated to save.")

    print("\nDatabase and embeddings generation complete!")
    print(f"SQLite DB: {os.path.join(OUTPUT_DATABASE_DIR, SQLITE_DB_NAME)}")
    print(f"Optimized Images: {os.path.join(OUTPUT_DATABASE_DIR, OPTIMIZED_IMAGES_SUBDIR)}")

if __name__ == "__main__":
    # It's good practice for PyTorch, especially with CUDA.
    # If you're on Windows and use multiprocessing in helper functions (not the case here directly),
    # multiprocessing.freeze_support() would be needed.
    torch.set_grad_enabled(False) # We are only doing inference
    main()