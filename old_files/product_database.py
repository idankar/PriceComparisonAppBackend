# product_database.py
from sentence_transformers import SentenceTransformer, util
from PIL import Image
import os
import pickle
import numpy as np
import torch

class ProductDatabase:
    def __init__(self, model_name='clip-ViT-B-16', db_path='data/product_db.pkl'):
        self.model = SentenceTransformer(model_name)
        self.db_path = db_path
        self.products = {}
        self.load_db_if_exists()
    
    def load_db_if_exists(self):
        if os.path.exists(self.db_path):
            print(f"Loading existing database from {self.db_path}")
            with open(self.db_path, 'rb') as f:
                self.products = pickle.load(f)
            print(f"Loaded {len(self.products)} products")
        else:
            print("No existing database found")
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def add_product(self, product_id, image_path, name_en, name_he, brand=None, prices=None):
        """Add a product to the database with multilingual descriptions"""
        try:
            # Load and encode the image
            image = Image.open(image_path).convert('RGB')
            image_embedding = self.model.encode(image)
            
            # Encode all text descriptions (English and Hebrew)
            text_descriptions = [name_en, name_he]
            if brand:
                text_descriptions.append(f"{brand} {name_en}")
                text_descriptions.append(f"{brand} {name_he}")
            
            text_embeddings = self.model.encode(text_descriptions)
            
            # Store product data
            self.products[product_id] = {
                'image_embedding': image_embedding,
                'text_embeddings': text_embeddings,
                'name_en': name_en,
                'name_he': name_he,
                'brand': brand,
                'prices': prices or {},
                'image_path': image_path
            }
            
            # Save the updated database
            self.save_db()
            
            print(f"Added product: {name_en}")
            return True
        except Exception as e:
            print(f"Error adding product {product_id}: {e}")
            return False
    
    def search_by_image(self, image_path, top_k=5):
        """Search for products using an image query"""
        if not self.products:
            print("Database is empty")
            return []
        
        # Load and encode the query image
        image = Image.open(image_path).convert('RGB')
        query_embedding = self.model.encode(image)
        
        # Compare with all products' image embeddings
        results = []
        for product_id, product in self.products.items():
            # Get image similarity
            image_similarity = util.cos_sim(
                query_embedding, 
                product['image_embedding']
            )[0][0].item()
            
            # Get best text match (across all languages)
            text_similarities = util.cos_sim(
                query_embedding.reshape(1, -1), 
                product['text_embeddings']
            )[0]
            best_text_similarity = torch.max(text_similarities).item()
            
            # Combined score (average of image and best text similarity)
            combined_score = (image_similarity + best_text_similarity) / 2
            
            results.append((product_id, combined_score, product))
        
        # Sort by combined similarity (highest first)
        results.sort(key=lambda x: x[1], reverse=True)
        
        return results[:top_k]
    
    def search_by_text(self, query_text, top_k=5):
        """Search for products using text query (works in any language)"""
        if not self.products:
            print("Database is empty")
            return []
        
        # Encode the query text
        query_embedding = self.model.encode(query_text)
        
        # Compare with all products' text embeddings
        results = []
        for product_id, product in self.products.items():
            # Get best text match across all stored descriptions
            text_similarities = util.cos_sim(
                query_embedding.reshape(1, -1), 
                product['text_embeddings']
            )[0]
            best_similarity = torch.max(text_similarities).item()
            
            results.append((product_id, best_similarity, product))
        
        # Sort by similarity (highest first)
        results.sort(key=lambda x: x[1], reverse=True)
        
        return results[:top_k]
    
    def save_db(self):
        """Save the product database to disk"""
        with open(self.db_path, 'wb') as f:
            pickle.dump(self.products, f)
        print(f"Database saved with {len(self.products)} products")