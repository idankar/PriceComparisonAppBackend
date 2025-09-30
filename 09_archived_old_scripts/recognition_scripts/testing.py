#!/usr/bin/env python3
"""Quick diagnostic script to identify issues in the training pipeline"""

import torch
import json
from pathlib import Path

def diagnose_dataset(jsonl_path):
    """Analyze the dataset for potential issues"""
    print("🔍 Analyzing dataset...")
    
    product_counts = {}
    text_lengths = []
    items_with_text = 0
    total_items = 0
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            item = json.loads(line.strip())
            total_items += 1
            
            # Count products
            pid = item['training_item_id']
            product_counts[pid] = product_counts.get(pid, 0) + 1
            
            # Check text
            if 'text_for_embedding' in item and item['text_for_embedding'].strip():
                items_with_text += 1
                text_lengths.append(len(item['text_for_embedding']))
    
    print(f"Total items: {total_items}")
    print(f"Unique products: {len(product_counts)}")
    print(f"Items with text: {items_with_text} ({100*items_with_text/total_items:.1f}%)")
    print(f"Average text length: {sum(text_lengths)/len(text_lengths):.1f} chars")
    
    # Check for single-sample products
    single_sample_products = sum(1 for count in product_counts.values() if count == 1)
    print(f"Products with only 1 sample: {single_sample_products} ({100*single_sample_products/len(product_counts):.1f}%)")
    
    # Distribution of samples per product
    counts = list(product_counts.values())
    print(f"Samples per product - Min: {min(counts)}, Max: {max(counts)}, Avg: {sum(counts)/len(counts):.1f}")
    
    return product_counts

def test_model_initialization():
    """Test if models initialize properly"""
    print("\n🧠 Testing model initialization...")
    
    try:
        from transformers import AutoTokenizer, AutoModel
        tokenizer = AutoTokenizer.from_pretrained('xlm-roberta-base')
        print("✓ Tokenizer loaded successfully")
        
        # Test tokenization
        test_text = "במבה אסם חטיף"
        tokens = tokenizer(test_text, return_tensors='pt')
        print(f"✓ Hebrew tokenization works: {tokens['input_ids'].shape}")
        
    except Exception as e:
        print(f"✗ Tokenizer error: {e}")
    
    try:
        # Test a simple forward pass
        model = AutoModel.from_pretrained('xlm-roberta-base')
        with torch.no_grad():
            output = model(**tokens)
        print(f"✓ Model forward pass works: {output.last_hidden_state.shape}")
    except Exception as e:
        print(f"✗ Model error: {e}")

def check_batch_statistics(batch_size=32, num_products=1000):
    """Calculate expected positive pairs per batch"""
    print(f"\n📊 Batch statistics (batch_size={batch_size}, products={num_products}):")
    
    # Probability of having at least one positive pair
    prob_no_positive = (1 - 1/num_products) ** (batch_size - 1)
    prob_has_positive = 1 - prob_no_positive
    
    print(f"Probability of positive pair in batch: {prob_has_positive:.4f}")
    print(f"Expected batches without positive pairs: {(1-prob_has_positive)*100:.1f}%")
    
    if prob_has_positive < 0.5:
        print("⚠️ WARNING: Most batches won't have positive pairs!")
        print("Solutions:")
        print("  1. Increase batch size")
        print("  2. Use a custom sampler to ensure positive pairs")
        print("  3. Use a different loss function")

def main():
    # Update this path to your dataset
    jsonl_path = '/Users/noa/Desktop/PriceComparisonApp/training_data.jsonl'
    
    if Path(jsonl_path).exists():
        product_counts = diagnose_dataset(jsonl_path)
        test_model_initialization()
        check_batch_statistics(batch_size=32, num_products=len(product_counts))
    else:
        print(f"Dataset not found at {jsonl_path}")

if __name__ == "__main__":
    main()