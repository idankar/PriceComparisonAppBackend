import torch
import open_clip
from open_clip import get_tokenizer

# Load the model and tokenizer
model_name = "ViT-B-32"
pretrained = "laion2b_s34b_b79k"

_, _, _ = open_clip.create_model_and_transforms(model_name, pretrained=pretrained)
tokenizer = get_tokenizer(model_name)

# Hebrew text examples
hebrew_texts = [
    "שם מוצר: חלב",
    "שם מוצר: יוגורט",
    "שם מוצר: גבינה",
    "שם מוצר: לחם",
    "מותג: תנובה",
    "מותג: שטראוס",
    "חלב טרי תנובה",
    "יוגורט וניל יופלה"
]

# English text examples for comparison
english_texts = [
    "product: milk",
    "product: yogurt",
    "product: cheese",
    "product: bread",
    "brand: Tnuva",
    "brand: Strauss",
    "Tnuva fresh milk",
    "Yoplait vanilla yogurt"
]

print("Testing CLIP tokenization for Hebrew vs. English text")
print("-" * 60)

# Tokenize Hebrew texts
print("\nHebrew text tokenization:")
for text in hebrew_texts:
    tokens = tokenizer([text])
    print(f"\nText: {text}")
    print(f"Number of tokens: {tokens.shape[1]}")
    
    # Get the first few token IDs
    token_ids = tokens[0, :10].tolist()
    print(f"First 10 token IDs: {token_ids}")
    
    # Check if these are BPE tokens from English vocab
    token_count = tokens.shape[1]
    average_token_per_char = token_count / len(text)
    print(f"Tokens per character: {average_token_per_char:.2f}")

# Tokenize English texts
print("\nEnglish text tokenization:")
for text in english_texts:
    tokens = tokenizer([text])
    print(f"\nText: {text}")
    print(f"Number of tokens: {tokens.shape[1]}")
    
    # Get the first few token IDs
    token_ids = tokens[0, :10].tolist()
    print(f"First 10 token IDs: {token_ids}")
    
    # Check efficiency
    token_count = tokens.shape[1]
    average_token_per_char = token_count / len(text)
    print(f"Tokens per character: {average_token_per_char:.2f}")

# Compare similarity between Hebrew texts
print("\nSimilarity between Hebrew texts:")
hebrew_tokens = tokenizer(hebrew_texts)
with torch.no_grad():
    hebrew_features = open_clip.model.encode_text(hebrew_tokens)
    hebrew_features = hebrew_features / hebrew_features.norm(dim=-1, keepdim=True)
    
    # Calculate similarity matrix
    similarity = hebrew_features @ hebrew_features.T
    
    # Print similarity matrix
    for i, text1 in enumerate(hebrew_texts):
        for j, text2 in enumerate(hebrew_texts):
            if i < j:  # Only print half of the matrix
                sim_value = similarity[i, j].item()
                print(f"{text1} <-> {text2}: {sim_value:.4f}")

# Compare similarity between English texts
print("\nSimilarity between English texts:")
english_tokens = tokenizer(english_texts)
with torch.no_grad():
    english_features = open_clip.model.encode_text(english_tokens)
    english_features = english_features / english_features.norm(dim=-1, keepdim=True)
    
    # Calculate similarity matrix
    similarity = english_features @ english_features.T
    
    # Print similarity matrix
    for i, text1 in enumerate(english_texts):
        for j, text2 in enumerate(english_texts):
            if i < j:  # Only print half of the matrix
                sim_value = similarity[i, j].item()
                print(f"{text1} <-> {text2}: {sim_value:.4f}")

print("\nConclusion:")
print("If Hebrew tokenization shows unusually high token counts per character")
print("or if similarity values don't reflect semantic relationships,")
print("then CLIP might not be handling Hebrew properly.")