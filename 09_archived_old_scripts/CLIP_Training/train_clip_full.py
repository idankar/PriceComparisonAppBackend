import os
import json
import torch
import torch.nn as nn
import random
import numpy as np
from torch.utils.data import DataLoader
from torchvision import transforms
from PIL import Image
from tqdm import tqdm
import open_clip
import time
import matplotlib.pyplot as plt
from transformers import AutoModel, AutoTokenizer
import multiprocessing

# --- CONFIGURATION ---
IMG_DIR = "data/augmented_dataset"
METADATA_PATH = os.path.join(IMG_DIR, "metadata.json")
BATCH_SIZE = 32  # Larger batch size for more stable training
MAX_EPOCHS = 100  # Maximum number of epochs to run
EARLY_STOP_TARGET_ACC = 0.90  # Stop training if accuracy reaches this level
EARLY_STOP_PATIENCE = 10  # Stop training if no improvement for this many epochs
LR = 2e-3  # High learning rate for efficient training
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# HeBERT+CLIP Hybrid configuration
VISION_MODEL = "ViT-L-14"
VISION_PRETRAINED = "laion2b_s32b_b82k"
TEXT_MODEL = "avichr/heBERT"
PROJECTION_DIM = 512
SAVE_DIR = "final_model"
MODEL_NAME = "hebert_clip_final"

# Number of workers for data loading
# Setting to 0 for macOS to avoid multiprocessing issues
NUM_WORKERS = 0 if os.name == 'posix' and not torch.cuda.is_available() else 4

# Evaluation settings
EVAL_INTERVAL = 1  # Evaluate after every epoch
EVAL_SAMPLES = 500  # Number of samples to use for evaluation
RANDOM_SEED = 42

# --- HEBERT+CLIP HYBRID MODEL ---
class HeBERTCLIP(nn.Module):
    def __init__(self, vision_model, text_model, projection_dim):
        super(HeBERTCLIP, self).__init__()
        self.vision_model = vision_model
        self.text_model = text_model
        
        # Get dimensions
        vision_output_dim = vision_model.visual.output_dim
        text_output_dim = text_model.config.hidden_size
        
        print(f"Vision output dimension: {vision_output_dim}")
        print(f"Text output dimension: {text_output_dim}")
        
        # Projection layers to align embeddings 
        self.vision_projection = nn.Linear(vision_output_dim, projection_dim)
        self.text_projection = nn.Linear(text_output_dim, projection_dim)
        
        # Initialize projection layers
        nn.init.normal_(self.vision_projection.weight, std=0.02)
        nn.init.normal_(self.text_projection.weight, std=0.02)
        nn.init.zeros_(self.vision_projection.bias)
        nn.init.zeros_(self.text_projection.bias)
    
    def encode_image(self, images):
        # Extract vision features
        vision_features = self.vision_model.encode_image(images)
        # Project to common space
        projected_vision_features = self.vision_projection(vision_features)
        return projected_vision_features
    
    def encode_text(self, text_input_ids, attention_mask=None):
        # Get HeBERT features using mean pooling
        text_outputs = self.text_model(
            input_ids=text_input_ids,
            attention_mask=attention_mask,
            return_dict=True
        )
        
        # Use mean pooling for better representation
        last_hidden_states = text_outputs.last_hidden_state
        
        # Create mask for mean pooling (exclude padding tokens)
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(last_hidden_states.size()).float()
        
        # Sum the embeddings of tokens with attention and divide by the sum of the mask
        sum_embeddings = torch.sum(last_hidden_states * input_mask_expanded, 1)
        sum_mask = input_mask_expanded.sum(1)
        sum_mask = torch.clamp(sum_mask, min=1e-9)
        mean_embeddings = sum_embeddings / sum_mask
        
        # Project to common space
        projected_text_features = self.text_projection(mean_embeddings)
        return projected_text_features
    
    def forward(self, images, text_input_ids, attention_mask=None):
        # Get projected features
        image_features = self.encode_image(images)
        text_features = self.encode_text(text_input_ids, attention_mask)
        
        # Normalize features
        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        text_features = text_features / text_features.norm(dim=-1, keepdim=True)
        
        # Compute logits
        logits_per_image = image_features @ text_features.T
        logits_per_text = logits_per_image.T
        
        return logits_per_image, logits_per_text

# --- DATASET ---
class HeBERTCLIPDataset(torch.utils.data.Dataset):
    def __init__(self, samples, img_dir, transform, tokenizer, max_length=128):
        self.samples = samples
        self.img_dir = img_dir
        self.transform = transform
        self.tokenizer = tokenizer
        self.max_length = max_length
        self.load_errors = 0

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        filename, meta = self.samples[idx]
        image_path = os.path.join(self.img_dir, filename)
        
        # Handle potential file not found errors
        try:
            image = self.transform(Image.open(image_path).convert("RGB"))
        except Exception as e:
            self.load_errors += 1
            if self.load_errors <= 5:
                print(f"\nWarning: Could not open {image_path}: {e}")
            elif self.load_errors == 6:
                print("\nToo many image loading errors, suppressing further messages...")
            
            # Return a random valid image instead
            while True:
                try:
                    random_idx = random.randint(0, len(self.samples)-1)
                    random_filename, random_meta = self.samples[random_idx]
                    random_path = os.path.join(self.img_dir, random_filename)
                    image = self.transform(Image.open(random_path).convert("RGB"))
                    meta = random_meta
                    break
                except:
                    continue
            
        # Ensure we have required fields
        name_he = meta.get('name_he', 'Unknown Product')
        brand = meta.get('brand', 'Unknown Brand')
        
        text = f"שם מוצר: {name_he}\nמותג: {brand}"
        
        # Tokenize text
        encoding = self.tokenizer(
            text,
            truncation=True,
            max_length=self.max_length,
            padding="max_length",
            return_tensors="pt"
        )
        
        # Remove batch dimension added by tokenizer
        input_ids = encoding['input_ids'].squeeze(0)
        attention_mask = encoding['attention_mask'].squeeze(0)
        
        return image, input_ids, attention_mask, idx

# --- SAVE TRAINING HISTORY ---
def plot_training_history(history, save_path):
    epochs = list(range(1, len(history['train_loss']) + 1))
    
    # Create figure with 2 rows, 2 columns
    fig, axs = plt.subplots(2, 2, figsize=(15, 10))
    
    # Plot training and validation loss
    axs[0, 0].plot(epochs, history['train_loss'], 'b-', label='Training Loss')
    axs[0, 0].plot(epochs, history['val_loss'], 'r-', label='Validation Loss')
    axs[0, 0].set_title('Loss Over Epochs')
    axs[0, 0].set_xlabel('Epoch')
    axs[0, 0].set_ylabel('Loss')
    axs[0, 0].legend()
    axs[0, 0].grid(True)
    
    # Plot average accuracy
    axs[0, 1].plot(epochs, history['train_avg_acc'], 'b-', label='Training Accuracy')
    axs[0, 1].plot(epochs, history['val_avg_acc'], 'r-', label='Validation Accuracy')
    axs[0, 1].set_title('Average Accuracy Over Epochs')
    axs[0, 1].set_xlabel('Epoch')
    axs[0, 1].set_ylabel('Accuracy')
    axs[0, 1].legend()
    axs[0, 1].grid(True)
    
    # Plot image-to-text accuracy
    axs[1, 0].plot(epochs, history['train_i2t_acc'], 'b-', label='Training I2T Accuracy')
    axs[1, 0].plot(epochs, history['val_i2t_acc'], 'r-', label='Validation I2T Accuracy')
    axs[1, 0].set_title('Image-to-Text Accuracy Over Epochs')
    axs[1, 0].set_xlabel('Epoch')
    axs[1, 0].set_ylabel('Accuracy')
    axs[1, 0].legend()
    axs[1, 0].grid(True)
    
    # Plot text-to-image accuracy
    axs[1, 1].plot(epochs, history['train_t2i_acc'], 'b-', label='Training T2I Accuracy')
    axs[1, 1].plot(epochs, history['val_t2i_acc'], 'r-', label='Validation T2I Accuracy')
    axs[1, 1].set_title('Text-to-Image Accuracy Over Epochs')
    axs[1, 1].set_xlabel('Epoch')
    axs[1, 1].set_ylabel('Accuracy')
    axs[1, 1].legend()
    axs[1, 1].grid(True)
    
    plt.tight_layout()
    plt.savefig(save_path)
    print(f"Training history plot saved to {save_path}")

# --- EVALUATION FUNCTION ---
def evaluate(model, dataloader, device):
    model.eval()
    val_loss = 0.0
    val_i2t_correct = 0
    val_t2i_correct = 0
    val_total = 0
    
    with torch.no_grad():
        for images, input_ids, attention_mask, indices in tqdm(dataloader, desc="Evaluating"):
            images = images.to(device)
            input_ids = input_ids.to(device)
            attention_mask = attention_mask.to(device)
            
            # Forward pass
            logits_per_image, logits_per_text = model(images, input_ids, attention_mask)
            
            # Calculate loss
            labels = torch.arange(images.size(0), device=device)
            loss_i2t = loss_fn(logits_per_image, labels)
            loss_t2i = loss_fn(logits_per_text, labels)
            loss = (loss_i2t + loss_t2i) / 2
            
            val_loss += loss.item()
            
            # Calculate accuracy
            pred_i2t = torch.argmax(logits_per_image, dim=1)
            pred_t2i = torch.argmax(logits_per_text, dim=1)
            
            val_i2t_correct += (pred_i2t == labels).sum().item()
            val_t2i_correct += (pred_t2i == labels).sum().item()
            val_total += labels.size(0)
    
    # Return average loss and accuracies
    i2t_acc = val_i2t_correct / val_total
    t2i_acc = val_t2i_correct / val_total
    avg_acc = (i2t_acc + t2i_acc) / 2
    
    return val_loss / len(dataloader), i2t_acc, t2i_acc, avg_acc

def main():
    # --- ENSURE SAVE DIR EXISTS ---
    os.makedirs(SAVE_DIR, exist_ok=True)

    # Set random seed for reproducibility
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)
    torch.manual_seed(RANDOM_SEED)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(RANDOM_SEED)
        torch.backends.cudnn.deterministic = True

    # --- LOAD MODELS ---
    print(f"Loading vision model: {VISION_MODEL}, pretrained: {VISION_PRETRAINED}")
    vision_model, _, preprocess = open_clip.create_model_and_transforms(VISION_MODEL, pretrained=VISION_PRETRAINED)

    print(f"Loading text model: {TEXT_MODEL}")
    text_model = AutoModel.from_pretrained(TEXT_MODEL)
    tokenizer = AutoTokenizer.from_pretrained(TEXT_MODEL)

    # Create the HeBERT+CLIP model
    model = HeBERTCLIP(vision_model, text_model, PROJECTION_DIM)
    model = model.to(DEVICE)
    print(f"Created HeBERT+CLIP model with projection dimension: {PROJECTION_DIM}")
    print(f"Using device: {DEVICE}")

    # --- LOAD METADATA ---
    print(f"\n📂 Loading metadata from {METADATA_PATH}")
    with open(METADATA_PATH, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    samples = list(metadata.items())
    print(f"📊 Total dataset size: {len(samples)} samples")

    # Shuffle and split into train and validation
    random.shuffle(samples)
    val_size = min(EVAL_SAMPLES, int(len(samples) * 0.1))  # 10% for validation or EVAL_SAMPLES, whichever is smaller
    train_samples = samples[val_size:]
    val_samples = samples[:val_size]
    print(f"📊 Train samples: {len(train_samples)}, Validation samples: {len(val_samples)}")

    # --- PREPARE DATA ---
    train_dataset = HeBERTCLIPDataset(train_samples, IMG_DIR, preprocess, tokenizer)
    val_dataset = HeBERTCLIPDataset(val_samples, IMG_DIR, preprocess, tokenizer)

    train_dataloader = DataLoader(
        train_dataset, 
        batch_size=BATCH_SIZE, 
        shuffle=True, 
        num_workers=NUM_WORKERS,
        pin_memory=DEVICE != "cpu"
    )

    val_dataloader = DataLoader(
        val_dataset,
        batch_size=BATCH_SIZE * 2,  # Larger batch size for validation
        shuffle=False,
        num_workers=NUM_WORKERS,
        pin_memory=DEVICE != "cpu"
    )

    # --- TRAINING CONFIGURATION ---
    # Train all parameters for maximum performance
    all_params = list(model.parameters())

    # Calculate trainable parameters
    total_params = sum(p.numel() for p in model.parameters())
    trainable_param_count = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"📊 Total parameters: {total_params:,}")
    print(f"📊 Trainable parameters: {trainable_param_count:,} ({100*trainable_param_count/total_params:.2f}%)")

    # Setup optimizer with weight decay
    global optimizer, scheduler, loss_fn
    optimizer = torch.optim.AdamW(all_params, lr=LR, weight_decay=0.01)

    # Use OneCycleLR scheduler for faster convergence
    scheduler = torch.optim.lr_scheduler.OneCycleLR(
        optimizer,
        max_lr=LR,
        steps_per_epoch=len(train_dataloader),
        epochs=MAX_EPOCHS,  # Use maximum possible epochs for scheduler
        pct_start=0.1,  # Spend 10% of training time warming up
        div_factor=10,  # Start with LR/10
        final_div_factor=100  # End with LR/1000
    )

    loss_fn = nn.CrossEntropyLoss()

    # --- TRAINING LOOP ---
    print("\n🚀 Starting HeBERT+CLIP training on full dataset...")
    print(f"⚙️ Device: {DEVICE}")
    print(f"⚙️ Batch size: {BATCH_SIZE}")
    print(f"⚙️ Learning rate: {LR}")
    print(f"⚙️ Maximum epochs: {MAX_EPOCHS}")
    print(f"⚙️ Early stopping target accuracy: {EARLY_STOP_TARGET_ACC:.2f}")
    print(f"⚙️ Early stopping patience: {EARLY_STOP_PATIENCE} epochs")
    print(f"⚙️ Training on ALL parameters (no freezing)")
    print(f"⚙️ Number of workers: {NUM_WORKERS}")

    # Initialize tracking variables
    history = {
        'train_loss': [],
        'val_loss': [],
        'train_i2t_acc': [],
        'train_t2i_acc': [],
        'train_avg_acc': [],
        'val_i2t_acc': [],
        'val_t2i_acc': [],
        'val_avg_acc': [],
        'learning_rates': []
    }

    # Initial evaluation (epoch 0)
    print("\n📊 Evaluating initial model performance...")
    val_loss, val_i2t_acc, val_t2i_acc, val_avg_acc = evaluate(model, val_dataloader, DEVICE)
    print(f"Initial validation - Loss: {val_loss:.4f}, I2T Acc: {val_i2t_acc:.4f}, T2I Acc: {val_t2i_acc:.4f}, Avg Acc: {val_avg_acc:.4f}")

    # Initialize best metrics
    best_val_acc = val_avg_acc
    best_val_i2t = val_i2t_acc
    best_val_t2i = val_t2i_acc
    best_epoch = 0
    patience_counter = 0
    reached_target = False

    # Training loop
    for epoch in range(MAX_EPOCHS):
        epoch_start_time = time.time()
        model.train()
        train_loss = 0.0
        train_i2t_correct = 0
        train_t2i_correct = 0
        train_total = 0
        
        # Initialize progress bar
        progress_bar = tqdm(train_dataloader, desc=f"Epoch {epoch+1}/{MAX_EPOCHS}")
        
        for batch_idx, (images, input_ids, attention_mask, indices) in enumerate(progress_bar):
            # Move data to device
            images = images.to(DEVICE)
            input_ids = input_ids.to(DEVICE)
            attention_mask = attention_mask.to(DEVICE)
            
            # Forward pass
            logits_per_image, logits_per_text = model(images, input_ids, attention_mask)
            
            # Calculate loss
            labels = torch.arange(images.size(0), device=DEVICE)
            loss_i2t = loss_fn(logits_per_image, labels)
            loss_t2i = loss_fn(logits_per_text, labels)
            loss = (loss_i2t + loss_t2i) / 2
            
            # Backward and optimize
            optimizer.zero_grad()
            loss.backward()
            
            # Gradient clipping to prevent exploding gradients
            torch.nn.utils.clip_grad_norm_(all_params, max_norm=1.0)
            optimizer.step()
            scheduler.step()
            
            # Track metrics
            train_loss += loss.item()
            
            # Calculate training accuracy
            pred_i2t = torch.argmax(logits_per_image, dim=1)
            pred_t2i = torch.argmax(logits_per_text, dim=1)
            
            train_i2t_correct += (pred_i2t == labels).sum().item()
            train_t2i_correct += (pred_t2i == labels).sum().item()
            train_total += labels.size(0)
            
            # Update progress bar
            current_lr = scheduler.get_last_lr()[0]
            batch_i2t_acc = (pred_i2t == labels).sum().item() / labels.size(0)
            batch_t2i_acc = (pred_t2i == labels).sum().item() / labels.size(0)
            batch_avg_acc = (batch_i2t_acc + batch_t2i_acc) / 2
            
            progress_bar.set_postfix({
                'loss': f"{loss.item():.4f}",
                'acc': f"{batch_avg_acc:.4f}",
                'lr': f"{current_lr:.6f}"
            })
        
        # Calculate epoch statistics
        epoch_train_loss = train_loss / len(train_dataloader)
        epoch_train_i2t_acc = train_i2t_correct / train_total
        epoch_train_t2i_acc = train_t2i_correct / train_total
        epoch_train_avg_acc = (epoch_train_i2t_acc + epoch_train_t2i_acc) / 2
        
        # Track current learning rate
        current_lr = scheduler.get_last_lr()[0]
        history['learning_rates'].append(current_lr)
        
        # Evaluate on validation set every EVAL_INTERVAL epochs
        if (epoch + 1) % EVAL_INTERVAL == 0:
            val_loss, val_i2t_acc, val_t2i_acc, val_avg_acc = evaluate(model, val_dataloader, DEVICE)
        
        # Update history
        history['train_loss'].append(epoch_train_loss)
        history['val_loss'].append(val_loss)
        history['train_i2t_acc'].append(epoch_train_i2t_acc)
        history['train_t2i_acc'].append(epoch_train_t2i_acc)
        history['train_avg_acc'].append(epoch_train_avg_acc)
        history['val_i2t_acc'].append(val_i2t_acc)
        history['val_t2i_acc'].append(val_t2i_acc)
        history['val_avg_acc'].append(val_avg_acc)
        
        # Print epoch summary
        epoch_time = time.time() - epoch_start_time
        print(f"\n📊 Epoch {epoch+1} completed in {epoch_time:.1f}s:")
        print(f"   Train Loss: {epoch_train_loss:.4f}, Train Avg Acc: {epoch_train_avg_acc:.4f}")
        print(f"   Train I2T Acc: {epoch_train_i2t_acc:.4f}, Train T2I Acc: {epoch_train_t2i_acc:.4f}")
        print(f"   Val Loss: {val_loss:.4f}, Val Avg Acc: {val_avg_acc:.4f}")
        print(f"   Val I2T Acc: {val_i2t_acc:.4f}, Val T2I Acc: {val_t2i_acc:.4f}")
        print(f"   Learning Rate: {current_lr:.6f}")
        
        # Check if we've reached target accuracy
        if val_avg_acc >= EARLY_STOP_TARGET_ACC:
            reached_target = True
            print(f"🎯 Target accuracy of {EARLY_STOP_TARGET_ACC:.2f} reached!")
            
            # Save best model
            if val_avg_acc > best_val_acc:
                best_val_acc = val_avg_acc
                best_val_i2t = val_i2t_acc
                best_val_t2i = val_t2i_acc
                best_epoch = epoch + 1
                best_model_path = os.path.join(SAVE_DIR, f"{MODEL_NAME}_best.pt")
                torch.save(model.state_dict(), best_model_path)
                print(f"🏆 New best model saved with validation accuracy: {best_val_acc:.4f}")
                
            # Early stopping at target accuracy
            high_confidence_model_path = os.path.join(SAVE_DIR, f"{MODEL_NAME}_target_{int(EARLY_STOP_TARGET_ACC*100)}.pt")
            torch.save(model.state_dict(), high_confidence_model_path)
            print(f"✨ High confidence model saved to {high_confidence_model_path}")
            break
        
        # Save best model
        if val_avg_acc > best_val_acc:
            best_val_acc = val_avg_acc
            best_val_i2t = val_i2t_acc
            best_val_t2i = val_t2i_acc
            best_epoch = epoch + 1
            best_model_path = os.path.join(SAVE_DIR, f"{MODEL_NAME}_best.pt")
            torch.save(model.state_dict(), best_model_path)
            print(f"🏆 New best model saved with validation accuracy: {best_val_acc:.4f}")
            patience_counter = 0  # Reset patience counter when we find a better model
        else:
            patience_counter += 1
            print(f"⏳ No improvement for {patience_counter}/{EARLY_STOP_PATIENCE} epochs")
        
        # Early stopping check for plateau
        if patience_counter >= EARLY_STOP_PATIENCE:
            print(f"🛑 Early stopping triggered. No improvement for {EARLY_STOP_PATIENCE} epochs.")
            break
        
        # Save checkpoint every 5 epochs
        if (epoch + 1) % 5 == 0:
            checkpoint_path = os.path.join(SAVE_DIR, f"{MODEL_NAME}_epoch_{epoch+1}.pt")
            torch.save(model.state_dict(), checkpoint_path)
            print(f"💾 Checkpoint saved: {checkpoint_path}")
        
        # Plot and save training history
        history_path = os.path.join(SAVE_DIR, f"{MODEL_NAME}_training_curve.png")
        plot_training_history(history, history_path)
        
        # Save history as JSON
        history_json_path = os.path.join(SAVE_DIR, f"{MODEL_NAME}_history.json")
        with open(history_json_path, 'w') as f:
            json.dump({
                'train_loss': [float(x) for x in history['train_loss']],
                'val_loss': [float(x) for x in history['val_loss']],
                'train_i2t_acc': [float(x) for x in history['train_i2t_acc']],
                'train_t2i_acc': [float(x) for x in history['train_t2i_acc']],
                'train_avg_acc': [float(x) for x in history['train_avg_acc']],
                'val_i2t_acc': [float(x) for x in history['val_i2t_acc']],
                'val_t2i_acc': [float(x) for x in history['val_t2i_acc']],
                'val_avg_acc': [float(x) for x in history['val_avg_acc']],
                'learning_rates': [float(x) for x in history['learning_rates']],
                'best_epoch': best_epoch,
                'best_accuracy': float(best_val_acc),
                'early_stopped': patience_counter >= EARLY_STOP_PATIENCE,
                'target_reached': reached_target,
            }, f, indent=2)

    # --- FINAL REPORT ---
    print("\n📊 Training Summary:")
    print(f"Total epochs completed: {epoch+1}")
    print(f"Best validation accuracy: {best_val_acc:.4f} (epoch {best_epoch})")
    print(f"Best I2T accuracy: {best_val_i2t:.4f}")
    print(f"Best T2I accuracy: {best_val_t2i:.4f}")
    print(f"Final learning rate: {current_lr:.6f}")

    # Report early stopping result
    if reached_target:
        print(f"\n🎯 Training successfully reached target accuracy of {EARLY_STOP_TARGET_ACC:.2f}!")
        print(f"High confidence model saved to: {high_confidence_model_path}")
    elif patience_counter >= EARLY_STOP_PATIENCE:
        print(f"\n⚠️ Training stopped due to no improvement for {EARLY_STOP_PATIENCE} epochs")
        print(f"Best accuracy achieved: {best_val_acc:.4f}")
    else:
        print(f"\n✅ Training completed all {MAX_EPOCHS} epochs")
        print(f"Best accuracy achieved: {best_val_acc:.4f}")

    print(f"Best model saved to: {best_model_path}")

    # Calculate improvement from initial to best
    initial_val_acc = history['val_avg_acc'][0]
    improvement = best_val_acc - initial_val_acc
    print(f"\n📈 Improvement from initial model:")
    print(f"Initial validation accuracy: {initial_val_acc:.4f}")
    print(f"Best validation accuracy: {best_val_acc:.4f}")
    print(f"Absolute improvement: {improvement:.4f} ({improvement*100:.1f}%)")

    # Save final model if not early stopped
    if not reached_target and patience_counter < EARLY_STOP_PATIENCE:
        final_model_path = os.path.join(SAVE_DIR, f"{MODEL_NAME}_final.pt")
        torch.save(model.state_dict(), final_model_path)
        print(f"\n✅ Final model after {epoch+1} epochs saved to: {final_model_path}")

    print("\n✅ Model training completed successfully!")
    print("To use this model in your app, load the best model from:")
    print(f"  {best_model_path}")

    # Model quality assessment
    if best_val_acc >= 0.90:
        print("\n🌟 EXCELLENT MODEL: Performance exceeds 90% accuracy!")
        print("   Ready for production use in your price comparison app.")
    elif best_val_acc >= 0.80:
        print("\n🌟 VERY GOOD MODEL: High accuracy above 80%")
        print("   Should perform well in your price comparison app.")
    elif best_val_acc >= 0.60:
        print("\n✓ GOOD MODEL: Reasonable accuracy above 60%")
        print("   Useful for your app, but expect some incorrect matches.")
    else:
        print("\n⚠️ MODERATE MODEL: Limited accuracy")
        print("   Consider tweaking hyperparameters or the model architecture for better results.")

if __name__ == "__main__":
    # Add multiprocessing safeguard
    multiprocessing.freeze_support()
    main()