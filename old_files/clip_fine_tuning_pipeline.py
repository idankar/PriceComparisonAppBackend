import os
import json
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from transformers import CLIPProcessor, CLIPModel
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint
from PIL import Image
import random
import numpy as np
from torchvision import transforms
import albumentations as A
from albumentations.pytorch import ToTensorV2

class ProductDataset(Dataset):
    def __init__(self, data_dir, manifest_path, split='train'):
        """
        Dataset for product images with contrastive learning support
        
        Args:
            data_dir: Root directory for product data
            manifest_path: Path to JSON manifest file
            split: 'train' or 'val'
        """
        self.data_dir = data_dir
        with open(manifest_path, 'r', encoding='utf-8') as f:
            self.manifest = json.load(f)
        
        self.products = self.manifest[split]
        self.product_ids = list(self.products.keys())
        
        # Map product IDs to indices for faster lookup
        self.product_id_to_idx = {pid: idx for idx, pid in enumerate(self.product_ids)}
        
        # Create image paths list
        self.image_paths = []
        self.image_labels = []
        self.image_product_ids = []
        
        for pid in self.product_ids:
            product_info = self.products[pid]
            product_dir = os.path.join(data_dir, pid)
            
            for img_file in product_info['images']:
                self.image_paths.append(os.path.join(product_dir, img_file))
                self.image_labels.append(self.product_id_to_idx[pid])
                self.image_product_ids.append(pid)
        
        # Load CLIP processor
        self.processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
        
        # Data augmentation
        if split == 'train':
            self.transform = A.Compose([
                A.RandomResizedCrop(height=224, width=224, scale=(0.8, 1.0)),
                A.ColorJitter(brightness=0.4, contrast=0.4, saturation=0.4, hue=0.1, p=0.8),
                A.HorizontalFlip(p=0.5),
                A.Rotate(limit=15, p=0.5),
                A.GaussianBlur(blur_limit=(3, 7), p=0.3),
                A.Perspective(scale=(0.05, 0.1), p=0.3),
                A.ToGray(p=0.1),
                A.Normalize(mean=(0.48145466, 0.4578275, 0.40821073), 
                           std=(0.26862954, 0.26130258, 0.27577711)),
                ToTensorV2()
            ])
        else:
            self.transform = A.Compose([
                A.Resize(height=224, width=224),
                A.Normalize(mean=(0.48145466, 0.4578275, 0.40821073), 
                           std=(0.26862954, 0.26130258, 0.27577711)),
                ToTensorV2()
            ])
    
    def __len__(self):
        return len(self.image_paths)
    
    def __getitem__(self, idx):
        img_path = self.image_paths[idx]
        label = self.image_labels[idx]
        product_id = self.image_product_ids[idx]
        
        # Load image
        image = Image.open(img_path).convert("RGB")
        image_np = np.array(image)
        
        # Apply transforms
        transformed = self.transform(image=image_np)
        image_tensor = transformed["image"]
        
        # Get product metadata
        metadata_path = os.path.join(self.data_dir, product_id, "metadata.json")
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        # Create bilingual text for CLIP
        text_en = f"{metadata['brand']} {metadata['name']}"
        text_he = metadata.get('name_he', '')
        
        # Combine both languages when available
        if text_he:
            text = f"{text_en} {text_he}"
        else:
            text = text_en
            
        # Process text through CLIP processor
        text_inputs = self.processor(text=text, return_tensors="pt", padding=True)
        text_inputs = {k: v.squeeze(0) for k, v in text_inputs.items()}
        
        return {
            "image": image_tensor,
            "text_inputs": text_inputs,
            "label": label,
            "product_id": product_id
        }

    def get_hard_negatives(self, anchor_idx, k=5):
        """Get hard negatives for an anchor image"""
        anchor_label = self.image_labels[anchor_idx]
        anchor_product = self.products[self.image_product_ids[anchor_idx]]
        
        # Find products with similar attributes but different labels
        similar_products = []
        for pid in self.product_ids:
            if self.product_id_to_idx[pid] == anchor_label:
                continue
                
            product = self.products[pid]
            # Check if category is the same
            if product['category'] == anchor_product['category']:
                # Check if brand is the same
                if product['brand'] == anchor_product['brand']:
                    similar_products.append(pid)
                    
        # If not enough similar products, add random ones
        if len(similar_products) < k:
            remaining = k - len(similar_products)
            all_other_products = [pid for pid in self.product_ids 
                                 if self.product_id_to_idx[pid] != anchor_label 
                                 and pid not in similar_products]
            random_products = random.sample(all_other_products, min(remaining, len(all_other_products)))
            similar_products.extend(random_products)
        
        # Get random images from similar products
        negative_indices = []
        for pid in similar_products[:k]:
            pid_indices = [i for i, p in enumerate(self.image_product_ids) if p == pid]
            if pid_indices:
                negative_indices.append(random.choice(pid_indices))
        
        return negative_indices


class TripletLoss(nn.Module):
    def __init__(self, margin=0.3):
        super(TripletLoss, self).__init__()
        self.margin = margin
        
    def forward(self, anchor, positive, negative):
        distance_positive = (anchor - positive).pow(2).sum(1)
        distance_negative = (anchor - negative).pow(2).sum(1)
        losses = F.relu(distance_positive - distance_negative + self.margin)
        return losses.mean()


class ProductCLIPModel(pl.LightningModule):
    def __init__(self, num_classes, learning_rate=2e-5):
        super().__init__()
        self.save_hyperparameters()
        
        # Load CLIP model
        self.clip_model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
        
        # Freeze certain layers
        for param in self.clip_model.text_model.parameters():
            param.requires_grad = False
            
        # Keep vision model trainable
        for param in self.clip_model.vision_model.parameters():
            param.requires_grad = True
        
        # Add classification head
        self.classifier = nn.Linear(self.clip_model.config.projection_dim, num_classes)
        
        # Triplet loss
        self.triplet_loss = TripletLoss(margin=0.3)
        
        # Learning rate
        self.learning_rate = learning_rate
        
    def forward(self, image, text_inputs=None):
        # Get image embedding
        vision_outputs = self.clip_model.vision_model(
            image,
            output_hidden_states=True,
            return_dict=True
        )
        image_embeds = vision_outputs.pooler_output
        image_features = self.clip_model.visual_projection(image_embeds)
        
        # Get classification prediction
        logits = self.classifier(image_features)
        
        # If text inputs provided, also get text embeddings
        if text_inputs:
            text_outputs = self.clip_model.text_model(
                input_ids=text_inputs['input_ids'],
                attention_mask=text_inputs['attention_mask'],
                output_hidden_states=True,
                return_dict=True
            )
            text_embeds = text_outputs.pooler_output
            text_features = self.clip_model.text_projection(text_embeds)
            
            return image_features, text_features, logits
        
        return image_features, logits
    
    def training_step(self, batch, batch_idx):
        images = batch["image"]
        text_inputs = batch["text_inputs"]
        labels = batch["label"]
        
        # Forward pass
        image_features, text_features, logits = self.forward(images, text_inputs)
        
        # Classification loss
        ce_loss = F.cross_entropy(logits, labels)
        
        # Contrastive loss between image and text
        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        text_features = text_features / text_features.norm(dim=-1, keepdim=True)
        
        # Get similarity scores
        similarity = torch.matmul(image_features, text_features.t())
        
        # Contrastive loss (image to text)
        labels_contrastive = torch.arange(similarity.size(0), device=self.device)
        contrastive_loss = (
            F.cross_entropy(similarity, labels_contrastive) + 
            F.cross_entropy(similarity.t(), labels_contrastive)
        ) / 2
        
        # Combine losses
        loss = ce_loss + contrastive_loss
        
        # Log metrics
        self.log("train_loss", loss)
        self.log("train_ce_loss", ce_loss)
        self.log("train_contrastive_loss", contrastive_loss)
        
        return loss
    
    def validation_step(self, batch, batch_idx):
        images = batch["image"]
        text_inputs = batch["text_inputs"]
        labels = batch["label"]
        
        # Forward pass
        image_features, text_features, logits = self.forward(images, text_inputs)
        
        # Classification accuracy
        preds = torch.argmax(logits, dim=1)
        acc = (preds == labels).float().mean()
        
        # Normalize features for similarity computation
        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        text_features = text_features / text_features.norm(dim=-1, keepdim=True)
        
        # Calculate similarity scores
        similarity = torch.matmul(image_features, text_features.t())
        
        # Calculate top-1 and top-5 retrieval accuracy
        labels_contrastive = torch.arange(similarity.size(0), device=self.device)
        
        # Image to text retrieval
        i2t_top1 = (torch.argmax(similarity, dim=1) == labels_contrastive).float().mean()
        i2t_top5 = 0
        for i in range(similarity.size(0)):
            _, indices = similarity[i].topk(5)
            if labels_contrastive[i] in indices:
                i2t_top5 += 1
        i2t_top5 /= similarity.size(0)
        
        # Text to image retrieval
        t2i_top1 = (torch.argmax(similarity, dim=0) == labels_contrastive).float().mean()
        t2i_top5 = 0
        for i in range(similarity.size(0)):
            _, indices = similarity[:, i].topk(5)
            if labels_contrastive[i] in indices:
                t2i_top5 += 1
        t2i_top5 /= similarity.size(0)
        
        # Log metrics
        self.log("val_acc", acc)
        self.log("val_i2t_top1", i2t_top1)
        self.log("val_i2t_top5", i2t_top5)
        self.log("val_t2i_top1", t2i_top1)
        self.log("val_t2i_top5", t2i_top5)
        
        return {
            "val_acc": acc,
            "val_i2t_top1": i2t_top1,
            "val_i2t_top5": i2t_top5
        }
    
    def configure_optimizers(self):
        # Use different learning rates for different parts of the model
        params = [
            {"params": self.clip_model.vision_model.parameters(), "lr": self.learning_rate},
            {"params": self.clip_model.visual_projection.parameters(), "lr": self.learning_rate},
            {"params": self.classifier.parameters(), "lr": self.learning_rate * 10}
        ]
        
        optimizer = torch.optim.AdamW(params, weight_decay=0.01)
        
        # Learning rate scheduler
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer, T_max=10, eta_min=self.learning_rate / 100
        )
        
        return {
            "optimizer": optimizer,
            "lr_scheduler": {
                "scheduler": scheduler,
                "monitor": "val_i2t_top1"
            }
        }


def train_model(data_dir, manifest_path, output_dir, batch_size=32, num_epochs=20):
    """Train the product recognition model"""
    # Create datasets
    train_dataset = ProductDataset(data_dir, manifest_path, split='train')
    val_dataset = ProductDataset(data_dir, manifest_path, split='val')
    
    # Create dataloaders
    train_loader = DataLoader(
        train_dataset, 
        batch_size=batch_size, 
        shuffle=True, 
        num_workers=4,
        pin_memory=True
    )
    
    val_loader = DataLoader(
        val_dataset, 
        batch_size=batch_size, 
        shuffle=False, 
        num_workers=4,
        pin_memory=True
    )
    
    # Count number of unique products
    num_classes = len(train_dataset.product_ids)
    
    # Create model
    model = ProductCLIPModel(num_classes=num_classes)
    
    # Create checkpointing
    checkpoint_callback = ModelCheckpoint(
        monitor="val_i2t_top1",
        dirpath=output_dir,
        filename="clip-product-{epoch:02d}-{val_i2t_top1:.4f}",
        save_top_k=3,
        mode="max",
    )
    
    # Create trainer
    trainer = pl.Trainer(
        max_epochs=num_epochs,
        accelerator="auto",  # Use GPU if available
        devices=1,
        callbacks=[checkpoint_callback],
        precision=16,  # Use mixed precision for faster training
        gradient_clip_val=1.0
    )
    
    # Train model
    trainer.fit(model, train_loader, val_loader)
    
    # Save final model
    model.eval()
    model.freeze()
    torch.save(model.state_dict(), os.path.join(output_dir, "clip_product_final.pt"))
    
    return model


if __name__ == "__main__":
    # Example usage
    data_dir = "data/products"
    manifest_path = "data/train_manifest.json"
    output_dir = "models/clip_product"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Train model
    model = train_model(data_dir, manifest_path, output_dir)