import os
import torch
import torch.nn as nn
import torch.nn.functional as F
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping
from pytorch_lightning.loggers import TensorBoardLogger
from torch.utils.data import Dataset, DataLoader
from transformers import CLIPProcessor, CLIPModel, CLIPTextModel, CLIPVisionModel
import timm
from PIL import Image
import pandas as pd
import numpy as np
import albumentations as A
from albumentations.pytorch import ToTensorV2
import faiss
import json
from tqdm import tqdm
import random
import argparse
from typing import List, Dict, Optional, Tuple, Union
from pathlib import Path

# Constants
HEBREW_CHARS = set('אבגדהוזחטיכלמנסעפצקרשתךםןףץ')

class ProductDataset(Dataset):
    """
    Dataset for product images with support for contrastive, classification,
    and multilingual training.
    """
    def __init__(
        self,
        data_dir: str,
        manifest_path: str,
        split: str = 'train',
        img_size: int = 224,
        max_text_length: int = 77,
        use_augmentation: bool = True,
        use_hard_negatives: bool = True,
        multilingual: bool = True,
        hard_neg_strategy: str = 'brand_category'
    ):
        """
        Initialize the dataset
        
        Args:
            data_dir: Root directory for product data
            manifest_path: Path to manifest JSON file
            split: 'train' or 'val'
            img_size: Size to resize images to
            max_text_length: Maximum text length for text encoder
            use_augmentation: Whether to use data augmentation
            use_hard_negatives: Whether to use hard negative mining
            multilingual: Whether to use both English and Hebrew product names
            hard_neg_strategy: Strategy for hard negative mining ('random', 'brand', 'category', 'brand_category')
        """
        self.data_dir = Path(data_dir)
        self.img_size = img_size
        self.max_text_length = max_text_length
        self.use_augmentation = use_augmentation
        self.use_hard_negatives = use_hard_negatives
        self.multilingual = multilingual
        self.hard_neg_strategy = hard_neg_strategy
        
        # Load manifest
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        
        if split not in manifest:
            raise ValueError(f"Split '{split}' not found in manifest")
            
        self.products = manifest[split]
        self.product_ids = list(self.products.keys())
        
        # Create mapping from product ID to index for faster lookup
        self.product_id_to_idx = {pid: idx for idx, pid in enumerate(self.product_ids)}
        
        # Create brand and category mapping for hard negative mining
        self.brand_to_products = {}
        self.category_to_products = {}
        
        for pid, info in self.products.items():
            brand = info.get('brand', '').strip().lower()
            category = info.get('category', '').strip().lower()
            
            if brand:
                if brand not in self.brand_to_products:
                    self.brand_to_products[brand] = []
                self.brand_to_products[brand].append(pid)
                
            if category:
                if category not in self.category_to_products:
                    self.category_to_products[category] = []
                self.category_to_products[category].append(pid)
        
        # Create image paths and labels
        self.image_paths = []
        self.image_labels = []
        self.image_product_ids = []
        
        for pid in self.product_ids:
            product_info = self.products[pid]
            product_dir = self.data_dir / pid
            
            for img_file in product_info['images']:
                self.image_paths.append(str(product_dir / img_file))
                self.image_labels.append(self.product_id_to_idx[pid])
                self.image_product_ids.append(pid)
        
        # Load CLIP processor
        self.processor = CLIPProcessor.from_pretrained("openai/clip-vit-b-32")
        
        # Define transformations
        if use_augmentation and split == 'train':
            self.transform = A.Compose([
                # Geometric transformations
                A.RandomResizedCrop(height=img_size, width=img_size, scale=(0.8, 1.0)),
                A.HorizontalFlip(p=0.5),
                A.Rotate(limit=15, p=0.5),
                A.Perspective(scale=(0.05, 0.1), p=0.3),
                
                # Color transformations (simulating different lighting, camera settings)
                A.ColorJitter(brightness=0.4, contrast=0.4, saturation=0.4, hue=0.1, p=0.8),
                A.ToGray(p=0.1),
                
                # Blur and noise (simulating camera quality issues)
                A.OneOf([
                    A.GaussianBlur(blur_limit=(3, 5)),
                    A.MotionBlur(blur_limit=(3, 5)),
                    A.MedianBlur(blur_limit=3),
                ], p=0.3),
                A.GaussNoise(var_limit=(10.0, 30.0), p=0.2),
                
                # Occlusion simulation (for shelf scenarios)
                A.CoarseDropout(max_holes=5, max_height=30, max_width=30, p=0.3),
                
                # Normalization
                A.Normalize(mean=(0.48145466, 0.4578275, 0.40821073), 
                           std=(0.26862954, 0.26130258, 0.27577711)),
                ToTensorV2()
            ])
        else:
            # Validation transformations (minimal, just resize and normalize)
            self.transform = A.Compose([
                A.Resize(height=img_size, width=img_size),
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
        try:
            image = Image.open(img_path).convert("RGB")
            image_np = np.array(image)
            
            # Apply transforms
            transformed = self.transform(image=image_np)
            image_tensor = transformed["image"]
        except Exception as e:
            print(f"Error loading image {img_path}: {e}")
            # Return a blank image in case of error
            image_tensor = torch.zeros((3, self.img_size, self.img_size))
        
        # Get product metadata
        product_info = self.products[product_id]
        
        # Create text descriptions
        name_en = product_info.get('name', '').strip()
        brand = product_info.get('brand', '').strip()
        name_he = product_info.get('name_he', '').strip()
        
        # Check if Hebrew text is present
        has_hebrew = bool(name_he) or any(c in HEBREW_CHARS for c in name_en)
        
        # Create texts for CLIP
        if self.multilingual and has_hebrew:
            # Both languages when available
            text_primary = f"{brand} {name_en}" if brand else name_en
            text_he = name_he if name_he else ''.join(c for c in name_en if c in HEBREW_CHARS)
            
            if text_he:
                text = f"{text_primary} {text_he}"
            else:
                text = text_primary
        else:
            # English only
            text = f"{brand} {name_en}" if brand else name_en
        
        # Process text through CLIP processor
        text_inputs = self.processor(
            text=text, 
            return_tensors="pt", 
            padding="max_length", 
            max_length=self.max_text_length,
            truncation=True
        )
        text_inputs = {k: v.squeeze(0) for k, v in text_inputs.items()}
        
        return {
            "image": image_tensor,
            "text_inputs": text_inputs,
            "label": label,
            "product_id": product_id,
            "has_hebrew": has_hebrew
        }
    
    def get_hard_negatives(self, anchor_idx, k=5):
        """
        Get hard negative samples for contrastive learning
        
        Args:
            anchor_idx: Index of the anchor image
            k: Number of hard negatives to return
        
        Returns:
            List of indices of hard negative samples
        """
        if not self.use_hard_negatives:
            # If hard negatives disabled, return random negatives
            all_other_indices = [i for i in range(len(self)) 
                               if self.image_labels[i] != self.image_labels[anchor_idx]]
            return random.sample(all_other_indices, min(k, len(all_other_indices)))
        
        anchor_label = self.image_labels[anchor_idx]
        anchor_product_id = self.image_product_ids[anchor_idx]
        anchor_product = self.products[anchor_product_id]
        
        anchor_brand = anchor_product.get('brand', '').strip().lower()
        anchor_category = anchor_product.get('category', '').strip().lower()
        
        negative_candidates = []
        
        # Strategy: Find products from same brand or category but different label
        if self.hard_neg_strategy == 'brand' and anchor_brand:
            # Same brand different product
            for pid in self.brand_to_products.get(anchor_brand, []):
                if self.product_id_to_idx[pid] != anchor_label:
                    negative_candidates.append(pid)
                    
        elif self.hard_neg_strategy == 'category' and anchor_category:
            # Same category different product
            for pid in self.category_to_products.get(anchor_category, []):
                if self.product_id_to_idx[pid] != anchor_label:
                    negative_candidates.append(pid)
                    
        elif self.hard_neg_strategy == 'brand_category':
            # First prioritize same brand different product
            if anchor_brand:
                for pid in self.brand_to_products.get(anchor_brand, []):
                    if self.product_id_to_idx[pid] != anchor_label:
                        negative_candidates.append(pid)
            
            # Then add same category different product
            if anchor_category and len(negative_candidates) < k*2:
                for pid in self.category_to_products.get(anchor_category, []):
                    if (self.product_id_to_idx[pid] != anchor_label and 
                        pid not in negative_candidates):
                        negative_candidates.append(pid)
        
        # If not enough candidates, add random products
        if len(negative_candidates) < k:
            remaining = k - len(negative_candidates)
            all_other_products = [pid for pid in self.product_ids 
                                if self.product_id_to_idx[pid] != anchor_label 
                                and pid not in negative_candidates]
            random_products = random.sample(all_other_products, min(remaining, len(all_other_products)))
            negative_candidates.extend(random_products)
        
        # Randomly select k candidates
        selected_products = random.sample(negative_candidates, min(k, len(negative_candidates)))
        
        # Convert product IDs to image indices
        negative_indices = []
        for pid in selected_products:
            # Find images with this product ID
            pid_indices = [i for i, p in enumerate(self.image_product_ids) if p == pid]
            if pid_indices:
                negative_indices.append(random.choice(pid_indices))
        
        # If we couldn't find enough negatives, add random ones
        if len(negative_indices) < k:
            remaining = k - len(negative_indices)
            all_other_indices = [i for i in range(len(self)) 
                               if self.image_labels[i] != anchor_label
                               and i not in negative_indices]
            negative_indices.extend(random.sample(all_other_indices, min(remaining, len(all_other_indices))))
        
        return negative_indices[:k]  # Ensure we return exactly k negatives


class TripletLoss(nn.Module):
    """
    Triplet loss with hard negative mining for learning image embeddings
    """
    def __init__(self, margin=0.3):
        super(TripletLoss, self).__init__()
        self.margin = margin
        
    def forward(self, anchor, positive, negative):
        # Calculate distances
        distance_positive = (anchor - positive).pow(2).sum(1)
        distance_negative = (anchor - negative).pow(2).sum(1)
        
        # Apply margin and relu to get non-negative loss
        losses = F.relu(distance_positive - distance_negative + self.margin)
        return losses.mean()


class CLIPProductModel(pl.LightningModule):
    """
    CLIP-based model for fine-grained product recognition
    """
    def __init__(
        self, 
        num_classes, 
        learning_rate=2e-5,
        clip_model_name="openai/clip-vit-b-32",
        use_triplet_loss=True,
        use_text_contrastive_loss=True,
        use_classification_loss=True,
        triplet_weight=1.0,
        contrastive_weight=1.0,
        classification_weight=1.0,
        freeze_text_encoder=True,
        temperature=0.07
    ):
        """
        Initialize the CLIP-based product recognition model
        
        Args:
            num_classes: Number of product classes
            learning_rate: Learning rate for training
            clip_model_name: CLIP model variant to use
            use_triplet_loss: Whether to use triplet loss for image embeddings
            use_text_contrastive_loss: Whether to use contrastive loss between images and text
            use_classification_loss: Whether to use classification loss
            triplet_weight: Weight for triplet loss
            contrastive_weight: Weight for contrastive loss
            classification_weight: Weight for classification loss
            freeze_text_encoder: Whether to freeze the text encoder
            temperature: Temperature for contrastive loss
        """
        super().__init__()
        self.save_hyperparameters()
        
        # Load CLIP model
        self.clip_model = CLIPModel.from_pretrained(clip_model_name)
        
        # Freeze text model if specified
        if freeze_text_encoder:
            for param in self.clip_model.text_model.parameters():
                param.requires_grad = False
            
        # Keep vision model trainable
        for param in self.clip_model.vision_model.parameters():
            param.requires_grad = True
        
        # Projection dimensions
        self.embed_dim = self.clip_model.projection_dim
        
        # Add classification head
        self.classifier = nn.Linear(self.embed_dim, num_classes)
        
        # Add triplet loss
        self.triplet_loss = TripletLoss(margin=0.3)
        
        # Temperature parameter for contrastive loss
        self.temperature = temperature
        
        # Loss weights
        self.triplet_weight = triplet_weight
        self.contrastive_weight = contrastive_weight
        self.classification_weight = classification_weight
        
        # Loss usage flags
        self.use_triplet_loss = use_triplet_loss
        self.use_text_contrastive_loss = use_text_contrastive_loss
        self.use_classification_loss = use_classification_loss
        
        # Learning rate
        self.learning_rate = learning_rate
        
    def forward(self, image, text_inputs=None):
        """
        Forward pass
        
        Args:
            image: Image tensor
            text_inputs: Optional text inputs
            
        Returns:
            Tuple of (image_features, text_features, class_logits)
        """
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
        if text_inputs is not None:
            text_outputs = self.clip_model.text_model(
                input_ids=text_inputs['input_ids'],
                attention_mask=text_inputs['attention_mask'],
                output_hidden_states=True,
                return_dict=True
            )
            text_embeds = text_outputs.pooler_output
            text_features = self.clip_model.text_projection(text_embeds)
            
            return image_features, text_features, logits
        
        return image_features, None, logits
    
    def training_step(self, batch, batch_idx):
        """
        Training step
        
        Args:
            batch: Batch of data
            batch_idx: Batch index
            
        Returns:
            Loss value
        """
        images = batch["image"]
        text_inputs = batch["text_inputs"]
        labels = batch["label"]
        
        # Forward pass
        image_features, text_features, logits = self.forward(images, text_inputs)
        
        total_loss = 0.0
        loss_components = {}
        
        # Classification loss
        if self.use_classification_loss:
            ce_loss = F.cross_entropy(logits, labels)
            loss_components["ce_loss"] = ce_loss
            total_loss += self.classification_weight * ce_loss
        
        # Normalize features for similarity computation
        if self.use_text_contrastive_loss or self.use_triplet_loss:
            image_features_normalized = image_features / image_features.norm(dim=-1, keepdim=True)
        
        # Contrastive loss between image and text
        if self.use_text_contrastive_loss:
            text_features_normalized = text_features / text_features.norm(dim=-1, keepdim=True)
            
            # Calculate similarity matrix
            logits_per_image = torch.matmul(
                image_features_normalized, 
                text_features_normalized.t()
            ) / self.temperature
            
            # Labels should be on the diagonal (i.e., each image matches with its text)
            contrastive_labels = torch.arange(logits_per_image.size(0), device=self.device)
            
            # Symmetric contrastive loss (image-to-text and text-to-image)
            i2t_loss = F.cross_entropy(logits_per_image, contrastive_labels)
            t2i_loss = F.cross_entropy(logits_per_image.t(), contrastive_labels)
            contrastive_loss = (i2t_loss + t2i_loss) / 2
            
            loss_components["contrastive_loss"] = contrastive_loss
            total_loss += self.contrastive_weight * contrastive_loss
        
        # Triplet loss for image embeddings
        if self.use_triplet_loss and batch_idx % 2 == 0:  # Only apply triplet every other batch to save compute
            # We need to form triplets: (anchor, positive, negative)
            # Anchor and positive should be the same product, negative a different product
            triplet_loss = 0.0
            num_triplets = 0
            
            # Process mini-batches of triplets to avoid creating too many combinations
            # and running out of memory
            for anchor_idx in range(min(8, len(images))):
                # Find positives (same label as anchor)
                label = labels[anchor_idx].item()
                pos_indices = [i for i in range(len(images)) if i != anchor_idx and labels[i].item() == label]
                
                if pos_indices:
                    # If we have positives, select one randomly
                    pos_idx = random.choice(pos_indices)
                    
                    # Get 1 hard negative
                    # Since we're in the middle of training, we can't use the dataset's get_hard_negatives
                    # So we'll just find a random negative
                    neg_indices = [i for i in range(len(images)) if labels[i].item() != label]
                    
                    if neg_indices:
                        neg_idx = random.choice(neg_indices)
                        
                        # Form triplet
                        anchor = image_features_normalized[anchor_idx]
                        positive = image_features_normalized[pos_idx]
                        negative = image_features_normalized[neg_idx]
                        
                        # Add to triplet loss
                        triplet_loss += F.relu(
                            (anchor - positive).pow(2).sum() - 
                            (anchor - negative).pow(2).sum() + 
                            0.3  # margin
                        )
                        num_triplets += 1
            
            if num_triplets > 0:
                triplet_loss = triplet_loss / num_triplets
                loss_components["triplet_loss"] = triplet_loss
                total_loss += self.triplet_weight * triplet_loss
        
        # Log all loss components
        for name, loss in loss_components.items():
            self.log(f"train_{name}", loss, prog_bar=True)
            
        self.log("train_loss", total_loss)
        
        return total_loss
    
    def validation_step(self, batch, batch_idx):
        """
        Validation step
        
        Args:
            batch: Batch of data
            batch_idx: Batch index
            
        Returns:
            Dictionary of validation metrics
        """
        images = batch["image"]
        text_inputs = batch["text_inputs"]
        labels = batch["label"]
        has_hebrew = batch.get("has_hebrew", torch.zeros(len(images), dtype=torch.bool))
        
        # Forward pass
        image_features, text_features, logits = self.forward(images, text_inputs)
        
        # Classification accuracy
        preds = torch.argmax(logits, dim=1)
        acc = (preds == labels).float().mean()
        
        # Separate accuracy for Hebrew vs non-Hebrew
        if has_hebrew.any():
            hebrew_acc = (preds[has_hebrew] == labels[has_hebrew]).float().mean()
            non_hebrew_acc = (preds[~has_hebrew] == labels[~has_hebrew]).float().mean()
            self.log("val_hebrew_acc", hebrew_acc)
            self.log("val_non_hebrew_acc", non_hebrew_acc)
        
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
            _, indices = similarity[i].topk(min(5, similarity.size(0)))
            if labels_contrastive[i] in indices:
                i2t_top5 += 1
        i2t_top5 /= similarity.size(0)
        
        # Text to image retrieval
        t2i_top1 = (torch.argmax(similarity, dim=0) == labels_contrastive).float().mean()
        t2i_top5 = 0
        for i in range(similarity.size(0)):
            _, indices = similarity[:, i].topk(min(5, similarity.size(0)))
            if labels_contrastive[i] in indices:
                t2i_top5 += 1
        t2i_top5 /= similarity.size(0)
        
        # Log all metrics
        self.log("val_acc", acc, prog_bar=True)
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
        """
        Configure optimizers
        
        Returns:
            Tuple of (optimizer, scheduler)
        """
        # Use different learning rates for different parts of the model
        params = [
            {"params": self.clip_model.vision_model.parameters(), "lr": self.learning_rate},
            {"params": self.clip_model.visual_projection.parameters(), "lr": self.learning_rate},
            {"params": self.classifier.parameters(), "lr": self.learning_rate * 10}
        ]
        
        # If text encoder is trainable, add it with a smaller learning rate
        if not self.hparams.freeze_text_encoder:
            params.append({
                "params": self.clip_model.text_model.parameters(), 
                "lr": self.learning_rate * 0.1
            })
            params.append({
                "params": self.clip_model.text_projection.parameters(), 
                "lr": self.learning_rate * 0.1
            })
        
        optimizer = torch.optim.AdamW(params, weight_decay=0.01)
        
        # Learning rate scheduler with warmup
        warmup_steps = 100
        total_steps = self.trainer.estimated_stepping_batches
        
        scheduler = torch.optim.lr_scheduler.OneCycleLR(
            optimizer, 
            max_lr=[group["lr"] for group in params],
            total_steps=total_steps,
            pct_start=warmup_steps / total_steps,
            anneal_strategy='cos'
        )
        
        return {
            "optimizer": optimizer,
            "lr_scheduler": {
                "scheduler": scheduler,
                "interval": "step"
            }
        }


class MobileProductModel(pl.LightningModule):
    """
    Mobile-optimized model for product recognition based on MobileViT
    """
    def __init__(
        self,
        num_classes,
        learning_rate=1e-4,
        teacher_model=None,
        embed_dim=768,
        model_name="apple/mobilevit-small",
        use_distillation=True,
        distillation_temp=4.0,
        distillation_weight=0.5
    ):
        super().__init__()
        self.save_hyperparameters(ignore=['teacher_model'])
        
        # Load base model (MobileViT)
        self.backbone = timm.create_model(model_name, pretrained=True)
        
        # Replace the classifier with an embedding projector and classifier
        num_features = self.backbone.head.in_features
        self.backbone.head = nn.Identity()  # Remove classification head
        
        # Add embedding projection
        self.projector = nn.Sequential(
            nn.Linear(num_features, embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim)
        )
        
        # Add classification head
        self.classifier = nn.Linear(embed_dim, num_classes)
        
        # Teacher model (optional, for knowledge distillation)
        self.teacher_model = teacher_model
        self.use_distillation = use_distillation and teacher_model is not None
        self.distillation_temp = distillation_temp
        self.distillation_weight = distillation_weight
        
        # Learning rate
        self.learning_rate = learning_rate
    
    def forward(self, x):
        """
        Forward pass
        
        Args:
            x: Input image tensor
            
        Returns:
            Tuple of (embeddings, class_logits)
        """
        # Extract features from backbone
        features = self.backbone(x)
        
        # Project to embedding space
        embeddings = self.projector(features)
        
        # Get classification logits
        logits = self.classifier(embeddings)
        
        return embeddings, logits
    
    def training_step(self, batch, batch_idx):
        """
        Training step
        
        Args:
            batch: Batch of data
            batch_idx: Batch index
            
        Returns:
            Loss value
        """
        images = batch["image"]
        labels = batch["label"]
        
        # Forward pass
        embeddings, logits = self.forward(images)
        
        # Classification loss
        ce_loss = F.cross_entropy(logits, labels)
        total_loss = ce_loss
        
        # Distillation loss if teacher model is available
        if self.use_distillation:
            # Get teacher predictions
            with torch.no_grad():
                # If the teacher is a CLIP model
                if hasattr(self.teacher_model, 'clip_model'):
                    teacher_img_features, _, teacher_logits = self.teacher_model(images)
                    teacher_embeddings = teacher_img_features
                else:
                    # If teacher is the same architecture as student
                    teacher_embeddings, teacher_logits = self.teacher_model(images)
                
                # Normalize teacher embeddings
                teacher_embeddings = teacher_embeddings / teacher_embeddings.norm(dim=-1, keepdim=True)
            
            # Normalize student embeddings
            student_embeddings = embeddings / embeddings.norm(dim=-1, keepdim=True)
            
            # Embedding distillation (cosine similarity loss)
            embedding_loss = 1 - torch.mean(
                torch.sum(student_embeddings * teacher_embeddings, dim=1)
            )
            
            # Logits distillation (KL divergence)
            T = self.distillation_temp
            distill_loss = F.kl_div(
                F.log_softmax(logits / T, dim=1),
                F.softmax(teacher_logits / T, dim=1),
                reduction='batchmean'
            ) * (T * T)
            
            # Combine losses
            distillation_loss = (embedding_loss + distill_loss) / 2
            total_loss = (1 - self.distillation_weight) * ce_loss + self.distillation_weight * distillation_loss
            
            # Log distillation losses
            self.log("train_embedding_loss", embedding_loss)
            self.log("train_distill_loss", distill_loss)
            self.log("train_distillation_loss", distillation_loss)
        
        # Calculate accuracy
        preds = torch.argmax(logits, dim=1)
        acc = (preds == labels).float().mean()
        
        # Log metrics
        self.log("train_loss", total_loss)
        self.log("train_ce_loss", ce_loss)
        self.log("train_acc", acc)
        
        return total_loss
    
    def validation_step(self, batch, batch_idx):
        """
        Validation step
        
        Args:
            batch: Batch of data
            batch_idx: Batch index
            
        Returns:
            Dictionary of validation metrics
        """
        images = batch["image"]
        labels = batch["label"]
        has_hebrew = batch.get("has_hebrew", torch.zeros(len(images), dtype=torch.bool))
        
        # Forward pass
        embeddings, logits = self.forward(images)
        
        # Calculate accuracy
        preds = torch.argmax(logits, dim=1)
        acc = (preds == labels).float().mean()
        
        # Separate accuracy for Hebrew vs non-Hebrew
        if has_hebrew.any():
            hebrew_acc = (preds[has_hebrew] == labels[has_hebrew]).float().mean()
            non_hebrew_acc = (preds[~has_hebrew] == labels[~has_hebrew]).float().mean()
            self.log("val_hebrew_acc", hebrew_acc)
            self.log("val_non_hebrew_acc", non_hebrew_acc)
        
        # Log overall accuracy
        self.log("val_acc", acc, prog_bar=True)
        
        # Also calculate top-5 accuracy
        top5_acc = 0
        for i in range(logits.size(0)):
            _, top5 = logits[i].topk(min(5, logits.size(1)))
            if labels[i] in top5:
                top5_acc += 1
        top5_acc /= logits.size(0)
        self.log("val_top5_acc", top5_acc, prog_bar=True)
        
        return {
            "val_acc": acc,
            "val_top5_acc": top5_acc
        }
    
    def configure_optimizers(self):
        """
        Configure optimizers
        
        Returns:
            Dictionary with optimizer and lr scheduler
        """
        # Different learning rates for different parts
        backbone_params = list(self.backbone.parameters())
        head_params = list(self.projector.parameters()) + list(self.classifier.parameters())
        
        optimizer = torch.optim.AdamW([
            {'params': backbone_params, 'lr': self.learning_rate * 0.1},
            {'params': head_params, 'lr': self.learning_rate}
        ], weight_decay=0.01)
        
        # OneCycle LR scheduler
        scheduler = torch.optim.lr_scheduler.OneCycleLR(
            optimizer,
            max_lr=[self.learning_rate * 0.1, self.learning_rate],
            total_steps=self.trainer.estimated_stepping_batches,
            pct_start=0.1,
            anneal_strategy='cos'
        )
        
        return {
            "optimizer": optimizer,
            "lr_scheduler": {
                "scheduler": scheduler,
                "interval": "step"
            }
        }


class ProductEmbeddingService:
    """
    Service for generating and querying product embeddings
    """
    def __init__(
        self, 
        model, 
        processor=None,
        img_size=224,
        device="cpu",
        use_faiss=True,
        index_type="L2"
    ):
        """
        Initialize the embedding service
        
        Args:
            model: Trained model (CLIP or Mobile)
            processor: Image processor (for CLIP)
            img_size: Image size for preprocessing
            device: Device to run inference on
            use_faiss: Whether to use FAISS for similarity search
            index_type: FAISS index type ('L2', 'IP', or 'cosine')
        """
        self.model = model.to(device)
        self.model.eval()
        self.processor = processor
        self.device = device
        self.img_size = img_size
        self.use_faiss = use_faiss
        self.index_type = index_type
        
        # Setup image transform
        self.transform = A.Compose([
            A.Resize(height=img_size, width=img_size),
            A.Normalize(mean=(0.48145466, 0.4578275, 0.40821073), 
                       std=(0.26862954, 0.26130258, 0.27577711)),
            ToTensorV2()
        ])
        
        # Placeholder for index and product data
        self.index = None
        self.product_embeddings = None
        self.product_data = None
        
    def load_product_database(self, product_db_path):
        """
        Load product database
        
        Args:
            product_db_path: Path to product database file (JSON or CSV)
        
        Returns:
            Product data dictionary
        """
        ext = os.path.splitext(product_db_path)[1].lower()
        
        if ext == '.csv':
            df = pd.read_csv(product_db_path)
            # Convert DataFrame to dictionary
            self.product_data = df.to_dict('records')
            # Create mapping from index to product ID
            self.id_mapping = {i: item['product_id'] for i, item in enumerate(self.product_data)}
        elif ext == '.json':
            with open(product_db_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Check if it's a dictionary with product IDs as keys
            if isinstance(data, dict):
                self.product_data = [{"product_id": k, **v} for k, v in data.items()]
            else:
                self.product_data = data
                
            # Create mapping from index to product ID
            self.id_mapping = {i: item['product_id'] for i, item in enumerate(self.product_data)}
        else:
            raise ValueError(f"Unsupported database format: {ext}")
            
        return self.product_data
    
    def build_index(self, reference_image_dir=None):
        """
        Build search index from product images or existing embeddings
        
        Args:
            reference_image_dir: Directory containing reference images
        
        Returns:
            FAISS index
        """
        if self.product_data is None:
            raise ValueError("Product database must be loaded first")
            
        # Generate embeddings
        embeddings = []
        
        # For each product
        for i, product in enumerate(tqdm(self.product_data, desc="Building product index")):
            product_id = product['product_id']
            
            if reference_image_dir:
                # Look for reference image
                img_path = os.path.join(reference_image_dir, f"{product_id}.jpg")
                if not os.path.exists(img_path):
                    # Try alternate extensions
                    for ext in ['.jpeg', '.png']:
                        alt_path = os.path.join(reference_image_dir, f"{product_id}{ext}")
                        if os.path.exists(alt_path):
                            img_path = alt_path
                            break
                
                if os.path.exists(img_path):
                    # Process image and get embedding
                    embedding = self._get_image_embedding(img_path)
                    embeddings.append(embedding)
                else:
                    # Create text embedding instead
                    embedding = self._get_text_embedding(product)
                    embeddings.append(embedding)
            else:
                # Create text embedding if no reference images
                embedding = self._get_text_embedding(product)
                embeddings.append(embedding)
        
        # Stack embeddings
        self.product_embeddings = np.vstack(embeddings).astype(np.float32)
        
        # Create FAISS index
        if self.use_faiss:
            d = self.product_embeddings.shape[1]  # Embedding dimension
            
            if self.index_type == "L2":
                # L2 distance index (Euclidean)
                self.index = faiss.IndexFlatL2(d)
                self.index.add(self.product_embeddings)
            elif self.index_type == "IP":
                # Inner product index (dot product)
                self.index = faiss.IndexFlatIP(d)
                self.index.add(self.product_embeddings)
            elif self.index_type == "cosine":
                # Cosine similarity index (normalized inner product)
                self.index = faiss.IndexFlatIP(d)
                # Normalize all vectors
                faiss.normalize_L2(self.product_embeddings)
                self.index.add(self.product_embeddings)
            else:
                raise ValueError(f"Unsupported index type: {self.index_type}")
        
        return self.index
    
    def _get_image_embedding(self, img_path):
        """
        Get embedding for an image
        
        Args:
            img_path: Path to image
            
        Returns:
            Numpy array of embedding
        """
        # Read image
        image = Image.open(img_path).convert('RGB')
        image_np = np.array(image)
        
        # Apply transform
        transformed = self.transform(image=image_np)
        image_tensor = transformed["image"].unsqueeze(0).to(self.device)
        
        # Generate embedding
        with torch.no_grad():
            if hasattr(self.model, 'clip_model'):
                # CLIP model
                image_features, _, _ = self.model(image_tensor)
                # Normalize
                image_features = image_features / image_features.norm(dim=-1, keepdim=True)
            else:
                # Mobile model
                image_features, _ = self.model(image_tensor)
                # Normalize
                image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        
        return image_features.cpu().numpy()[0]
    
    def _get_text_embedding(self, product):
        """
        Get embedding for product text
        
        Args:
            product: Product data dictionary
            
        Returns:
            Numpy array of embedding
        """
        # Only applicable for CLIP models
        if not hasattr(self.model, 'clip_model') or self.processor is None:
            # For non-CLIP models, return a random embedding
            # Not ideal, but a fallback
            embed_dim = self.product_embeddings[0].shape[0] if len(self.product_embeddings) > 0 else 768
            return np.random.randn(embed_dim).astype(np.float32)
        
        # Construct product text
        name = product.get('name', '')
        brand = product.get('brand', '')
        name_he = product.get('name_he', '')
        
        if name_he:
            text = f"{brand} {name} {name_he}"
        else:
            text = f"{brand} {name}"
        
        # Process text
        text_inputs = self.processor(
            text=text, 
            return_tensors="pt", 
            padding=True
        ).to(self.device)
        
        # Generate embedding
        with torch.no_grad():
            text_outputs = self.model.clip_model.text_model(
                input_ids=text_inputs.input_ids,
                attention_mask=text_inputs.attention_mask,
                output_hidden_states=True,
                return_dict=True
            )
            text_embeds = text_outputs.pooler_output
            text_features = self.model.clip_model.text_projection(text_embeds)
            
            # Normalize
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)
        
        return text_features.cpu().numpy()[0]
    
    def search_by_image(self, img_path, top_k=5):
        """
        Search for products similar to an input image
        
        Args:
            img_path: Path to query image
            top_k: Number of results to return
            
        Returns:
            List of product matches with similarity scores
        """
        if self.index is None:
            raise ValueError("Search index must be built first")
            
        # Get image embedding
        embedding = self._get_image_embedding(img_path)
        
        # Reshape for search
        query_embedding = embedding.reshape(1, -1).astype(np.float32)
        
        # Normalize if using cosine similarity
        if self.index_type == "cosine":
            faiss.normalize_L2(query_embedding)
        
        # Search
        if self.use_faiss:
            distances, indices = self.index.search(query_embedding, top_k)
            
            # Create results
            results = []
            for i, (distance, idx) in enumerate(zip(distances[0], indices[0])):
                # Get product
                product_id = self.id_mapping[idx]
                product = next((p for p in self.product_data if p['product_id'] == product_id), None)
                
                if product:
                    # Add similarity score
                    if self.index_type == "L2":
                        # Convert L2 distance to similarity score (1 / (1 + distance))
                        similarity = 1.0 / (1.0 + distance)
                    else:
                        # For IP and cosine, distance is already similarity
                        similarity = float(distance)
                    
                    # Add to results
                    results.append({
                        "rank": i + 1,
                        "product_id": product_id,
                        "similarity": similarity,
                        "product": product
                    })
            
            return results
        else:
            # Compute similarities manually
            similarities = []
            
            for i, product_embedding in enumerate(self.product_embeddings):
                if self.index_type == "L2":
                    # Compute L2 distance
                    dist = np.sum((embedding - product_embedding) ** 2)
                    # Convert to similarity
                    similarity = 1.0 / (1.0 + dist)
                else:
                    # Compute dot product
                    similarity = np.dot(embedding, product_embedding)
                
                similarities.append((similarity, i))
            
            # Sort by similarity (descending)
            similarities.sort(reverse=True)
            
            # Take top_k
            results = []
            for i, (similarity, idx) in enumerate(similarities[:top_k]):
                # Get product
                product_id = self.id_mapping[idx]
                product = next((p for p in self.product_data if p['product_id'] == product_id), None)
                
                if product:
                    results.append({
                        "rank": i + 1,
                        "product_id": product_id,
                        "similarity": float(similarity),
                        "product": product
                    })
            
            return results
    
    def classify_image(self, img_path):
        """
        Classify an image using the model's classifier head
        
        Args:
            img_path: Path to query image
            
        Returns:
            Dictionary with classification result
        """
        # Read image
        image = Image.open(img_path).convert('RGB')
        image_np = np.array(image)
        
        # Apply transform
        transformed = self.transform(image=image_np)
        image_tensor = transformed["image"].unsqueeze(0).to(self.device)
        
        # Predict
        with torch.no_grad():
            if hasattr(self.model, 'clip_model'):
                # CLIP model
                _, _, logits = self.model(image_tensor)
            else:
                # Mobile model
                _, logits = self.model(image_tensor)
            
            # Get probabilities
            probs = F.softmax(logits, dim=1)
            
            # Get prediction
            confidence, pred_idx = torch.max(probs, dim=1)
        
        # Get product ID
        class_idx = pred_idx.item()
        product_id = self.id_mapping[class_idx]
        
        # Get product info
        product = next((p for p in self.product_data if p['product_id'] == product_id), None)
        
        if product:
            return {
                "product_id": product_id,
                "confidence": float(confidence.item()),
                "product": product
            }
        else:
            return {
                "error": f"Product with ID {product_id} not found in database"
            }


def train_clip_model(data_dir, manifest_path, output_dir, args):
    """
    Train the CLIP-based product recognition model
    
    Args:
        data_dir: Directory containing product images
        manifest_path: Path to manifest file
        output_dir: Directory to save model checkpoints
        args: Training arguments
    
    Returns:
        Trained model
    """
    # Create datasets
    train_dataset = ProductDataset(
        data_dir=data_dir,
        manifest_path=manifest_path,
        split='train',
        img_size=args.img_size,
        use_augmentation=True,
        use_hard_negatives=args.use_hard_negatives,
        multilingual=args.multilingual,
        hard_neg_strategy=args.hard_neg_strategy
    )
    
    val_dataset = ProductDataset(
        data_dir=data_dir,
        manifest_path=manifest_path,
        split='val',
        img_size=args.img_size,
        use_augmentation=False,
        use_hard_negatives=False,
        multilingual=args.multilingual
    )
    
    # Create dataloaders
    train_loader = DataLoader(
        train_dataset,
        batch_size=args.batch_size,
        shuffle=True,
        num_workers=args.num_workers,
        pin_memory=True
    )
    
    val_loader = DataLoader(
        val_dataset,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=args.num_workers,
        pin_memory=True
    )
    
    # Get number of classes
    num_classes = len(train_dataset.product_ids)
    print(f"Training with {num_classes} classes")
    print(f"Training set: {len(train_dataset)} images")
    print(f"Validation set: {len(val_dataset)} images")
    
    # Create model
    model = CLIPProductModel(
        num_classes=num_classes,
        learning_rate=args.learning_rate,
        clip_model_name=args.clip_model,
        use_triplet_loss=args.use_triplet_loss,
        use_text_contrastive_loss=args.use_text_contrastive_loss,
        use_classification_loss=args.use_classification_loss,
        triplet_weight=args.triplet_weight,
        contrastive_weight=args.contrastive_weight,
        classification_weight=args.classification_weight,
        freeze_text_encoder=args.freeze_text_encoder
    )
    
    # Create logger
    logger = TensorBoardLogger(output_dir, name="clip_model")
    
    # Create callbacks
    checkpoint_callback = ModelCheckpoint(
        dirpath=os.path.join(output_dir, "clip_model"),
        filename="clip-product-{epoch:02d}-{val_acc:.4f}",
        monitor="val_acc",
        save_top_k=3,
        mode="max"
    )
    
    early_stop_callback = EarlyStopping(
        monitor="val_acc",
        patience=args.patience,
        mode="max"
    )
    
    # Create trainer
    trainer = pl.Trainer(
        max_epochs=args.epochs,
        accelerator="gpu" if torch.cuda.is_available() else "cpu",
        devices=args.devices,
        logger=logger,
        callbacks=[checkpoint_callback, early_stop_callback],
        precision="16-mixed" if torch.cuda.is_available() else 32,
        gradient_clip_val=1.0,
        log_every_n_steps=10
    )
    
    # Train model
    trainer.fit(model, train_loader, val_loader)
    
    # Save final model
    final_path = os.path.join(output_dir, "clip_model", "clip_product_final.pt")
    torch.save(model.state_dict(), final_path)
    
    print(f"Model saved to {final_path}")
    
    return model, checkpoint_callback.best_model_path


def train_mobile_model(data_dir, manifest_path, output_dir, teacher_model_path, args):
    """
    Train the mobile-optimized product recognition model
    
    Args:
        data_dir: Directory containing product images
        manifest_path: Path to manifest file
        output_dir: Directory to save model checkpoints
        teacher_model_path: Path to teacher model checkpoint
        args: Training arguments
    
    Returns:
        Trained model
    """
    # Create datasets
    train_dataset = ProductDataset(
        data_dir=data_dir,
        manifest_path=manifest_path,
        split='train',
        img_size=args.img_size,
        use_augmentation=True,
        use_hard_negatives=False,
        multilingual=args.multilingual
    )
    
    val_dataset = ProductDataset(
        data_dir=data_dir,
        manifest_path=manifest_path,
        split='val',
        img_size=args.img_size,
        use_augmentation=False,
        use_hard_negatives=False,
        multilingual=args.multilingual
    )
    
    # Create dataloaders
    train_loader = DataLoader(
        train_dataset,
        batch_size=args.batch_size * 2,  # Larger batch size for distillation
        shuffle=True,
        num_workers=args.num_workers,
        pin_memory=True
    )
    
    val_loader = DataLoader(
        val_dataset,
        batch_size=args.batch_size * 2,
        shuffle=False,
        num_workers=args.num_workers,
        pin_memory=True
    )
    
    # Get number of classes
    num_classes = len(train_dataset.product_ids)
    print(f"Training with {num_classes} classes")
    
    # Load teacher model if available
    teacher_model = None
    if teacher_model_path and os.path.exists(teacher_model_path):
        print(f"Loading teacher model from {teacher_model_path}")
        # Load the teacher model architecture
        teacher_model = CLIPProductModel.load_from_checkpoint(teacher_model_path)
        teacher_model.eval()
        teacher_model.freeze()
        
        # Make sure teacher knows we're in inference mode (no triplet loss)
        teacher_model.use_triplet_loss = False
    
    # Create mobile model
    model = MobileProductModel(
        num_classes=num_classes,
        learning_rate=args.learning_rate,
        teacher_model=teacher_model,
        embed_dim=args.embed_dim,
        model_name=args.mobile_model,
        use_distillation=args.use_distillation and teacher_model is not None,
        distillation_temp=args.distillation_temp,
        distillation_weight=args.distillation_weight
    )
    
    # Create logger
    logger = TensorBoardLogger(output_dir, name="mobile_model")
    
    # Create callbacks
    checkpoint_callback = ModelCheckpoint(
        dirpath=os.path.join(output_dir, "mobile_model"),
        filename="mobile-product-{epoch:02d}-{val_acc:.4f}",
        monitor="val_acc",
        save_top_k=3,
        mode="max"
    )
    
    early_stop_callback = EarlyStopping(
        monitor="val_acc",
        patience=args.patience,
        mode="max"
    )
    
    # Create trainer
    trainer = pl.Trainer(
        max_epochs=args.epochs,
        accelerator="gpu" if torch.cuda.is_available() else "cpu",
        devices=args.devices,
        logger=logger,
        callbacks=[checkpoint_callback, early_stop_callback],
        precision="16-mixed" if torch.cuda.is_available() else 32,
        gradient_clip_val=1.0,
        log_every_n_steps=10
    )
    
    # Train model
    trainer.fit(model, train_loader, val_loader)
    
    # Save final model
    final_path = os.path.join(output_dir, "mobile_model", "mobile_product_final.pt")
    torch.save(model.state_dict(), final_path)
    
    print(f"Model saved to {final_path}")
    
    return model, checkpoint_callback.best_model_path


def export_for_mobile(model_path, output_path, model_type="mobile", img_size=224, num_classes=None):
    """
    Export model for mobile deployment
    
    Args:
        model_path: Path to model checkpoint
        output_path: Path to save exported model
        model_type: 'clip' or 'mobile'
        img_size: Image size for model input
        num_classes: Number of classes (required for some formats)
    
    Returns:
        Path to exported model
    """
    import torch.onnx
    
    # Load model
    if model_type == "clip":
        model = CLIPProductModel.load_from_checkpoint(model_path)
    else:
        model = MobileProductModel.load_from_checkpoint(model_path)
    
    model.eval()
    
    # Create dummy input
    dummy_input = torch.randn(1, 3, img_size, img_size)
    
    # Export to ONNX
    onnx_path = output_path + ".onnx"
    
    # Export the model
    torch.onnx.export(
        model,
        dummy_input,  # model input
        onnx_path,  # output path
        export_params=True,  # store the trained parameter weights inside the model file
        opset_version=12,  # the ONNX version to export the model to
        do_constant_folding=True,  # whether to execute constant folding for optimization
        input_names=['input'],  # the model's input names
        output_names=['embeddings', 'logits'],  # the model's output names
        dynamic_axes={'input': {0: 'batch_size'},  # variable length axes
                    'embeddings': {0: 'batch_size'},
                    'logits': {0: 'batch_size'}}
    )
    
    print(f"Model exported to {onnx_path}")
    
    # For iOS deployment, we can also export to CoreML
    try:
        import coremltools as ct
        from coremltools.models.neural_network import quantization_utils
        
        # Export to CoreML
        coreml_path = output_path + ".mlmodel"
        
        # Convert PyTorch model to CoreML
        traced_model = torch.jit.trace(model, dummy_input)
        mlmodel = ct.convert(
            traced_model,
            inputs=[ct.ImageType(name="input", shape=dummy_input.shape)],
            convert_to="neuralnetwork"
        )
        
        # Set metadata
        mlmodel.author = "PriceComparisonApp"
        mlmodel.license = "Private"
        mlmodel.short_description = "Product recognition model"
        
        # Save model
        mlmodel.save(coreml_path)
        
        # Optionally create a quantized version for smaller size
        quantized_path = output_path + "_quantized.mlmodel"
        quantized_model = quantization_utils.quantize_weights(mlmodel, 8)
        quantized_model.save(quantized_path)
        
        print(f"CoreML model exported to {coreml_path}")
        print(f"Quantized CoreML model exported to {quantized_path}")
        
        return onnx_path, coreml_path, quantized_path
    except ImportError:
        print("CoreML export skipped - coremltools not installed")
        return onnx_path


def main():
    parser = argparse.ArgumentParser(description="Train product recognition models")
    
    # Data arguments
    parser.add_argument("--data-dir", type=str, required=True, help="Data directory")
    parser.add_argument("--manifest-path", type=str, required=True, help="Path to manifest file")
    parser.add_argument("--output-dir", type=str, default="models", help="Output directory")
    
    # Training mode
    parser.add_argument("--mode", type=str, choices=["clip", "mobile", "both"], default="both", 
                        help="Training mode")
    
    # CLIP model arguments
    parser.add_argument("--clip-model", type=str, default="openai/clip-vit-b-32", 
                        help="CLIP model name")
    parser.add_argument("--use-triplet-loss", action="store_true", help="Use triplet loss")
    parser.add_argument("--use-text-contrastive-loss", action="store_true", help="Use text contrastive loss")
    parser.add_argument("--use-classification-loss", action="store_true", help="Use classification loss")
    parser.add_argument("--triplet-weight", type=float, default=1.0, help="Weight for triplet loss")
    parser.add_argument("--contrastive-weight", type=float, default=1.0, help="Weight for contrastive loss")
    parser.add_argument("--classification-weight", type=float, default=1.0, help="Weight for classification loss")
    parser.add_argument("--freeze-text-encoder", action="store_true", help="Freeze text encoder")
    parser.add_argument("--use-hard-negatives", action="store_true", help="Use hard negative mining")
    parser.add_argument("--hard-neg-strategy", type=str, default="brand_category", 
                        choices=["random", "brand", "category", "brand_category"], 
                        help="Hard negative mining strategy")
    
    # Mobile model arguments
    parser.add_argument("--mobile-model", type=str, default="mobilevit_s", 
                        help="Mobile model type")
    parser.add_argument("--use-distillation", action="store_true", help="Use knowledge distillation")
    parser.add_argument("--distillation-temp", type=float, default=4.0, help="Temperature for distillation")
    parser.add_argument("--distillation-weight", type=float, default=0.5, help="Weight for distillation loss")
    parser.add_argument("--embed-dim", type=int, default=768, help="Embedding dimension for mobile model")
    
    # General training arguments
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size")
    parser.add_argument("--epochs", type=int, default=20, help="Number of epochs")
    parser.add_argument("--learning-rate", type=float, default=2e-5, help="Learning rate")
    parser.add_argument("--img-size", type=int, default=224, help="Image size")
    parser.add_argument("--num-workers", type=int, default=4, help="Number of workers for data loading")
    parser.add_argument("--devices", type=int, default=1, help="Number of devices to use")
    parser.add_argument("--patience", type=int, default=5, help="Patience for early stopping")
    parser.add_argument("--multilingual", action="store_true", help="Use multilingual training")
    
    # Export arguments
    parser.add_argument("--export", action="store_true", help="Export model for mobile")
    parser.add_argument("--export-format", type=str, default="onnx", choices=["onnx", "coreml", "both"],
                       help="Export format for mobile deployment")
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Set default loss flags if none specified
    if not any([args.use_triplet_loss, args.use_text_contrastive_loss, args.use_classification_loss]):
        args.use_triplet_loss = True
        args.use_text_contrastive_loss = True
        args.use_classification_loss = True
    
    # Train CLIP model
    clip_model_path = None
    if args.mode in ["clip", "both"]:
        print("=== Training CLIP model ===")
        _, clip_model_path = train_clip_model(
            args.data_dir, 
            args.manifest_path, 
            args.output_dir,
            args
        )
    
    # Train mobile model
    if args.mode in ["mobile", "both"]:
        print("=== Training mobile model ===")
        _, mobile_model_path = train_mobile_model(
            args.data_dir, 
            args.manifest_path, 
            args.output_dir,
            clip_model_path,  # Use CLIP model as teacher if available
            args
        )
        
        if args.export:
            print("=== Exporting mobile model ===")
            export_for_mobile(
                mobile_model_path,
                os.path.join(args.output_dir, "mobile_model_export"),
                model_type="mobile",
                img_size=args.img_size
            )

if __name__ == "__main__":
    main()