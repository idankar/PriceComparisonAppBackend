#!/usr/bin/env python3
"""
Enhanced Multi-modal Product Recognition Training with Data Augmentation
(Updated for Local macOS Testing Environment)

This version implements data augmentation to create multiple views of each product,
enabling contrastive learning even with single-sample products.
"""

import os
import json
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader, Sampler
from torchvision import transforms, models
from transformers import AutoTokenizer, AutoModel
from PIL import Image
import numpy as np
from pathlib import Path
from tqdm import tqdm
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime
import re
import cv2
from collections import defaultdict
import random
import torchvision.transforms.functional as TF

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TextPreprocessor:
    """Enhanced text preprocessing with Hebrew optimization and brand filtering"""

    def __init__(self):
        self.priority_hebrew_words = [
            'במבה', 'נסקפה', 'האגיס', 'שופרסל', 'אסם', 'תנובה', 'דנונה', 'יטבתה',
            'עלית', 'סנו', 'חלב', 'קפה', 'חיתולים', 'קרקר', 'תפוח', 'אדמה'
        ]
        self.priority_english_brands = [
            'NESCAFE', 'HUGGIES', 'PAMPERS', 'COCA', 'PEPSI', 'NESTLE', 'UNILEVER',
            'Tasters', 'Choice', 'Freedom', 'Dry'
        ]

    def filter_relevant_text(self, text: str, words: List[str]) -> Tuple[str, List[str]]:
        """Filter text to keep only brand/product relevant information"""
        if not text or not words:
            return "", []
        filtered_words = []
        for word in words:
            word = word.strip()
            if len(word) < 2: continue
            if (re.match(r'^[\d\s₪%=\|\-\+\*\/#]+$', word) or
                re.match(r'^\d+$', word) or
                word in ['/', '|', '-', '+', '*', '#', '=', '%']):
                continue
            marketing_words = ['של', 'עם', 'בטעם', 'ללא', 'חדש', 'משופר', 'איכות', 'טעים', 'נהדר',
                               'NEW', 'IMPROVED', 'QUALITY', 'FRESH', 'BEST', 'PREMIUM', 'נוחות', 'בכל', 'תנועה']
            if word in marketing_words: continue
            if (word in self.priority_hebrew_words or
                word in self.priority_english_brands or
                word.upper() in self.priority_english_brands):
                filtered_words.append(word)
                continue
            if re.match(r'^[א-ת]{2,}$', word):
                filtered_words.append(word)
                continue
            if re.match(r'^[A-Z][a-zA-Z]{1,}$', word):
                filtered_words.append(word)
                continue
            if re.match(r'^[A-Z]{2,}$', word):
                filtered_words.append(word)
                continue
            if re.match(r'^\d+[מלגק"ק]+$', word):
                filtered_words.append(word)
                continue
        seen = set()
        unique_filtered_words = [x for x in filtered_words if not (x in seen or seen.add(x))]
        return ' '.join(unique_filtered_words), unique_filtered_words


class AugmentedProductDataset(Dataset):
    """
    Enhanced dataset that creates multiple augmented views of each product.
    """
    def __init__(
        self,
        jsonl_path: str,
        base_image_directory: str,
        tokenizer,
        max_text_length: int = 128,
        transform=None,
        use_enhanced_text: bool = True,
        num_augmentations: int = 3,
        augmentation_prob: float = 0.8
    ):
        self.tokenizer = tokenizer
        self.max_text_length = max_text_length
        self.transform = transform
        self.use_enhanced_text = use_enhanced_text
        self.num_augmentations = num_augmentations
        self.augmentation_prob = augmentation_prob
        self.base_image_directory = base_image_directory
        self.text_preprocessor = TextPreprocessor()
        self.base_data = []
        self.product_to_idx = {}

        logger.info(f"Loading data from {jsonl_path}")
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                item = json.loads(line.strip())
                relative_image_path = item['local_image_path']
                full_image_path = os.path.join(self.base_image_directory, relative_image_path)
                if os.path.exists(full_image_path):
                    item['resolved_image_path'] = full_image_path
                    if self.use_enhanced_text and 'text_for_embedding' in item:
                        original_text = item['text_for_embedding']
                        filtered_text, _ = self.text_preprocessor.filter_relevant_text(original_text, original_text.split())
                        item['enhanced_text'] = filtered_text if filtered_text.strip() else original_text
                    else:
                        item['enhanced_text'] = item.get('text_for_embedding', '')
                    self.base_data.append(item)
        
        unique_products = sorted(list(set(item['training_item_id'] for item in self.base_data)))
        self.product_to_idx = {pid: idx for idx, pid in enumerate(unique_products)}
        self.num_products = len(unique_products)

        self.augmented_indices = [(b_idx, a_idx) for b_idx in range(len(self.base_data)) for a_idx in range(self.num_augmentations)]

        logger.info(f"Loaded {len(self.base_data)} base products with valid images.")
        logger.info(f"Created {len(self.augmented_indices)} augmented samples ({self.num_augmentations}x augmentation).")
        logger.info(f"Found {self.num_products} unique products.")

        self.augmentation_transforms = [
            transforms.Compose([
                transforms.RandomResizedCrop(224, scale=(0.9, 1.0)), transforms.RandomHorizontalFlip(p=0.3),
                transforms.ColorJitter(brightness=0.1, contrast=0.1, saturation=0.1, hue=0.05),
                transforms.ToTensor(), transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
            ]),
            transforms.Compose([
                transforms.RandomResizedCrop(224, scale=(0.7, 1.0)), transforms.RandomHorizontalFlip(p=0.5),
                transforms.RandomRotation(degrees=15), transforms.ColorJitter(brightness=0.2, contrast=0.2, saturation=0.2, hue=0.1),
                transforms.ToTensor(), transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
            ]),
            transforms.Compose([
                transforms.RandomResizedCrop(224, scale=(0.5, 1.0)), transforms.RandomHorizontalFlip(p=0.5),
                transforms.RandomRotation(degrees=30), transforms.RandomPerspective(distortion_scale=0.2, p=0.5),
                transforms.ColorJitter(brightness=0.3, contrast=0.3, saturation=0.3, hue=0.15), transforms.RandomGrayscale(p=0.1),
                transforms.ToTensor(), transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
            ])
        ]
        if self.num_augmentations > 0 and len(self.augmentation_transforms) > 0:
            while len(self.augmentation_transforms) < self.num_augmentations: self.augmentation_transforms.append(self.augmentation_transforms[-1])
        
        self.text_augmentations = [lambda x: x, lambda x: self._add_variations(x)]
        if self.num_augmentations > 0 and len(self.text_augmentations) > 0:
            while len(self.text_augmentations) < self.num_augmentations: self.text_augmentations.append(self.text_augmentations[-1])

    def _add_variations(self, text: str) -> str:
        if not text: return text
        words = text.split()
        if len(words) > 1 and random.random() < 0.3:
            idx = random.randint(0, len(words) - 1)
            words.insert(idx + 1, words[idx])
        return ' '.join(words)

    def __len__(self):
        return len(self.augmented_indices)

    def __getitem__(self, idx):
        base_idx, aug_idx = self.augmented_indices[idx]
        item = self.base_data[base_idx]
        image = Image.open(item['resolved_image_path']).convert('RGB')

        if aug_idx > 0 and random.random() < self.augmentation_prob:
            current_aug_transform_idx = min(aug_idx - 1, len(self.augmentation_transforms) - 1)
            image = self.augmentation_transforms[current_aug_transform_idx](image)
            current_text_aug_idx = min(aug_idx, len(self.text_augmentations) - 1)
            text = self.text_augmentations[current_text_aug_idx](item['enhanced_text'])
        else:
            if self.transform: image = self.transform(image)
            text = item['enhanced_text']

        encoded = self.tokenizer(text, max_length=self.max_text_length, padding='max_length', truncation=True, return_tensors='pt')
        product_idx = self.product_to_idx[item['training_item_id']]
        
        return {
            'image': image,
            'input_ids': encoded['input_ids'].squeeze(0),
            'attention_mask': encoded['attention_mask'].squeeze(0),
            'product_idx': product_idx,
            'augmentation_idx': aug_idx
        }


class AugmentationAwareBalancedSampler(Sampler):
    """ Sampler that ensures each batch has multiple views of the same products. """
    def __init__(self, dataset, batch_size, num_augmentations):
        self.dataset = dataset
        self.num_augmentations = num_augmentations
        self.num_base_products_in_subset = len(dataset.indices) // num_augmentations
        self.products_per_batch = max(1, batch_size // num_augmentations)
        self.batch_size = self.products_per_batch * num_augmentations
        
        logger.info(f"AugmentationAwareBalancedSampler: Base products in subset: {self.num_base_products_in_subset}, "
                    f"Products per batch: {self.products_per_batch}, Effective batch size: {self.batch_size}")
        
        # Build a map from the full dataset's index to the subset's index
        self.full_to_subset_idx_map = {full_idx: subset_idx for subset_idx, full_idx in enumerate(self.dataset.indices)}
        # Get the original indices of base products that are part of this subset
        self.subset_base_product_original_indices = sorted(list(set(i // self.num_augmentations for i in self.dataset.indices)))

    def __iter__(self):
        random.shuffle(self.subset_base_product_original_indices)
        for i in range(0, len(self.subset_base_product_original_indices), self.products_per_batch):
            batch_base_indices = self.subset_base_product_original_indices[i:i + self.products_per_batch]
            if len(batch_base_indices) < self.products_per_batch: continue # Drop last partial batch of base products
            
            batch_subset_indices = []
            for base_idx_original in batch_base_indices:
                for aug_idx in range(self.num_augmentations):
                    full_dataset_idx = base_idx_original * self.num_augmentations + aug_idx
                    if full_dataset_idx in self.full_to_subset_idx_map:
                        batch_subset_indices.append(self.full_to_subset_idx_map[full_dataset_idx])
            
            random.shuffle(batch_subset_indices)
            yield batch_subset_indices

    def __len__(self):
        return self.num_base_products_in_subset // self.products_per_batch


class VisionEncoder(nn.Module):
    """CLIP-style vision encoder with EfficientNet backbone"""
    def __init__(self, embedding_dim: int = 512, num_products: int = 1000):
        super().__init__()
        self.backbone = models.efficientnet_b3(weights=models.EfficientNet_B3_Weights.IMAGENET1K_V1)
        num_features = self.backbone.classifier[1].in_features
        self.backbone.classifier = nn.Identity()
        self.fc = nn.Sequential(
            nn.Linear(num_features, embedding_dim * 2), nn.ReLU(),
            nn.Dropout(0.2), nn.Linear(embedding_dim * 2, embedding_dim)
        )
        self.classifier = nn.Linear(embedding_dim, num_products)
        self.layer_norm = nn.LayerNorm(embedding_dim)
        nn.init.normal_(self.classifier.weight, mean=0.0, std=0.02)
        nn.init.constant_(self.classifier.bias, 0)

    def forward(self, x, return_logits=False):
        features = self.backbone(x)
        if not next(self.backbone.parameters()).requires_grad: features = features.detach()
        embeddings = self.layer_norm(self.fc(features))
        if return_logits: return embeddings, self.classifier(embeddings)
        return F.normalize(embeddings, p=2, dim=1)


class TextEncoder(nn.Module):
    """Enhanced XLM-RoBERTa encoder"""
    def __init__(self, model_name: str = 'xlm-roberta-base', embedding_dim: int = 512, num_products: int = 1000):
        super().__init__()
        self.transformer = AutoModel.from_pretrained(model_name)
        hidden_size = self.transformer.config.hidden_size
        self.output_projection = nn.Sequential(
            nn.Linear(hidden_size, embedding_dim * 2), nn.ReLU(),
            nn.Dropout(0.2), nn.Linear(embedding_dim * 2, embedding_dim)
        )
        self.classifier = nn.Linear(embedding_dim, num_products)
        self.layer_norm = nn.LayerNorm(embedding_dim)
        nn.init.normal_(self.classifier.weight, mean=0.0, std=0.02)
        nn.init.constant_(self.classifier.bias, 0)

    def forward(self, input_ids, attention_mask, return_logits=False):
        outputs = self.transformer(input_ids=input_ids, attention_mask=attention_mask)
        pooled_output = outputs.last_hidden_state[:, 0, :]
        if not next(self.transformer.parameters()).requires_grad: pooled_output = pooled_output.detach()
        embeddings = self.layer_norm(self.output_projection(pooled_output))
        if return_logits: return embeddings, self.classifier(embeddings)
        return F.normalize(embeddings, p=2, dim=1)


class CrossModalFusion(nn.Module):
    """Enhanced fusion module with attention"""
    def __init__(self, embedding_dim: int = 512, num_products: int = 1000):
        super().__init__()
        self.vision_attention = nn.Sequential(nn.Linear(embedding_dim, 1), nn.Sigmoid())
        self.text_attention = nn.Sequential(nn.Linear(embedding_dim, 1), nn.Sigmoid())
        self.fusion = nn.Sequential(
            nn.Linear(embedding_dim * 2, embedding_dim), nn.LayerNorm(embedding_dim), nn.ReLU(), nn.Dropout(0.2),
            nn.Linear(embedding_dim, embedding_dim // 2), nn.LayerNorm(embedding_dim // 2), nn.ReLU(), nn.Dropout(0.2),
            nn.Linear(embedding_dim // 2, num_products)
        )

    def forward(self, vision_emb, text_emb):
        vision_weight = self.vision_attention(vision_emb)
        text_weight = self.text_attention(text_emb)
        total_weight = vision_weight + text_weight + 1e-8
        weighted_vision = vision_emb * (vision_weight / total_weight)
        weighted_text = text_emb * (text_weight / total_weight)
        return self.fusion(torch.cat([weighted_vision, weighted_text], dim=1))


class ContrastiveLoss(nn.Module):
    """Contrastive loss for embedding learning"""
    def __init__(self, temperature: float = 0.07):
        super().__init__()
        self.temperature = temperature

    def forward(self, embeddings, labels):
        similarity_matrix = torch.matmul(embeddings, embeddings.T) / self.temperature
        mask = torch.eq(labels.view(-1, 1), labels.view(1, -1)).float()
        diagonal_mask = torch.eye(mask.size(0), device=mask.device).bool()
        mask.masked_fill_(diagonal_mask, 0)
        exp_sim = torch.exp(similarity_matrix)
        exp_sim.masked_fill_(diagonal_mask, 0)
        pos_sim = (exp_sim * mask).sum(dim=1)
        all_sim = exp_sim.sum(dim=1)
        loss = -torch.log(pos_sim / (all_sim + 1e-8) + 1e-8)
        valid_mask = mask.sum(dim=1) > 0
        return loss[valid_mask].mean() if valid_mask.any() else torch.tensor(0.0, device=embeddings.device)


def freeze_unfreeze_layers(model, requires_grad):
    for param in model.parameters(): param.requires_grad = requires_grad

class ProductRecognitionTrainer:
    """Trainer for multi-modal product recognition"""
    def __init__(self, config: dict):
        self.config = config
        # --- Device Setup Updated for Mac ---
        if torch.backends.mps.is_available() and torch.backends.mps.is_built():
            self.device = torch.device('mps')
        elif torch.cuda.is_available():
            self.device = torch.device('cuda')
        else:
            self.device = torch.device('cpu')
        logger.info(f"Using device: {self.device}")
        
        self.tokenizer = AutoTokenizer.from_pretrained(config['text_model'])
        self.setup_data()
        
        num_products = self.train_dataset.dataset.num_products
        self.vision_encoder = VisionEncoder(config['embedding_dim'], num_products).to(self.device)
        self.text_encoder = TextEncoder(config['text_model'], config['embedding_dim'], num_products).to(self.device)
        self.cross_modal_fusion = CrossModalFusion(config['embedding_dim'], num_products).to(self.device)
        
        self.vision_contrastive_loss = ContrastiveLoss(config['vision_temperature'])
        self.text_contrastive_loss = ContrastiveLoss(config['text_temperature'])
        self.cross_modal_loss = nn.CrossEntropyLoss()
        self.vision_classification_loss = nn.CrossEntropyLoss()
        self.text_classification_loss = nn.CrossEntropyLoss()
        
        self.setup_optimizers()
        self.best_accuracy = 0.0
        self.stage = 0

    def setup_data(self):
        train_transform = transforms.Compose([
            transforms.RandomResizedCrop(224, scale=(0.9, 1.0)), transforms.RandomHorizontalFlip(p=0.3),
            transforms.ColorJitter(brightness=0.1, contrast=0.1, saturation=0.1), transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])
        val_transform = transforms.Compose([
            transforms.Resize(256), transforms.CenterCrop(224), transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])
        
        full_dataset = AugmentedProductDataset(
            jsonl_path=self.config['data_path'], base_image_directory=self.config['base_image_directory'],
            tokenizer=self.tokenizer, max_text_length=self.config['max_text_length'], transform=train_transform,
            use_enhanced_text=self.config.get('use_enhanced_text', True), num_augmentations=self.config.get('num_augmentations', 3),
            augmentation_prob=self.config.get('augmentation_prob', 0.8)
        )
        
        num_base_products = len(full_dataset.base_data)
        train_size_base = int(0.9 * num_base_products)
        base_indices = list(range(num_base_products)); random.shuffle(base_indices)
        train_base_indices = base_indices[:train_size_base]
        val_base_indices = base_indices[train_size_base:]
        
        num_augs = self.config.get('num_augmentations', 3)
        train_subset_indices = [idx * num_augs + aug for idx in train_base_indices for aug in range(num_augs)]
        val_subset_indices = [idx * num_augs for idx in val_base_indices]
        
        self.train_dataset = torch.utils.data.Subset(full_dataset, train_subset_indices)
        self.val_dataset = torch.utils.data.Subset(full_dataset, val_subset_indices)
        
        # Set the transform on the shared dataset instance to the val_transform for the val_loader
        self.val_dataset.dataset.transform = val_transform
        
        train_sampler = AugmentationAwareBalancedSampler(self.train_dataset, self.config['batch_size'], num_augs)
        self.train_loader = DataLoader(self.train_dataset, batch_sampler=train_sampler, num_workers=self.config.get('num_workers', 0), pin_memory=True)
        self.val_loader = DataLoader(self.val_dataset, batch_size=self.config['batch_size'], shuffle=False, num_workers=self.config.get('num_workers', 0), pin_memory=True)
        
        logger.info(f"Train subset: {len(self.train_dataset)} samples. Val subset: {len(self.val_dataset)} samples.")

    def setup_optimizers(self):
        stage1_params = [
            {'params': self.vision_encoder.fc.parameters()}, {'params': self.vision_encoder.classifier.parameters()},
            {'params': self.vision_encoder.layer_norm.parameters()}, {'params': self.text_encoder.output_projection.parameters()},
            {'params': self.text_encoder.classifier.parameters()}, {'params': self.text_encoder.layer_norm.parameters()}
        ]
        self.stage1_optimizer = optim.AdamW(stage1_params, lr=self.config['stage1_lr'], weight_decay=0.01)
        
        stage2_params = [
            {'params': self.vision_encoder.parameters(), 'lr': self.config['stage2_vision_lr']},
            {'params': self.text_encoder.parameters(), 'lr': self.config['stage2_text_lr']},
            {'params': self.cross_modal_fusion.parameters(), 'lr': self.config['stage2_fusion_lr']}
        ]
        self.stage2_optimizer = optim.AdamW(stage2_params, weight_decay=0.01)
        
        stage2_epochs = self.config['num_epochs'] - self.config['stage1_epochs']
        self.stage1_scheduler = optim.lr_scheduler.CosineAnnealingLR(self.stage1_optimizer, T_max=self.config['stage1_epochs'])
        self.stage2_scheduler = optim.lr_scheduler.CosineAnnealingLR(self.stage2_optimizer, T_max=max(1, stage2_epochs))

    def train_epoch(self, epoch: int):
        self.vision_encoder.train(); self.text_encoder.train(); self.cross_modal_fusion.train()
        
        new_stage = 1 if epoch < self.config['stage1_epochs'] else 2
        if self.stage != new_stage:
            self.stage = new_stage
            if self.stage == 1:
                logger.info("Setting up for Stage 1: Freezing backbones.")
                freeze_unfreeze_layers(self.vision_encoder.backbone, False)
                freeze_unfreeze_layers(self.text_encoder.transformer, False)
            else: # Stage 2
                logger.info("Setting up for Stage 2: Unfreezing all layers.")
                freeze_unfreeze_layers(self.vision_encoder, True)
                freeze_unfreeze_layers(self.text_encoder, True)
        
        optimizer = self.stage1_optimizer if self.stage == 1 else self.stage2_optimizer
        progress_bar = tqdm(self.train_loader, desc=f'Epoch {epoch+1}/{self.config["num_epochs"]} [Stage {self.stage}]')
        
        total_loss = 0
        for batch in progress_bar:
            images = batch['image'].to(self.device); input_ids = batch['input_ids'].to(self.device)
            attention_mask = batch['attention_mask'].to(self.device); product_idx = batch['product_idx'].to(self.device)
            
            optimizer.zero_grad()
            if self.stage == 1:
                _, vision_logits = self.vision_encoder(images, return_logits=True)
                _, text_logits = self.text_encoder(input_ids, attention_mask, return_logits=True)
                loss = self.vision_classification_loss(vision_logits, product_idx) + self.text_classification_loss(text_logits, product_idx)
            else:
                vision_embeddings = self.vision_encoder(images)
                text_embeddings = self.text_encoder(input_ids, attention_mask)
                vision_loss = self.vision_contrastive_loss(vision_embeddings, product_idx)
                text_loss = self.text_contrastive_loss(text_embeddings, product_idx)
                fusion_output = self.cross_modal_fusion(vision_embeddings.detach(), text_embeddings.detach())
                fusion_loss = self.cross_modal_loss(fusion_output, product_idx)
                loss = (self.config['vision_loss_weight'] * vision_loss + self.config['text_loss_weight'] * text_loss + self.config['fusion_loss_weight'] * fusion_loss)
            
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            progress_bar.set_postfix({'loss': f'{loss.item():.4f}'})
            
        (self.stage1_scheduler if self.stage == 1 else self.stage2_scheduler).step()
        return total_loss / len(self.train_loader) if self.train_loader else 0.0

    def evaluate(self):
        self.vision_encoder.eval(); self.text_encoder.eval(); self.cross_modal_fusion.eval()
        all_vision_embeddings, all_text_embeddings, all_labels, all_fusion_preds = [], [], [], []
        k_val = self.config.get('retrieval_k_eval', 5)
        
        with torch.no_grad():
            for batch in tqdm(self.val_loader, desc='Evaluating'):
                images = batch['image'].to(self.device); input_ids = batch['input_ids'].to(self.device)
                attention_mask = batch['attention_mask'].to(self.device); product_idx = batch['product_idx'].to(self.device)
                
                vision_embeddings = self.vision_encoder(images)
                text_embeddings = self.text_encoder(input_ids, attention_mask)
                fusion_output = self.cross_modal_fusion(vision_embeddings, text_embeddings)
                
                all_vision_embeddings.append(vision_embeddings.cpu()); all_text_embeddings.append(text_embeddings.cpu())
                all_labels.append(product_idx.cpu()); all_fusion_preds.append(torch.argmax(fusion_output, dim=1).cpu())
        
        all_vision = torch.cat(all_vision_embeddings); all_text = torch.cat(all_text_embeddings)
        all_labels = torch.cat(all_labels); all_fusion_preds = torch.cat(all_fusion_preds)
        
        vision_acc = self.compute_retrieval_accuracy(all_vision, all_labels, k=k_val)
        text_acc = self.compute_retrieval_accuracy(all_text, all_labels, k=k_val)
        cross_modal_acc = (all_fusion_preds == all_labels).float().mean().item()
        
        return {f'vision_retrieval_acc_at_{k_val}': vision_acc, f'text_retrieval_acc_at_{k_val}': text_acc, 'cross_modal_acc': cross_modal_acc}

    def compute_retrieval_accuracy(self, embeddings, labels, k=5):
        sim_matrix = torch.matmul(embeddings, embeddings.T)
        sim_matrix.fill_diagonal_(-float('inf'))
        actual_k = min(k, len(labels) - 1)
        if actual_k <= 0: return 0.0
        _, top_k_indices = torch.topk(sim_matrix, k=actual_k, dim=1)
        correct = sum(1 for i in range(len(labels)) if (labels[top_k_indices[i]] == labels[i]).any())
        return correct / len(labels) if len(labels) > 0 else 0.0

    def save_checkpoint(self, epoch, metrics, is_best):
        save_dir = Path(self.config['save_dir']); save_dir.mkdir(parents=True, exist_ok=True)
        checkpoint = {'epoch': epoch + 1, 'vision_encoder': self.vision_encoder.state_dict(),
                      'text_encoder': self.text_encoder.state_dict(), 'cross_modal_fusion': self.cross_modal_fusion.state_dict(),
                      'optimizers': {'stage1': self.stage1_optimizer.state_dict(), 'stage2': self.stage2_optimizer.state_dict()},
                      'metrics': metrics, 'config': self.config}
        torch.save(checkpoint, save_dir / 'last_model.pt')
        if is_best: torch.save(checkpoint, save_dir / 'best_model.pt'); logger.info(f"💾 New best model saved at epoch {epoch+1}")

    def train(self):
        logger.info("🚀 Starting training...")
        k_val = self.config.get('retrieval_k_eval', 5)
        for epoch in range(self.config['num_epochs']):
            train_loss = self.train_epoch(epoch)
            metrics = self.evaluate()
            
            logger.info(f"Epoch {epoch+1}/{self.config['num_epochs']} | Train Loss: {train_loss:.4f} | "
                        f"Vision Acc@{k_val}: {metrics[f'vision_retrieval_acc_at_{k_val}']:.4f} | "
                        f"Text Acc@{k_val}: {metrics[f'text_retrieval_acc_at_{k_val}']:.4f} | "
                        f"Fusion Acc: {metrics['cross_modal_acc']:.4f}")
            
            current_score = metrics['cross_modal_acc'] if self.stage == 2 else (metrics[f'vision_retrieval_acc_at_{k_val}'] + metrics[f'text_retrieval_acc_at_{k_val}']) / 2
            is_best = current_score > self.best_accuracy
            if is_best: self.best_accuracy = current_score
            self.save_checkpoint(epoch, metrics, is_best)
        logger.info("🏁 Training complete.")


def main():
    config = {
        # --- Data Paths (Updated for Local Mac Environment) ---
        'data_path': '/Users/noa/Desktop/PriceComparisonApp/training_data.jsonl',
        'base_image_directory': '/Users/noa/Desktop/PriceComparisonApp/',
        'save_dir': '/Users/noa/Desktop/PriceComparisonApp/models/product_recognition_test',
        
        # --- Model Config ---
        'text_model': 'xlm-roberta-base',
        'embedding_dim': 512,
        'max_text_length': 128,
        
        # --- Text Preprocessing ---
        'use_enhanced_text': True,
        
        # --- Data Augmentation Settings ---
        'num_augmentations': 3,
        'augmentation_prob': 0.8,
        
        # --- Training Parameters (Adjusted for Local Testing) ---
        'num_epochs': 10,
        'stage1_epochs': 3,
        'batch_size': 12,  # Reduced for local Mac memory. Must be divisible by num_augmentations.
        
        # --- Learning Rates ---
        'stage1_lr': 5e-4,
        'stage2_vision_lr': 1e-5, # Reduced for stability during full fine-tuning
        'stage2_text_lr': 1e-6,   # Reduced for stability
        'stage2_fusion_lr': 5e-4,
        
        # --- Loss Weights for Stage 2 ---
        'vision_loss_weight': 1.0,
        'text_loss_weight': 1.0,
        'fusion_loss_weight': 2.0,
        
        # --- Temperature for Contrastive Loss ---
        'vision_temperature': 0.07,
        'text_temperature': 0.07,
        
        # --- Evaluation ---
        'retrieval_k_eval': 5,
        
        # --- System ---
        'num_workers': 0, # Set to 0 for local testing on Mac to avoid multiprocessing issues
    }

    trainer = ProductRecognitionTrainer(config)
    trainer.train()

if __name__ == "__main__":
    main()