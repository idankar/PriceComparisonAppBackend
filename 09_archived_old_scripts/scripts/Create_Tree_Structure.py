import os
import sys

def create_directory_structure():
    # Create main directory
    base_dir = "product_image_recognition"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    
    # Create subdirectories
    directories = [
        os.path.join(base_dir, "static", "css"),
        os.path.join(base_dir, "static", "js"),
        os.path.join(base_dir, "templates")
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
    
    # Create empty files
    files = [
        os.path.join(base_dir, "app.py"),
        os.path.join(base_dir, "static", "css", "style.css"),
        os.path.join(base_dir, "static", "js", "main.js"),
        os.path.join(base_dir, "templates", "index.html"),
        os.path.join(base_dir, "requirements.txt")
    ]
    
    for file_path in files:
        with open(file_path, 'w') as f:
            pass  # Create empty file
    
    # Print success message
    print("Directory structure created successfully!")
    print("Structure:")
    for root, dirs, files in os.walk(base_dir):
        level = root.replace(base_dir, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f"{indent}{os.path.basename(root)}/")
        sub_indent = ' ' * 4 * (level + 1)
        for file in files:
            print(f"{sub_indent}{file}")
    
    print("\nDone! You can now copy-paste the content into each file.")

if __name__ == "__main__":
    create_directory_structure()