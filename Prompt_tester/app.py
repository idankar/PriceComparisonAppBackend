from flask import Flask, render_template, request, jsonify
import os
import base64
import mimetypes
import traceback
import json
import random
import glob
import litellm

app = Flask(__name__)

# --- Configuration ---
api_key = "AIzaSyAupE8O_TPdDoTXhdHUxhGslVcXOY5ghgk"
os.environ['GEMINI_API_KEY'] = api_key

if 'GEMINI_API_KEY' not in os.environ:
    raise ValueError("GEMINI_API_KEY environment variable not set.")

target_model = "gemini/gemini-2.5-flash-preview-04-17"
batch_size = 10  # Process 10 random images

# --- Directory for product images ---
image_dir = "/Users/noa/Desktop/PriceComparisonApp/data/raw_images/"

# --- Default prompt ---
DEFAULT_PROMPT = """
Please perform the following steps based on the provided image:
1. Identify the main text that serves as the product's name or description.
2. Identify the brand name, typically found in a logo or other prominent text.
3. Look for any indication of quantity or weight.
4. Summarize the information you found in a JSON dictionary with the keys "name", "brand", and "amount".

IMPORTANT: Return each value in the language in which it appears on the image, including Hebrew text if present. Do not transliterate or translate the text. For Hebrew or other right-to-left scripts, preserve the exact characters as they appear. If you cannot clearly detect information for a given key ("name", "brand", or "amount") directly from the image, set its value to null. Do not add any information that does not appear in the image.

Make sure your response is valid JSON that can be parsed by Python's json.loads() function. The JSON should maintain Unicode characters intact.

Also, make sure to correctly identify and distinguish between the product name and brand name for Hebrew products.
"""

# --- Image Processing Functions ---
def encode_image_to_base64(filepath):
    """Reads an image file, encodes it to base64, and determines MIME type."""
    try:
        # Guess the MIME type based on the file extension
        mime_type, _ = mimetypes.guess_type(filepath)
        if not mime_type or not mime_type.startswith('image'):
            print(f"Warning: Could not reliably determine MIME type for {filepath}. Assuming image/jpeg.")
            mime_type = "image/jpeg"

        with open(filepath, "rb") as image_file:
            binary_data = image_file.read()
            base64_encoded_data = base64.b64encode(binary_data)
            base64_string = base64_encoded_data.decode('utf-8')
            return f"data:{mime_type};base64,{base64_string}", mime_type, base64_string
    except FileNotFoundError:
        print(f"Error: Image file not found at {filepath}")
        return None, None, None
    except Exception as e:
        print(f"Error processing image file {filepath}: {e}")
        traceback.print_exc()
        return None, None, None

def process_image(image_path, prompt_text):
    """Process a single image with Gemini and return results."""
    result = {
        "image_path": image_path,
        "image_filename": os.path.basename(image_path),
        "image_data_uri": None,
        "image_base64": None,
        "response_content": None,
        "parsed_json": None,
        "error": None
    }
    
    # Encode the image
    image_data_uri, detected_mime_type, base64_data = encode_image_to_base64(image_path)
    result["image_data_uri"] = image_data_uri
    result["image_base64"] = base64_data
    
    if not image_data_uri:
        result["error"] = "Failed to encode image"
        return result
    
    # Construct Multimodal Message
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt_text},
                {"type": "image_url", "image_url": {"url": image_data_uri}}
            ]
        }
    ]
    
    # Model Invocation
    try:
        print(f"Processing image: {os.path.basename(image_path)}")
        response = litellm.completion(
            model=target_model,
            messages=messages
        )
        
        # Extract content
        if response.choices and response.choices[0].message and response.choices[0].message.content:
            result["response_content"] = response.choices[0].message.content
            
            # Try to parse JSON
            try:
                # Clean up JSON string if needed
                cleaned_json_string = result["response_content"].strip()
                if cleaned_json_string.startswith("```json"):
                    cleaned_json_string = cleaned_json_string[7:].strip()
                    if cleaned_json_string.endswith("```"):
                        cleaned_json_string = cleaned_json_string[:-3].strip()
                elif cleaned_json_string.startswith("```"):
                    cleaned_json_string = cleaned_json_string[3:].strip()
                    if cleaned_json_string.endswith("```"):
                        cleaned_json_string = cleaned_json_string[:-3].strip()
                
                result["parsed_json"] = json.loads(cleaned_json_string)
                print(f"Successfully parsed JSON for {os.path.basename(image_path)}")
            except json.JSONDecodeError as json_err:
                result["error"] = f"JSON parse error: {json_err}"
                print(f"JSON parse error for {os.path.basename(image_path)}: {json_err}")
                print(f"Raw content: {result['response_content'][:100]}...")
            except Exception as parse_err:
                result["error"] = f"Unexpected parsing error: {parse_err}"
        else:
            result["error"] = "No valid content in response"
            
    except Exception as e:
        result["error"] = f"API call error: {str(e)}"
    
    return result

# --- Routes ---
@app.route('/')
def index():
    return render_template('index.html', default_prompt=DEFAULT_PROMPT)

@app.route('/process', methods=['POST'])
def process_images():
    # Get the prompt from the request
    data = request.get_json()
    prompt_text = data.get('prompt', DEFAULT_PROMPT)
    
    # Get all image files from directory
    image_files = glob.glob(os.path.join(image_dir, "*.jpg")) + \
                  glob.glob(os.path.join(image_dir, "*.jpeg")) + \
                  glob.glob(os.path.join(image_dir, "*.png"))
    
    if not image_files:
        return jsonify({"error": f"No image files found in {image_dir}"}), 404
    
    # Select random batch
    if len(image_files) > batch_size:
        selected_images = random.sample(image_files, batch_size)
    else:
        selected_images = image_files
    
    results = []
    
    # Process each image
    for image_path in selected_images:
        result = process_image(image_path, prompt_text)
        # Convert result to a format suitable for JSON
        processed_result = {
            "filename": result["image_filename"],
            "image_base64": result["image_base64"],
            "error": result["error"]
        }
        
        if result["parsed_json"]:
            processed_result["json"] = result["parsed_json"]
        elif result["response_content"]:
            processed_result["rawResponse"] = result["response_content"]
        
        results.append(processed_result)
    
    return jsonify({"results": results})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)