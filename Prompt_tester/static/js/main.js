document.addEventListener('DOMContentLoaded', function() {
    // DOM Elements
    const processButton = document.getElementById('processButton');
    const processButtonText = document.getElementById('processButtonText');
    const loadingSpinner = document.getElementById('loadingSpinner');
    const promptText = document.getElementById('promptText');
    const resultsContainer = document.getElementById('resultsContainer');
    const prevButton = document.getElementById('prevButton');
    const nextButton = document.getElementById('nextButton');
    const currentImage = document.getElementById('currentImage');
    const filenameDisplay = document.getElementById('filenameDisplay');
    const jsonDisplay = document.getElementById('jsonDisplay');
    const imageCounter = document.getElementById('imageCounter');
    const errorDisplay = document.getElementById('errorDisplay');
    const copyButton = document.getElementById('copyButton');
    const statusText = document.getElementById('statusText');

    // State variables
    let imageResults = [];
    let currentImageIndex = 0;

    // Event listeners
    processButton.addEventListener('click', processImages);
    prevButton.addEventListener('click', showPreviousImage);
    nextButton.addEventListener('click', showNextImage);
    copyButton.addEventListener('click', copyJsonToClipboard);

    // Functions
    function processImages() {
        // Show loading state
        processButton.disabled = true;
        processButtonText.textContent = 'Processing...';
        loadingSpinner.classList.remove('d-none');
        
        // Hide results if previously shown
        resultsContainer.classList.add('d-none');
        
        // Get the prompt text
        const prompt = promptText.value;
        
        // Send request to server
        fetch('/process', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ prompt: prompt })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            // Reset loading state
            processButton.disabled = false;
            processButtonText.textContent = 'Process Images';
            loadingSpinner.classList.add('d-none');
            
            // Store results
            imageResults = data.results;
            
            if (imageResults && imageResults.length > 0) {
                // Show results container
                resultsContainer.classList.remove('d-none');
                
                // Update status
                statusText.textContent = `Processed ${imageResults.length} images`;
                
                // Reset to first image
                currentImageIndex = 0;
                showCurrentImage();
                
                // Enable/disable navigation buttons
                updateNavigationButtons();
            } else {
                alert('No images were processed successfully.');
            }
        })
        .catch(error => {
            console.error('Error:', error);
            processButton.disabled = false;
            processButtonText.textContent = 'Process Images';
            loadingSpinner.classList.add('d-none');
            alert('An error occurred: ' + error.message);
        });
    }
    
    function showCurrentImage() {
        if (!imageResults || imageResults.length === 0) return;
        
        const result = imageResults[currentImageIndex];
        
        // Update counter
        imageCounter.textContent = `Image ${currentImageIndex + 1} of ${imageResults.length}`;
        
        // Update image
        currentImage.src = `data:image/jpeg;base64,${result.image_base64}`;
        filenameDisplay.textContent = result.filename;
        
        // Update JSON display
        if (result.json) {
            const formattedJson = formatJsonWithSyntaxHighlighting(result.json);
            jsonDisplay.innerHTML = formattedJson;
            errorDisplay.classList.add('d-none');
        } else if (result.rawResponse) {
            jsonDisplay.innerHTML = `<span class="text-warning">Failed to parse JSON:</span>\n${escapeHtml(result.rawResponse)}`;
            errorDisplay.classList.add('d-none');
        } else {
            jsonDisplay.textContent = '';
            errorDisplay.textContent = result.error || 'Unknown error';
            errorDisplay.classList.remove('d-none');
        }
    }
    
    function showPreviousImage() {
        if (currentImageIndex > 0) {
            currentImageIndex--;
            showCurrentImage();
            updateNavigationButtons();
        }
    }
    
    function showNextImage() {
        if (currentImageIndex < imageResults.length - 1) {
            currentImageIndex++;
            showCurrentImage();
            updateNavigationButtons();
        }
    }
    
    function updateNavigationButtons() {
        prevButton.disabled = currentImageIndex === 0;
        nextButton.disabled = currentImageIndex === imageResults.length - 1;
    }
    
    function copyJsonToClipboard() {
        const result = imageResults[currentImageIndex];
        if (result.json) {
            const jsonText = JSON.stringify(result.json, null, 2);
            navigator.clipboard.writeText(jsonText).then(() => {
                const originalText = copyButton.textContent;
                copyButton.textContent = 'Copied!';
                setTimeout(() => {
                    copyButton.textContent = originalText;
                }, 2000);
            });
        }
    }
    
    function formatJsonWithSyntaxHighlighting(json) {
        const jsonStr = JSON.stringify(json, null, 2);
        
        // Helper function to check if a string contains Hebrew
        function hasHebrew(str) {
            return /[\u0590-\u05FF\uFB1D-\uFB4F]/.test(str);
        }
        
        // Format with syntax highlighting and RTL support
        return jsonStr.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
            let cls = 'json-number';
            let formatted = match;
            
            if (/^"/.test(match)) {
                if (/:$/.test(match)) {
                    cls = 'json-key';
                    formatted = match.replace(/"/g, '').replace(/:$/, '');
                    return `"<span class="${cls}">${formatted}</span>":`;
                } else {
                    cls = 'json-string';
                    formatted = match.substring(1, match.length - 1);
                    
                    // Check for Hebrew text and apply RTL if found
                    if (hasHebrew(formatted)) {
                        return `"<span class="${cls} rtl-text">${formatted}</span>"`;
                    } else {
                        return `"<span class="${cls}">${formatted}</span>"`;
                    }
                }
            } else if (/true|false/.test(match)) {
                cls = 'json-boolean';
            } else if (/null/.test(match)) {
                cls = 'json-null';
            }
            
            return `<span class="${cls}">${match}</span>`;
        });
    }
    
    function escapeHtml(unsafe) {
        return unsafe
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }
});