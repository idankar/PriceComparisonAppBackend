# spiders/nutella.py
import scrapy
from scrapy.http import Request

class NutellaSpider(scrapy.Spider):
    name = 'nutella'
    
    def start_requests(self):
        url = "https://www.shufersal.co.il/online/he/search?q=נוטלה"
        
        yield Request(
            url=url,
            callback=self.parse,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    {"method": "wait_for_selector", "selector": ".productBox", "timeout": 30000},
                ],
            }
        )
    
    async def parse(self, response):
        page = response.meta["playwright_page"]
        
        # Wait for products to load
        await page.wait_for_selector(".productBox", timeout=10000)
        
        # Extract using JavaScript in the page context
        products = await page.evaluate("""() => {
            const results = [];
            
            // Try to get all product elements
            const productElements = document.querySelectorAll('.productBox');
            console.log("Found " + productElements.length + " products");
            
            productElements.forEach(element => {
                try {
                    // Get name element
                    const nameElement = element.querySelector('.description, .text');
                    // Get price element
                    const priceElement = element.querySelector('.number, .price');
                    // Get image element
                    const imageElement = element.querySelector('img');
                    
                    let name = nameElement ? nameElement.textContent.trim() : "Unknown";
                    let price = priceElement ? priceElement.textContent.trim() : "0";
                    let imageUrl = imageElement ? imageElement.src : "";
                    
                    // Clean price
                    price = price.replace('₪', '').replace(',', '').trim();
                    
                    results.push({
                        name: name,
                        price: price,
                        image: imageUrl
                    });
                } catch(e) {
                    console.error("Error extracting product data:", e);
                }
            });
            
            return results;
        }""")
        
        # Log how many products were found
        self.logger.info(f"Found {len(products)} products")
        
        # Yield each product
        for product in products:
            yield product
        
        await page.close()