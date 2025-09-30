# items.py
import scrapy

class ProductItem(scrapy.Item):
    name = scrapy.Field()
    price = scrapy.Field()
    image = scrapy.Field()
    
# pipelines.py
class PriceCleaningPipeline:
    def process_item(self, item, spider):
        # Clean price text
        if 'price' in item:
            price_text = item['price']
            if price_text:
                # Remove currency symbols and convert to float
                clean_price = price_text.replace('₪', '').replace(',', '').strip()
                try:
                    item['price'] = float(clean_price)
                except ValueError:
                    item['price'] = None
        return item