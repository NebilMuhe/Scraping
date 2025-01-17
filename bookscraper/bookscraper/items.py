# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class BookItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
   url = Field()
   title = Field()
   upc = Field()
   product_type = Field()
   price_excl_tax = Field()
   price_incl_tax = Field()
   tax = Field()
   availability = Field()
   num_reviews = Field()
   stars = Field()
   category = Field()
   description = Field()
   price = Field()
