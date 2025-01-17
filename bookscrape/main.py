import httpx
import os
import random
import time
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()

class XScraper:
    headers = {
        # human looking header copy it from your browser
    }
    def __init__(self, url):
        self.url = url
        self.client = httpx.Client(base_url=self.url, headers=self.headers, timeout=30)

        # initialize the session
        self.response = self.client.get('/')

    def process(self, data):
        # process the data here
        print("data")
        print(data)

    def run(self):
        # do the scraping here
        main_soup = BeautifulSoup(self.response.text, 'html.parser')
        product_list = []
        for value in main_soup.find_all('article',{'class':'product_pod'}):
            url = value.find('a')['href']
            if 'catalogue/' not in url:
                url = 'catalogue/' + url
            data = httpx.get(self.url + url)
            soup = BeautifulSoup(data.text, 'html.parser')
            name = soup.find('div',{'class': 'product_main'}).find('h1').text
            price = soup.find('div',{'class': 'product_main'}).find('p',{'class': 'price_color'}).text
            availability = soup.find('div',{'class':'product_main'}).find('p',{'class':'availability'}).text
            rating = soup.find('div',{'class':'product_main'}).select('p.star-rating')[0]['class'][1]
            description = None if soup.find('div',{'id':'product_description'}) == None else soup.find('div',{'id':'product_description'}).find_next('p').text
            upc = soup.find('table',{'class':'table'}).select('tr')[0].find('td').text
            product_type = soup.find('table',{'class':'table'}).select('tr')[1].find('td').text
            price_excl_tax = soup.find('table',{'class':'table'}).select('tr')[2].find('td').text
            price_incl_tax = soup.find('table',{'class':'table'}).select('tr')[3].find('td').text
            tax = soup.find('table',{'class':'table'}).select('tr')[4].find('td').text
            num_reviews = soup.find('table',{'class':'table'}).select('tr')[5].find('td').text        
            product_list.append([name, price, availability, rating, description, upc, product_type, price_excl_tax, price_incl_tax, tax, num_reviews])
        
        next = None if main_soup.find('li',{'class':'next'}) == None else main_soup.find('li',{'class':'next'}).find('a').get('href')
        if next is not None:
            self.go_to_page(next)

    def close(self):
        self.client.close()
    
    def __del__(self):
        self.close()
    
    def go_to_page(self, url, wait_time=5, retry=0):
        # self.driver.get(url)

        # if self.is_error_page():
        #     if retry > 3:
        #         print(f"Retried {retry} times and we couldn't load the page properly")
        #         exit()
        #     self.go_to_page(url, wait_time, retry + 1)

        # scroll_height = random.randint(100, 500)
        # self.driver.execute_script(f"window.scrollTo(0, {scroll_height});")
        # print(f"scrolled the page and Waiting for {wait_time} seconds to load {url}")
        # time.sleep(wait_time)

        # if self.debug:
        #     fl_name = url.split("/")[-1]
        #     print(f"Saving the page to {fl_name}.html")
        #     with open(f"{fl_name}.html", "w") as f:
        #         f.write(self.driver.page_source)
        updated_url = url
        if 'catalogue' not in url:
            updated_url = 'catalogue/' + updated_url

        self.response = self.client.get(updated_url)
        self.run()


class LocalXScraper(XScraper):
    # overide the process method
    def process(self, data):
        # process the data here
        pass

def main(url, is_local=False):
    is_local = is_local or os.getenv('IS_LOCAL', 'False').lower() == 'true'
    print(is_local)
    scraper_cls = LocalXScraper if is_local else XScraper
    scraper = scraper_cls(url)
    scraper.run()
    scraper.close()

if __name__ == '__main__':
    # main('https://example.com')
    main("https://books.toscrape.com/")
