from bs4 import BeautifulSoup
import requests
from requests.exceptions import HTTPError
from pprint import pprint as pp
import time
import random
import logging
import csv
from datetime import datetime
import re

# TODO: Add Async and AWS function.

logging.basicConfig(filename='scrape.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def scrape_voa(output_file: str, root_url: str ='https://www.voashona.com/nhau-dzezimbabwe', page_number: int = 0) -> None:
    url = root_url

    n = 0
    while True:

        # Fetch webpage which has links to multiple articles
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f'unsuccessful request: {e}')
            break
        
        # Fetch all the articles on this webpage
        webpage = BeautifulSoup(response.text, 'lxml')
        articles_data = list()
        for block in webpage.find_all('div', class_='media-block'):

            try:
                # Retrieve the article url from the media block
                link = block.find('a', href=True)
                if link is None:
                    continue

                article_url = f"https://www.voashona.com/{link.get('href')}"

                # Request actual article content from article url
                article = requests.get(article_url)
                article.raise_for_status()
            except requests.RequestException as e:
                logging.error(f'failed to fetch the article: {e}')
                continue
            except AttributeError as e:
                logging.error(f'Error parsing article url: {e}')
                continue
            
            # Parse the article content and retrieve article data and metadata.
            article_soup = BeautifulSoup(article.text, 'lxml')
            scrape_date = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            article_date = article_soup.find('div', class_='published').find('span', class_='date').find('time').get('datetime')
            title = article_soup.find('h1', class_=['title', 'pg-title']).text.strip()

            # Extract all the article paragraphs and put together the article text
            article_text = []
            try:
                for article_body in article_soup.select('div.wsw'):
                    for paragraph in article_body.find_all('p'):
                        paragraph = re.sub(r'\n{2,}', ' ', paragraph.text.strip())
                        article_text.append(paragraph)
            except AttributeError as e:
                logging.error(f'Error parsing article text: {article_url}')
                continue

            content = ''.join(article_text)
            
            articles_data.append((scrape_date, article_url, article_date, page_number, title, content))
            
            # basic rate-limiting
            time.sleep(random.randint(2, 3))

            print('Article:', title)
        
        logging.info(f'{page_number} complete.')
        with open(output_file, 'a') as fout:
            writer = csv.writer(fout)
            writer.writerows(articles_data)
        
        # Retrieve the link for next webpage
        load_more = webpage.find('p', class_='btn--load-more').find('a', class_='link-showMore', href=True)
        if load_more:
            url = f"https://www.voashona.com/{load_more.get('href')}"
            page_number = url.split('=')[-1]
        else:
            break
        
        n += 1
        if n == 1:
            break

if __name__ == '__main__':
    output_file = 'voa_shona_articles.csv'
    root_url = 'https://www.voashona.com/nhau-dzezimbabwe'

    with open(output_file, 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['scrape_date', 'article_url', 'article_date', 'page_number', 'title', 'article_body'])
    
    scrape_voa(output_file, root_url, page_number=0)
    logging.info('VOA crawling completed.')