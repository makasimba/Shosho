import asyncio
import aiohttp
from bs4 import BeautifulSoup
import logging
import csv
from datetime import datetime
import re
import boto3
from botocore.exceptions import ClientError
import requests
from requests.exceptions import HTTPError
from pprint import pprint as pp
import time
import random
import json
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

load_dotenv()
ROOT_URL = os.getenv('ROOT_URL')
LOG_FILE = os.getenv('LOG_FILE')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh = logging.FileHandler(LOG_FILE)
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)
logger.addHandler(sh)

CHECKPOINT_FILE = os.getenv('CHECKPOINT_FILE')
OUTPUT_FILE = os.getenv('OUTPUT_FILE')

def save_checkpoint(ckpt: int) -> None:
    with open(CHECKPOINT_FILE, mode='w', encoding='utf-8') as fout:
        json.dump({'ckpt': ckpt}, fout)

def load_checkpoint() -> int:
    try:
        with open(CHECKPOINT_FILE, mode='r', encoding='utf-8') as f:
            ckpt = json.load(f)['ckpt']
            logger.info(f'Checkpoint {ckpt} found.')
            return ckpt + 1
    except FileNotFoundError:
        logger.info('Checkpoint not found. Returning zero.')
        return 0

def load_url(number):
    return f'{ROOT_URL}?page={number}'

def convert_to_data_object(article_data):
    return {}

@retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((HTTPError, ConnectionError, requests.exceptions.RequestException)),
        reraise=True,
)
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text(encoding='utf-8')

def get_article_content(article_soup):
    wsw_div = article_soup.find('div', class_='wsw')
    if wsw_div:
        article_paragraphs = [p.get_text(strip=True) for p in wsw_div.find_all('p')]
    return '\n'.join(article_paragraphs)

@retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((ConnectionError, requests.exceptions.RequestException, HTTPError)),
        reraise=True
)
async def scrape_article(session, article_url):
    try:
        article_html = await fetch(session, article_url)
        article_soup = BeautifulSoup(article_html, 'lxml')
        
        article_date = article_soup.find('div', class_='published').find('span', class_='date').find('time').get('datetime')
        article_title = article_soup.find('h1', class_=['title', 'pg-title']).text.strip()
        
        # Extract article content
        try:
            article_content = get_article_content(article_soup)
        except AttributeError as e:
            logger.error(f'Error parsing article, {article_url}, text: {e}')
        
        scrape_date = datetime.now().strftime(r'%d, %m %Y, %H:%M:%S')

        return {'scrape_date': scrape_date, 'article_url': article_url, 'article_date': article_date, 'article_title': article_title, 'article_content': article_content}
    except Exception as e:
        logging.error(f'Error fetching article {article_url}: {e}')
        return None

async def scrape(output_file: str, root_url: str) -> None:
    ckpt = load_checkpoint()
    url = load_url(ckpt)  # has links to actual news articles

    async with aiohttp.ClientSession() as session:
        # TODO: REMOVE N PLEASE
        n = 0
        while True:
            try:
                webpage_html = await fetch(session, url)
                webpage_soup = BeautifulSoup(webpage_html, 'lxml')
                
                article_tasks = []  # use to concurrently request all the articles on this url
                
                # create a task from each link
                for link in webpage_soup.find_all('a', class_="img-wrap img-wrap--t-spac img-wrap--size-3 img-wrap--float img-wrap--xs"):
                    href = link.get('href')
                    if href:
                        article_url = f"https://www.voashona.com{href}"
                        article_tasks.append(scrape_article(session, article_url))
                
                articles_data = await asyncio.gather(*article_tasks)
                articles_data = [article for article in articles_data if article is not None]
                
                # TODO: Implement S3 upload instead of local file write
                with open(output_file, 'a', newline='') as fout:
                    for article_data in articles_data:
                        fout.write(json.dumps(article_data, ensure_ascii=False) + '\n')

                logger.info(f'Page {ckpt} scraped successfully.')
                save_checkpoint(ckpt)
            except Exception as e:
                logger.error(f'Error processing page = {ckpt}, url = {url}: {e}')
                break
            finally:
                load_more = webpage_soup.find('a', class_='btn link-showMore btn__text')
                if load_more:
                    logger.info('Loading next page.')
                    ckpt += 1
                    url = load_url(ckpt)
                else:
                    logger.info('Next page not found. Exiting.')
                    break
            n += 1
            if n >= 5:
                break

if __name__ == '__main__':
    output_file = OUTPUT_FILE
    root_url = ROOT_URL

    # TODO: Implement S3 bucket initialization

    asyncio.run(scrape(output_file=output_file, root_url=root_url))
    logger.info('VOA crawling completed.')
