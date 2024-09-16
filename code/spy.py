import asyncio
import aiohttp
from bs4 import BeautifulSoup
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import requests
from requests.exceptions import HTTPError
import json
import os
import re
import random
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

load_dotenv()  # load environment variables from .env

ROOT_URL = os.getenv('ROOT_URL')
LOG_FILE = os.getenv('LOG_FILE')
BUCKET_NAME = os.getenv('BUCKET')
CHECKPOINT_FILE = os.getenv('CHECKPOINT_FILE')
REGION = os.getenv('REGION')

s3_client = boto3.client('s3', region_name=REGION)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')

fh = logging.FileHandler(LOG_FILE)
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)
logger.addHandler(sh)


def save_checkpoint(ckpt: int, total_tokens: int) -> None:
    with open(CHECKPOINT_FILE, mode='w', encoding='utf-8') as fout:
        json.dump({'ckpt': ckpt, 'total_tokens': total_tokens}, fout)
        

def load_checkpoint() -> int:
    try:
        with open(CHECKPOINT_FILE, mode='r', encoding='utf-8') as f:
            d = json.load(f)

            ckpt = d['ckpt']
            total_tokens = d['total_tokens']

            logger.info(f'Checkpoint found: {ckpt}')
            logger.info(f'Total tokens:  {total_tokens}')
            return ckpt + 1
    except FileNotFoundError:
        logger.info('Checkpoint not found.')
        return 0, 0

def load_url(x):
    return f'{ROOT_URL}?page={x}'

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text(encoding='utf-8')

def extract_content(article_soup):
    wsw_div = article_soup.find('div', class_='wsw')
    if wsw_div:
        article_paragraphs = [p.get_text(strip=True) for p in wsw_div.find_all('p')]
    else:
        return ''
    return '\n'.join(article_paragraphs)

@retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((ConnectionError, requests.exceptions.RequestException, HTTPError)),
        reraise=True
)
async def scrape_article(session, article_url):
    try:
        # fetch article HTML
        article_html = await fetch(session, article_url)
        article_soup = BeautifulSoup(article_html, 'lxml')
        
        article_date = article_soup.find('div', class_='published').find('span', class_='date').find('time').get('datetime')
        article_title = article_soup.find('h1', class_=['title', 'pg-title']).text.strip()
        scrape_date = datetime.now().strftime(r'%d, %m %Y, %H:%M:%S')
        
        try:
            # Extract article content
            article_content = extract_content(article_soup)
        except Exception as e:
            logger.error(f'Error parsing article, {article_url}, text: {e}')
        
        # Return article data
        return {'scrape_date': scrape_date, 'article_url': article_url, 'article_date': article_date, 'article_title': article_title, 'article_content': article_content}
    except Exception as e:
        logging.error(f'Error fetching article {article_url}: {e}')
        return None

async def upload_to_bucket(data, bucket, key):
    try:
        s3_client.put_object(Body=data, Bucket=bucket, Key=key)
        logger.info(f'Successfully uploaded {key} to {bucket}')
    except ClientError as e:
        logger.error(f'Error uploading {key} to {bucket}: {e}')

async def scrape(articles_per_file: int = 10) -> None:
    ckpt, total_tokens = load_checkpoint()
    url = load_url(ckpt)  # has multiple links to actual news articles on this url

    async with aiohttp.ClientSession() as session:
        articles_batch = []

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
                total_tokens += sum(len(article['article_content'].split()) for article in articles_data)
                articles_batch.extend(articles_data)

                if len(articles_data) >= articles_per_file:
                    batch_data = articles_batch[:articles_per_file]
                    articles_batch = articles_batch[articles_per_file:]

                    key = f'DATA/{ckpt: 07}.json'
                    data='\n'.join(json.dumps(article, ensure_ascii=False) for article in batch_data)
                    await upload_to_bucket(data=data, bucket=BUCKET_NAME, key=key)

                logger.info(f'Page {ckpt} scraped successfully.')
                logger.info(f'Estimated scraped total tokens: {total_tokens}')
                save_checkpoint(ckpt, total_tokens)
            except Exception as e:
                logger.error(f'Error processing page = {ckpt}, url = {url}: {e}')
                break
            finally:
                load_more = webpage_soup.find('a', class_='btn link-showMore btn__text')
                if load_more:
                    logger.info('Loading next page.')
                    ckpt += 1
                    url = load_url(ckpt)

                    # simulate basic human-like browsing and avoid getting blocked
                    wait_for = random.uniform(2, 7)
                    logger.info(f'Waiting for {wait_for:.2f} seconds before loading next page.')
                    await asyncio.sleep(wait_for)
                else:
                    logger.info('Next page not found. Exiting.')
                    break

if __name__ == '__main__':
    asyncio.run(scrape())
    logger.info('crawling completed.')

# TODO: Keep and log number of tokens collected
# TODO: Attach a notify service to this script