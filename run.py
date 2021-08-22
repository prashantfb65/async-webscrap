import asyncio
from timeit import default_timer
from aiohttp import ClientSession
import requests
import pickle
from collections import defaultdict
from bs4 import BeautifulSoup
import unicodedata
import traceback
from pymongo import MongoClient
from pymongo.operations import UpdateOne, InsertOne
import os
import time


mongoclient = MongoClient(host=MONGO_HOST, port=MONGO_PORT, username=MONGO_USER, password=MONGO_PASS)
db = mongoclient[MONGO_DB]
medium_posts = db['medium_posts']


articles_list = []

def fetch_async(urls):
    start_time = default_timer()
    loop = asyncio.get_event_loop() 
    future = asyncio.ensure_future(fetch_all(urls)) 
    loop.run_until_complete(future) 
    tot_elapsed = default_timer() - start_time
    print(f'Total time taken : {str(tot_elapsed)}')

async def fetch_all(urls):
    tasks = []
    fetch.start_time = dict() 
    async with ClientSession() as session:
        for url in urls:
            task = asyncio.ensure_future(fetch(url, session))
            tasks.append(task) 
        await asyncio.gather(*tasks)

async def fetch(url, session):
    fetch.start_time[url] = default_timer()
    try:
        async with session.get(url, max_redirects=30) as response:
            article = {}
            r = await response.read()
            elapsed = default_timer() - fetch.start_time[url]
            print(f"{url} took {str(elapsed)}")
            soup = BeautifulSoup(r, 'html.parser')
            title = soup.findAll('title')[0]
            title = title.get_text()
            author = soup.findAll('meta', {"name": "author"})[0]
            author = author.get('content')
            article['author'] = unicodedata.normalize('NFKD', author)
            article['link'] = url
            article['title'] = unicodedata.normalize('NFKD', title)
            paras = soup.findAll('p')
            text = ''
            nxt_line = '\n'
            for para in paras:
                text += unicodedata.normalize('NFKD',para.get_text()) + nxt_line
            article['text'] = text
            articles_list.append(article)
    except Exception as err:
        print(err)


if __name__ == '__main__':
    with open('reverse_articles_lookup.pickle', 'rb') as handle:
        b = pickle.load(handle)
    urls = list(b.keys())

    for i in range(2500, len(urls), 100):
        batched_urls = urls[i:i+100]
        fetch_async(batched_urls)
        result = medium_posts.bulk_write([
                    UpdateOne({"_id": article['link']}, {"$set": article}, upsert=True) for article in articles_list
                ], ordered=False)
        print(f"ingested {result.upserted_count} new medium posts and updated {result.matched_count}")
        articles_list = []
        time.sleep(10)
