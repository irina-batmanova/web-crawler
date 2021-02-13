import asyncio
import os

import aiohttp
from bs4 import BeautifulSoup
from singleton_decorator import singleton
import redis
from queue import Queue, Empty
import signal
import sys
import argparse


def signal_handler(sig, frame):
    print('You pressed Ctrl+C, exiting')
    UrlsHandler().store_links_from_prev_run()
    sys.exit(0)


@singleton
class CrawlerSettings:
    def __init__(self):
        self.dest = None
        self.init_urls = None

    def update_settings(self, dest, urls):
        self.dest = dest
        if not os.path.exists(self.dest):
            os.mkdir(self.dest)
        self.init_urls = urls


@singleton
class UrlsHandler:
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        to_visit_from_prev_run = self.redis.lrange("to_visit", 0, -1)
        self.to_visit = Queue()
        if len(to_visit_from_prev_run) == 0:
            init_urls = CrawlerSettings().init_urls
            for url in init_urls:
                self.to_visit.put(url)
        else:
            for url in to_visit_from_prev_run:
                self.to_visit.put(url.decode("utf-8"))

    def get_url_to_parse(self):
        try:
            url = self.to_visit.get()
        except Empty:
            return None
        return url

    def add_urls_to_parse(self, urls_list):
        for url in urls_list:
            num_in_redis = self.redis.get(url)
            if num_in_redis is None:
                self.to_visit.put(url)
                self.redis.set(url, 1)

    def store_links_from_prev_run(self):
        self.redis.delete("to_visit")
        if len(self.to_visit.queue) > 0:
            self.redis.lpush("to_visit", *list(self.to_visit.queue))


def link_is_valid(href):
    if href is not None and href != "" and (href.startswith("http://") or href.startswith("https://")):
        return True
    return False


async def handle_web_page(session, url):
    async with session.get(url) as response:
        html = await response.text()
        soup = BeautifulSoup(html, "lxml")
        links = []
        for link in soup.findAll('a'):
            href = link.get('href')
            if link_is_valid(href):
                links.append(href)
        f = open(os.path.join(CrawlerSettings().dest, url.replace("/", ".")), "w")
        f.write(html)
        f.close()
        UrlsHandler().add_urls_to_parse(links)


async def crawler_worker():
    async with aiohttp.ClientSession() as session:
        url = UrlsHandler().get_url_to_parse()
        while url is not None:
            await handle_web_page(session, url)
            url = UrlsHandler().get_url_to_parse()


async def asynchronous(num_workers):
    tasks = [asyncio.ensure_future(crawler_worker()) for i in range(num_workers)]
    await asyncio.wait(tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--urls', nargs='+', required=True, help='List of urls to start from')
    parser.add_argument('--dest', required=True, help='Name of folder for results')
    args = parser.parse_args()
    CrawlerSettings().update_settings(args.dest, args.urls)
    num_workers = 1
    signal.signal(signal.SIGINT, signal_handler)
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(asynchronous(num_workers))
    ioloop.close()
