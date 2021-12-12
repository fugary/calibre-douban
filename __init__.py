import hashlib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from queue import Queue, Empty
from urllib.parse import urlparse, unquote, urlencode
from urllib.request import Request, urlopen

from calibre.ebooks.metadata.book.base import Metadata
from calibre.ebooks.metadata.sources.base import Source, Option
from lxml import etree

DOUBAN_SEARCH_JSON_URL = "https://www.douban.com/j/search"
DOUBAN_BOOK_URL = 'https://book.douban.com/subject/%s/'
DOUBAN_BOOK_CAT = "1001"
DOUBAN_BOOK_CACHE_SIZE = 500  # 最大缓存数量
DOUBAN_CONCURRENCY_SIZE = 5  # 并发查询数
DEFAULT_HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3573.0 Safari/537.36'
}
PROVIDER_NAME = "New Douban Books"
PROVIDER_ID = "new_douban"


class DoubanBookSearcher:

    def __init__(self):
        self.book_loader = DoubanBookLoader()
        self.thread_pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix='douban_async')

    def calc_url(self, href):
        query = urlparse(href).query
        params = {item.split('=')[0]: item.split('=')[1] for item in query.split('&')}
        url = unquote(params['url'])
        return url

    def load_book_urls(self, query):
        url = DOUBAN_SEARCH_JSON_URL
        params = {"start": 0, "cat": DOUBAN_BOOK_CAT, "q": query}
        data = bytes(urlencode(params), encoding='utf8')
        res = urlopen(Request(url, data, headers=DEFAULT_HEADERS))
        book_urls = []
        if res.status in [200, 201]:
            book_list_content = json.load(res)
            for item in book_list_content['items'][0:DOUBAN_CONCURRENCY_SIZE]:  # 获取部分数据，默认5条
                html = etree.HTML(item)
                a = html.xpath('//a[@class="nbg"]')
                if len(a):
                    href = a[0].attrib['href']
                    parsed = self.calc_url(href)
                    book_urls.append(parsed)
        return book_urls

    def search_books(self, query, log):
        book_urls = self.load_book_urls(query)
        books = []
        futures = [self.thread_pool.submit(self.book_loader.load_book, book_url, log) for book_url in book_urls]
        for future in as_completed(futures):
            book = future.result()
            if book is not None:
                books.append(future.result())
        return books


class DoubanBookLoader:

    def __init__(self):
        self.book_parser = DoubanBookHtmlParser()

    @lru_cache(maxsize=DOUBAN_BOOK_CACHE_SIZE)
    def load_book(self, url, log):
        book = None
        start_time = time.time()
        res = urlopen(Request(url, headers=DEFAULT_HEADERS))
        if res.status in [200, 201]:
            log.info("下载书籍:{}成功,耗时{:.0f}ms".format(url, (time.time() - start_time) * 1000))
            book_detail_content = res.read()
            book = self.book_parser.parse_book(url, book_detail_content.decode("utf8"))
        return book


class DoubanBookHtmlParser:
    def __init__(self):
        self.id_pattern = re.compile(".*/subject/(\\d+)/?")

    def parse_book(self, url, book_content):
        book = {}
        html = etree.HTML(book_content)
        title_element = html.xpath("//span[@property='v:itemreviewed']")
        book['title'] = self.get_text(title_element)
        share_element = html.xpath("//a[@data-url]")
        if len(share_element):
            url = share_element[0].attrib['data-url']
        book['url'] = url
        id_match = self.id_pattern.match(url)
        if id_match:
            book['id'] = id_match.group(1)
        img_element = html.xpath("//a[@class='nbg']")
        if len(img_element):
            cover = img_element[0].attrib['href']
            if not cover or cover.endswith('update_image'):
                book['cover'] = ''
            else:
                book['cover'] = cover
        rating_element = html.xpath("//strong[@property='v:average']")
        book['rating'] = self.get_rating(rating_element)
        elements = html.xpath("//span[@class='pl']")
        book['authors'] = []
        book['publisher'] = ''
        for element in elements:
            text = self.get_text(element)
            if text.startswith("作者"):
                book['authors'].extend([self.get_text(author_element) for author_element in element.findall("..//a")])
            elif text.startswith("译者"):
                book['authors'].extend([self.get_text(author_element) for author_element in element.findall("..//a")])
            elif text.startswith("出版社"):
                book['publisher'] = self.get_tail(element)
            elif text.startswith("出版年"):
                book['publishedDate'] = self.get_tail(element)
            elif text.startswith("ISBN"):
                book['isbn'] = self.get_tail(element)
            elif text.startswith("丛书"):
                book['series'] = self.get_text(element.getnext())
        summary_element = html.xpath("//div[@id='link-report']//div[@class='intro']")
        book['description'] = ''
        if len(summary_element):
            book['description'] = etree.tostring(summary_element[-1], encoding="utf8").decode("utf8").strip()
        tag_elements = html.xpath("//a[contains(@class, 'tag')]")
        if len(tag_elements):
            book['tags'] = [tag_element.text.strip() for tag_element in tag_elements]
        book['source'] = {
            "id": PROVIDER_ID,
            "description": PROVIDER_NAME,
            "link": "https://book.douban.com/"
        }
        return book

    def get_rating(self, rating_element):
        return float(self.get_text(rating_element, '0')) / 2

    def get_text(self, element, default_str=''):
        text = default_str
        if len(element) and element[0].text:
            text = element[0].text.strip()
        elif isinstance(element, etree._Element) and element.text:
            text = element.text.strip()
        return text if text else default_str

    def get_tail(self, element, default_str=''):
        text = default_str
        if isinstance(element, etree._Element) and element.tail:
            text = element.tail.strip()
        return text if text else default_str


class NewDouban(Source):
    name = 'New Douban Books'  # Name of the plugin
    description = 'Downloads metadata and covers from Douban Books web site.'
    supported_platforms = ['windows', 'osx', 'linux']  # Platforms this plugin will run on
    author = 'Gary Fu'  # The author of this plugin
    version = (1, 0, 0)  # The version number of this plugin
    minimum_calibre_version = (5, 0, 0)
    capabilities = frozenset(['identify', 'cover'])
    touched_fields = frozenset([
        'title', 'authors', 'tags', 'pubdate', 'comments', 'publisher',
        'identifier:isbn', 'rating', 'identifier:' + PROVIDER_ID
    ])  # language currently disabled
    book_searcher = DoubanBookSearcher()

    options = (
        Option(),
    )

    def get_book_url(self, identifiers):  # {{{
        douban_id = identifiers.get(PROVIDER_ID, None)
        if douban_id is not None:
            return (PROVIDER_ID, douban_id, DOUBAN_BOOK_URL % douban_id)

    def download_cover(
            self,
            log,
            result_queue,
            abort,
            title=None,
            authors=None,
            identifiers={},
            timeout=30,
            get_best_cover=False):
        cached_url = self.get_cached_cover_url(identifiers)
        if cached_url is None:
            log.info('No cached cover found, running identify')
            rq = Queue()
            self.identify(
                log,
                rq,
                abort,
                title=title,
                authors=authors,
                identifiers=identifiers
            )
            if abort.is_set():
                return
            results = []
            while True:
                try:
                    results.append(rq.get_nowait())
                except Empty:
                    break
            results.sort(
                key=self.identify_results_keygen(
                    title=title, authors=authors, identifiers=identifiers
                )
            )
            for mi in results:
                cached_url = self.get_cached_cover_url(mi.identifiers)
                if cached_url is not None:
                    break
        if cached_url is None:
            log.info('No cover found')
            return
        log.info('下载封面地址:', cached_url)
        br = self.browser
        log('Downloading cover from:', cached_url)
        try:
            cdata = br.open_novisit(cached_url, timeout=timeout).read()
            if cdata:
                result_queue.put((self, cdata))
        except:
            log.exception('Failed to download cover from:', cached_url)

    def get_cached_cover_url(self, identifiers):  # {{{
        url = None
        db = identifiers.get(PROVIDER_ID, None)
        if db is None:
            isbn = identifiers.get('isbn', None)
            if isbn is not None:
                db = self.cached_isbn_to_identifier(isbn)
        if db is not None:
            url = self.cached_identifier_to_cover_url(db)

        return url

    def identify(
            self,
            log,
            result_queue,
            abort,
            title=None,
            authors=None,  # {{{
            identifiers={},
            timeout=30):
        books = self.book_searcher.search_books(title, log)
        for book in books:
            ans = self.to_metadata(book, log)
            if isinstance(ans, Metadata):
                db = ans.identifiers[PROVIDER_ID]
                if ans.isbn:
                    self.cache_isbn_to_identifier(ans.isbn, db)
                if ans.cover:
                    self.cache_identifier_to_cover_url(db, ans.cover)
                self.clean_downloaded_metadata(ans)
                result_queue.put(ans)

    def to_metadata(self, book, log):
        if book:
            mi = Metadata(book['title'], book['authors'])
            mi.identifiers = {PROVIDER_ID: book['id']}
            mi.url = book['url']
            mi.cover = book.get('cover', None)
            mi.publisher = book['publisher']
            pubdate = book['publishedDate']
            if pubdate:
                try:
                    if re.compile('^\\d{4}-\\d+$').match(pubdate):
                        mi.pubdate = datetime.strptime(pubdate, '%Y-%m')
                    elif re.compile('^\\d{4}-\\d+-\\d+$').match(pubdate):
                        mi.pubdate = datetime.strptime(pubdate, '%Y-%m-%d')
                except:
                    log.error('Failed to parse pubdate %r' % pubdate)
            mi.comments = book['description']
            mi.tags = book['tags']
            mi.rating = book['rating']
            mi.isbn = book['isbn']
            mi.series = book.get('series', [])
            mi.language = 'zh_CN'
            log.info('parsed book', book)
            return mi


if __name__ == "__main__":
    # To run these test use: calibre-debug -e ./__init__.py
    from calibre.ebooks.metadata.sources.test import (
        test_identify_plugin, title_test, authors_test
    )

    test_identify_plugin(
        NewDouban.name, [
            ({
                 'identifiers': {
                     'isbn': '9787536692930'
                 },
                 'title': '三体',
                 'authors': ['刘慈欣']
             }, [title_test('三体', exact=True),
                 authors_test(['刘慈欣'])]),
            ({
                 'title': 'Linux内核修炼之道',
                 'authors': ['任桥伟']
             }, [title_test('Linux内核修炼之道', exact=False)]),
        ]
    )
