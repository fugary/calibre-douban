import re
import time
import random
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue, Empty
from urllib.parse import urlparse, unquote, urlencode
from urllib.request import Request, urlopen

from calibre import random_user_agent
from calibre.ebooks.metadata import check_isbn
from calibre.ebooks.metadata.book.base import Metadata
from calibre.ebooks.metadata.sources.base import Source, Option
from lxml import etree
from bs4 import BeautifulSoup, Tag

DOUBAN_BOOK_BASE = "https://book.douban.com/"
DOUBAN_SEARCH_JSON_URL = "https://www.douban.com/j/search"
DOUBAN_SEARCH_URL = "https://www.douban.com/search"
DOUBAN_BOOK_URL = 'https://book.douban.com/subject/%s/'
DOUBAN_BOOK_CAT = "1001"
DOUBAN_CONCURRENCY_SIZE = 5  # 并发查询数
DOUBAN_BOOK_URL_PATTERN = re.compile(".*/subject/(\\d+)/?")
PROVIDER_NAME = "New Douban Books"
PROVIDER_ID = "new_douban"
PROVIDER_VERSION = (2, 2, 2)
PROVIDER_AUTHOR = 'Gary Fu'


class DoubanBookSearcher:

    def __init__(self, max_workers, douban_delay_enable, douban_login_cookie):
        self.book_parser = DoubanBookHtmlParser()
        self.max_workers = max_workers
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='douban_async')
        self.douban_delay_enable = douban_delay_enable
        self.douban_login_cookie = douban_login_cookie

    def calc_url(self, href):
        query = urlparse(href).query
        params = {item.split('=')[0]: item.split('=')[1] for item in query.split('&')}
        url = unquote(params['url'])
        if DOUBAN_BOOK_URL_PATTERN.match(url):
            return url

    def load_book_urls_new(self, query, log):
        params = {"cat": DOUBAN_BOOK_CAT, "q": query}
        url = DOUBAN_SEARCH_URL + "?" + urlencode(params)
        log.info(f'Load books by search url: {url}')
        res = urlopen(Request(url, headers=self.get_headers(), method='GET'))
        book_urls = []
        if res.status in [200, 201]:
            html_content = self.get_res_content(res)
            if self.is_prohibited(html_content, log):
                return book_urls
            html = etree.HTML(html_content)
            alist = html.xpath('//a[@class="nbg"]')
            for link in alist:
                href = link.attrib['href']
                parsed = self.calc_url(href)
                if parsed:
                    if len(book_urls) < self.max_workers:
                        book_urls.append(parsed)
        return book_urls

    def search_books(self, query, log):
        book_urls = self.load_book_urls_new(query, log)
        books = []
        futures = [self.thread_pool.submit(self.load_book, book_url, log) for book_url in book_urls]
        for future in as_completed(futures):
            book = future.result()
            if self.is_valid_book(book):
                books.append(book)
        return books

    def load_book(self, url, log):
        book = None
        start_time = time.time()
        if self.douban_delay_enable:
            self.random_sleep(log)
        res = urlopen(Request(url, headers=self.get_headers(), method='GET'))
        if res.status in [200, 201]:
            book_detail_content = self.get_res_content(res)
            if self.is_prohibited(book_detail_content, log):
                return
            log.info("Downloaded:{} Successful,Time {:.0f}ms".format(url, (time.time() - start_time) * 1000))
            try:
                book = self.book_parser.parse_book(url, book_detail_content)
                if not self.is_valid_book(book):
                    log.info(f"Parse book content error: {book_detail_content}")
            except Exception as e:
                log.info(f"Parse book content error: {e} \n Content: {book_detail_content}")
        return book

    def is_valid_book(self, book):
        return book is not None and book.get('title', None)

    def is_prohibited(self, html_content, log):
        prohibited = html_content is not None and '<title>禁止访问</title>' in html_content
        if prohibited:
            html = etree.HTML(html_content)
            content = html.xpath('//div[@id="content"]')
            if content:
                html_content = etree.tostring(content[0], encoding='unicode', method='html')
            log.info(f'Douban网页访问失败：{html_content}')
        return prohibited

    def get_res_content(self, res):
        encoding = res.info().get('Content-Encoding')
        if encoding == 'gzip':
            res_content = gzip.decompress(res.read())
        else:
            res_content = res.read()
        return res_content.decode(res.headers.get_content_charset())

    def get_headers(self):
        headers = {'User-Agent': random_user_agent(), 'Accept-Encoding': 'gzip, deflate'}
        if self.douban_login_cookie:
            headers['Cookie'] = self.douban_login_cookie
        return headers

    def random_sleep(self, log):
        random_sec = random.random() / 10
        log.info("Random sleep time {}s".format(random_sec))
        time.sleep(random_sec)


class DoubanBookHtmlParser:
    def __init__(self):
        self.id_pattern = DOUBAN_BOOK_URL_PATTERN  # 假设是 re.Pattern，如 re.compile(r'/subject/(\d+)/')
        self.tag_pattern = re.compile(r"criteria = '(.+)'")

    def parse_book(self, url, book_content):
        try:
            soup = BeautifulSoup(book_content, 'html.parser')
        except Exception as e:
            print(f"[ERROR] BeautifulSoup parse failed: {e}")
            return None

        book = {}

        # --- 1. 标题 ---
        title_element = soup.find('span', {'property': 'v:itemreviewed'})
        book['title'] = self.get_text([title_element] if title_element else [])

        # --- 2. URL & ID ---
        share_element = soup.find('a', {'data-url': True})
        if share_element and share_element.get('data-url'):
            url = share_element['data-url'].strip()
        book['url'] = url

        id_match = self.id_pattern.search(url)  # 改用 search 更健壮
        book['id'] = id_match.group(1) if id_match else ''

        # --- 3. 封面 ---
        img_element = soup.find('a', class_='nbg')
        book['cover'] = ''
        if img_element and img_element.get('href'):
            cover = img_element['href'].strip()
            if cover and not cover.endswith('update_image'):
                book['cover'] = cover

        # --- 4. 评分 ---
        rating_element = soup.find('strong', {'property': 'v:average'})
        book['rating'] = self.get_rating([rating_element] if rating_element else [])

        # --- 5. 信息字段：作者/译者/出版社等 ---
        elements = soup.find_all('span', class_='pl')
        book['authors'] = []
        book['translators'] = []
        book['publisher'] = ''
        book['publishedDate'] = ''
        book['isbn'] = ''
        book['series'] = ''

        for element in elements:
            text = self.get_text([element])
            # 找到包含该 span 的父容器（通常是 <div id="info"> 下的某行）
            parent_div = element.find_parent()

            if text.startswith("作者"):
                # 模拟原 xpath: ..//a → 当前 span 的父级下所有 <a>（排除广告等）
                # 原逻辑用 author_filter 过滤，这里同样过滤
                a_list = parent_div.find_all('a') if parent_div else []
                authors = [
                    self.get_text([a]) for a in a_list
                    if self.author_filter(a)
                ]
                book['authors'].extend(authors)

            elif text.startswith("译者"):
                a_list = parent_div.find_all('a') if parent_div else []
                translators = [
                    self.get_text([a]) for a in a_list
                    if self.author_filter(a)
                ]
                book['translators'].extend(translators)

            elif text.startswith("出版社"):
                book['publisher'] = self.get_tail(element)

            elif text.startswith("副标题"):
                tail = self.get_tail(element)
                if tail:
                    book['title'] = f"{book['title']}:{tail}"

            elif text.startswith("出版年"):
                book['publishedDate'] = self.get_tail(element)

            elif text.startswith("ISBN"):
                book['isbn'] = self.get_tail(element)

            elif text.startswith("丛书"):
                next_elem = element.find_next_sibling()
                if next_elem and isinstance(next_elem, Tag) and next_elem.name == 'a':
                    book['series'] = self.get_text([next_elem])

        # --- 6. 简介（description）---
        summary_elements = soup.select('#link-report div.intro')
        book['description'] = ''
        if summary_elements:
            # 取最后一个（通常是展开后的完整简介）
            last_intro = summary_elements[-1]
            # 模拟原逻辑：etree.tostring → 保留 HTML 结构（非纯文本）
            book['description'] = str(last_intro).strip()

        # --- 7. 标签（tags）---
        tag_elements = soup.find_all('a', class_=lambda x: x and 'tag' in x.split())
        if tag_elements:
            book['tags'] = [self.get_text([tag]) for tag in tag_elements]
        else:
            book['tags'] = self.get_tags(book_content)

        # --- 8. 来源（source）---
        book['source'] = {
            "id": PROVIDER_ID,
            "description": PROVIDER_NAME,
            "link": DOUBAN_BOOK_BASE
        }

        # --- 9. 语言 ---
        book['language'] = self.get_book_language(book['title'])

        return book

    # =============== 以下为辅助方法（适配 BeautifulSoup） ===============

    def get_book_language(self, title):
        pattern = r'^[a-zA-Z\-_]+$'
        if title and ('英文版' in title or bool(re.match(pattern, title))):
            return 'en_US'
        return 'zh_CN'

    def get_tags(self, book_content):
        tag_match = self.tag_pattern.findall(book_content)
        if tag_match:
            # 注意：原代码 tag_match[0].split('|')，但 criteria 可能是 JS 字符串，含引号/转义
            criteria_str = tag_match[0]
            # 尝试提取 7: 开头的标签（如 "7:小说|7:历史"）
            tags = []
            for part in criteria_str.split('|'):
                part = part.strip()
                if part.startswith('7:'):
                    tag = part[2:].strip()
                    if tag:
                        tags.append(tag)
            return tags
        return []

    def get_rating(self, rating_elements):
        # rating_elements 是 [Tag] 或 []
        text = self.get_text(rating_elements, '0')
        try:
            return float(text) / 2  # 注意：原逻辑是 /2！（豆瓣10分制 → 你的5分制？）
        except (ValueError, TypeError):
            return 0.0

    def author_filter(self, a_element):
        # a_element 是 BeautifulSoup.Tag
        href = a_element.get('href', '')
        return '/author' in href or '/search' in href

    def get_text(self, elements, default_str=''):
        """
        兼容原接口：elements 是 [Tag] 或 空列表
        """
        if not elements or not isinstance(elements, (list, tuple)):
            return default_str
        for elem in elements:
            if isinstance(elem, Tag) and elem.get_text(strip=True):
                return elem.get_text(strip=True)
        return default_str

    def get_tail(self, element, default_str=''):
        """
        改进版：跳过冒号、空格、换行，取后续第一个有效文本内容
        """
        if not isinstance(element, Tag):
            return default_str

        # 遍历后续兄弟节点，直到找到非空文本
        for sibling in element.next_siblings:
            if isinstance(sibling, str):
                # 清理文本：去空格、去冒号、去中文冒号
                text = sibling.strip().strip(':： \n\t')
                if text:
                    return text
            elif isinstance(sibling, Tag):
                # 如果是标签，且是 <a>，取其文本（如出版社是链接）
                if sibling.name == 'a' and sibling.get_text(strip=True):
                    return sibling.get_text(strip=True)
                # 遇到 <br>, <span>, <div> 等，停止（避免跨字段）
                if sibling.name in ['br', 'span', 'div', 'p']:
                    break
        return default_str

class NewDoubanBooks(Source):
    name = 'New Douban Books'  # Name of the plugin
    description = 'Downloads metadata and covers from Douban Books web site.'
    supported_platforms = ['windows', 'osx', 'linux']  # Platforms this plugin will run on
    author = PROVIDER_AUTHOR  # The author of this plugin
    version = PROVIDER_VERSION  # The version number of this plugin
    minimum_calibre_version = (5, 0, 0)
    capabilities = frozenset(['identify', 'cover'])
    touched_fields = frozenset([
        'title', 'authors', 'tags', 'pubdate', 'comments', 'publisher',
        'identifier:isbn', 'rating', 'identifier:' + PROVIDER_ID
    ])  # language currently disabled
    book_searcher = None
    options = (
        # name, type, default, label, default, choices
        # type 'number', 'string', 'bool', 'choices'
        Option(
            'douban_concurrency_size', 'number', DOUBAN_CONCURRENCY_SIZE,
            _('Douban concurrency size:'),
            _('The number of douban concurrency cannot be too high!')
        ),
        Option(
            'add_translator_to_author', 'bool', True,
            _('Add translator to author'),
            _('If selected, translator will be written to metadata as author')
        ),
        Option(
            'douban_delay_enable', 'bool', True,
            _('douban random delay'),
            _('Random delay for a period of time before request')
        ),
        Option(
            'douban_search_with_author', 'bool', True,
            _('search with authors'),
            _('add authors to search keywords')
        ),
        Option(
            'douban_login_cookie', 'string', None,
            _('douban login cookie'),
            _('Browser cookie after login')
        ),
    )

    def __init__(self, *args, **kwargs):
        Source.__init__(self, *args, **kwargs)
        concurrency_size = int(self.prefs.get('douban_concurrency_size'))
        douban_delay_enable = bool(self.prefs.get('douban_delay_enable'))
        douban_login_cookie = self.prefs.get('douban_login_cookie')
        self.douban_search_with_author = bool(self.prefs.get('douban_search_with_author'))
        self.book_searcher = DoubanBookSearcher(concurrency_size, douban_delay_enable, douban_login_cookie)

    def get_book_url(self, identifiers):  # {{{
        douban_id = identifiers.get(PROVIDER_ID, None)
        if douban_id is None:
            douban_id = identifiers.get('douban', None)
        if douban_id is not None:
            return PROVIDER_ID, douban_id, DOUBAN_BOOK_URL % douban_id

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
        br = self.browser
        log('Downloading cover from:', cached_url)
        try:
            if self.book_searcher.douban_login_cookie:
                br = br.clone_browser()
                br.set_current_header('Cookie', self.book_searcher.douban_login_cookie)
            br.set_current_header('Referer', DOUBAN_BOOK_BASE)
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
        add_translator_to_author = self.prefs.get(
            'add_translator_to_author')

        isbn = check_isbn(identifiers.get('isbn', None))
        new_douban = self.get_book_url(identifiers)
        if new_douban:
            # 如果有new_douban的id，直接精确获取数据
            log.info(f'Load book by {PROVIDER_ID}:{new_douban[1]}')
            book = self.book_searcher.load_book(new_douban[2], log)
            books = []
            if self.book_searcher.is_valid_book(book):
                books.append(book)
        else:
            search_keyword = title
            if self.douban_search_with_author and title and authors:
                authors_str = ','.join(authors)
                search_keyword = f'{title} {authors_str}'
            books = self.book_searcher.search_books(isbn or search_keyword, log)
            if not len(books) and title and (isbn or search_keyword != title):
                books = self.book_searcher.search_books(title, log)  # 用isbn或者title+auther没有数据，用title重新搜一遍
        for book in books:
            ans = self.to_metadata(book, add_translator_to_author, log)
            if isinstance(ans, Metadata):
                db = ans.identifiers[PROVIDER_ID]
                if ans.isbn:
                    self.cache_isbn_to_identifier(ans.isbn, db)
                if ans.cover:
                    self.cache_identifier_to_cover_url(db, ans.cover)
                self.clean_downloaded_metadata(ans)
                result_queue.put(ans)

    def to_metadata(self, book, add_translator_to_author, log):
        if book:
            authors = (book['authors'] + book['translators']
                       ) if add_translator_to_author else book['authors']
            mi = Metadata(book['title'], authors)
            mi.identifiers = {PROVIDER_ID: book['id']}
            mi.url = book['url']
            mi.cover = book.get('cover', None)
            mi.publisher = book['publisher']
            pubdate = book.get('publishedDate', None)
            if pubdate:
                try:
                    if re.compile('^\\d{4}-\\d+$').match(pubdate):
                        mi.pubdate = datetime.strptime(pubdate, '%Y-%m')
                    elif re.compile('^\\d{4}-\\d+-\\d+$').match(pubdate):
                        mi.pubdate = datetime.strptime(pubdate, '%Y-%m-%d')
                except:
                    log.error('Failed to parse pubdate %r' % pubdate)
            mi.comments = book['description']
            mi.tags = book.get('tags', [])
            mi.rating = book['rating']
            mi.isbn = book.get('isbn', '')
            mi.series = book.get('series', [])
            mi.language = book.get('language', 'zh_CN')
            log.info('parsed book', book)
            return mi


if __name__ == "__main__":
    # To run these test use: calibre-debug -e ./__init__.py
    from calibre.ebooks.metadata.sources.test import (
        test_identify_plugin, title_test, authors_test
    )

    test_identify_plugin(
        NewDoubanBooks.name, [
            ({
                 'identifiers': {
                     'isbn': '9787111544937'
                 },
                 'title': '深入理解计算机系统（原书第3版）'
             }, [title_test('深入理解计算机系统（原书第3版）', exact=True),
                 authors_test(['randal e.bryant', "david o'hallaron", '贺莲', '龚奕利'])]),
            ({
                 'title': '凤凰架构'
             }, [title_test('凤凰架构:构建可靠的大型分布式系统', exact=True),
                 authors_test(['周志明'])])
        ]
    )
