import json
import re
import time
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

DOUBAN_SEARCH_JSON_URL = "https://www.douban.com/j/search"
DOUBAN_BOOK_URL = 'https://book.douban.com/subject/%s/'
DOUBAN_BOOK_CAT = "1001"
DOUBAN_CONCURRENCY_SIZE = 5  # 并发查询数
PROVIDER_NAME = "New Douban Books"
PROVIDER_ID = "new_douban"
PROVIDER_VERSION = (1, 0, 4)
PROVIDER_AUTHOR = 'Gary Fu'

BOOKNAV = (
    (
        u"文学",
        (
            u"小说",
            u"外国文学",
            u"文学",
            u"随笔",
            u"中国文学",
            u"经典",
            u"散文",
            u"日本文学",
            u"村上春树",
            u"童话",
            u"诗歌",
            u"王小波",
            u"杂文",
            u"张爱玲",
            u"儿童文学",
            u"余华",
            u"古典文学",
            u"名著",
            u"钱钟书",
            u"当代文学",
            u"鲁迅",
            u"外国名著",
            u"诗词",
            u"茨威格",
            u"杜拉斯",
            u"米兰·昆德拉",
            u"港台",
        ),
    ),
    (
        u"流行",
        (
            u"漫画",
            u"绘本",
            u"推理",
            u"青春",
            u"言情",
            u"科幻",
            u"韩寒",
            u"武侠",
            u"悬疑",
            u"耽美",
            u"亦舒",
            u"东野圭吾",
            u"日本漫画",
            u"奇幻",
            u"安妮宝贝",
            u"三毛",
            u"郭敬明",
            u"网络小说",
            u"穿越",
            u"金庸",
            u"几米",
            u"轻小说",
            u"推理小说",
            u"阿加莎·克里斯蒂",
            u"张小娴",
            u"幾米",
            u"魔幻",
            u"青春文学",
            u"高木直子",
            u"J.K.罗琳",
            u"沧月",
            u"落落",
            u"张悦然",
            u"古龙",
            u"科幻小说",
            u"蔡康永",
        ),
    ),
    (
        u"文化",
        (
            u"历史",
            u"心理学",
            u"哲学",
            u"传记",
            u"文化",
            u"社会学",
            u"设计",
            u"艺术",
            u"政治",
            u"社会",
            u"建筑",
            u"宗教",
            u"电影",
            u"数学",
            u"政治学",
            u"回忆录",
            u"思想",
            u"国学",
            u"中国历史",
            u"音乐",
            u"人文",
            u"戏剧",
            u"人物传记",
            u"绘画",
            u"艺术史",
            u"佛教",
            u"军事",
            u"西方哲学",
            u"二战",
            u"自由主义",
            u"近代史",
            u"考古",
            u"美术",
        ),
    ),
    (
        u"生活",
        (
            u"爱情",
            u"旅行",
            u"生活",
            u"励志",
            u"成长",
            u"摄影",
            u"心理",
            u"女性",
            u"职场",
            u"美食",
            u"游记",
            u"教育",
            u"灵修",
            u"情感",
            u"健康",
            u"手工",
            u"养生",
            u"两性",
            u"家居",
            u"人际关系",
            u"自助游",
        ),
    ),
    (
        u"经管",
        (
            u"经济学",
            u"管理",
            u"经济",
            u"金融",
            u"商业",
            u"投资",
            u"营销",
            u"理财",
            u"创业",
            u"广告",
            u"股票",
            u"企业史",
            u"策划",
        ),
    ),
    (
        u"科技",
        (
            u"科普",
            u"互联网",
            u"编程",
            u"科学",
            u"交互设计",
            u"用户体验",
            u"算法",
            u"web",
            u"科技",
            u"UE",
            u"UCD",
            u"通信",
            u"交互",
            u"神经网络",
            u"程序",
        ),
    ),
)

class DoubanBookSearcher:

    def __init__(self, max_workers):
        self.book_loader = DoubanBookLoader()
        self.max_workers = max_workers
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='douban_async')

    def calc_url(self, href):
        query = urlparse(href).query
        params = {item.split('=')[0]: item.split('=')[1] for item in query.split('&')}
        url = unquote(params['url'])
        return url

    def load_book_urls(self, query):
        url = DOUBAN_SEARCH_JSON_URL
        params = {"start": 0, "cat": DOUBAN_BOOK_CAT, "q": query}
        data = bytes(urlencode(params), encoding='utf8')
        res = urlopen(Request(url, data, headers={'user-agent': random_user_agent()}))
        book_urls = []
        if res.status in [200, 201]:
            book_list_content = json.load(res)
            for item in book_list_content['items'][0:self.max_workers]:  # 获取部分数据，默认5条
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

    def load_book(self, url, log):
        book = None
        start_time = time.time()
        res = urlopen(Request(url, headers={'user-agent': random_user_agent()}))
        if res.status in [200, 201]:
            log.info("Downloaded:{} Successful,Time {:.0f}ms".format(url, (time.time() - start_time) * 1000))
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
                book['authors'].extend([self.get_text(author_element) for author_element in filter(self.author_filter, element.findall("..//a"))])
            elif text.startswith("译者"):
                book['authors'].extend([self.get_text(author_element) for author_element in filter(self.author_filter, element.findall("..//a"))])
            elif text.startswith("出版社"):
                book['publisher'] = self.get_tail(element)
            elif text.startswith("副标题"):
                book['title'] = book['title'] + ':' + self.get_tail(element)
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
            book['tags'] = [self.get_text(tag_element) for tag_element in tag_elements]
        if not len(tag_elements):
            ts = []
            for nn, tags in BOOKNAV:
                for tag in tags:
                    if tag in book['title'] or tag in book['description']:
                        ts.append(tag)
                    elif tag in book['authors']:
                        ts.append(tag)
            if len(ts) > 0:
                book['tags'] = ts[:8]
        book['source'] = {
            "id": PROVIDER_ID,
            "description": PROVIDER_NAME,
            "link": "https://book.douban.com/"
        }
        return book

    def get_rating(self, rating_element):
        return float(self.get_text(rating_element, '0')) / 2

    def author_filter(self, a_element):
        a_href = a_element.attrib['href']
        return '/author' in a_href or '/search' in a_href

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
            if not text:
                text = self.get_text(element.getnext(), default_str)
        return text if text else default_str


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
    book_searcher = DoubanBookSearcher(DOUBAN_CONCURRENCY_SIZE)
    options = (
        # name, type, default, label, default, choices
        # type 'number', 'string', 'bool', 'choices'
        Option(
            'douban_concurrency_size', 'number', DOUBAN_CONCURRENCY_SIZE,
            _('Douban concurrency size:'),
            _('The number of douban concurrency cannot be too high!')
        ),
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
        concurrency_size = int(self.prefs.get('douban_concurrency_size'))
        if concurrency_size != self.book_searcher.max_workers:
            self.book_searcher = DoubanBookSearcher(concurrency_size)
        isbn = check_isbn(identifiers.get('isbn', None))
        books = self.book_searcher.search_books(isbn or title, log)
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
            mi.language = 'zh_CN'
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
                 'title': '三国演义'
             }, [title_test('三国演义', exact=True),
                 authors_test(['罗贯中'])])
        ]
    )
