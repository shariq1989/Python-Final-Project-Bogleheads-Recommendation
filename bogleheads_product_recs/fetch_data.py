import scrapy
from urllib.parse import urlparse

from db.config import MyDatabase
from link_obj import Link


class BogleheadsRecsSpider(scrapy.Spider):
    name = 'bogleheads_recs'
    start_urls = [
        'http://bogleheads.org',
    ]
    count = 0
    domains = {}

    # point engine to URL and start scraping
    def parse(self, response, **kwargs):
        # fetch the thread ID of each thread on the front page
        threads = self.collect_threads(response)
        print('Number of threads on front page ' + str(len(threads)))
        for thread in threads:
            # call parse_posts on each thread page
            yield scrapy.Request('https://www.bogleheads.org/forum/viewtopic.php?t=' + thread,
                                 callback=self.parse_posts)

    # collect threads on the Bogleheads.org front page
    def collect_threads(self, response):
        threads_list = []
        # get each link on the front page
        for link in response.css("tr[style*=vertical-align] td a::attr('href')").getall():
            thread_id = self.extract_thread_id(link)
            # collect thread list
            if thread_id:
                threads_list.append(thread_id)
        # convert list to set, remove duplicates at O(1) speed
        thread_set = list(set(threads_list))
        return thread_set

    # extracts thread ID from a thread's URL
    def extract_thread_id(self, link):
        # if the link leads to a thread
        if 't=' in link:
            # get thread ID
            link = link.split("t=", 1)[1]
            break_char = 0
            # check for any extra info after the thread ID
            for i, char in enumerate(link):
                if char not in '0123456789':
                    # stop when the next char is not a digit
                    break_char = i
                    break
            # remove extra info
            if break_char != 0:
                link = link[0:break_char]
            return link
        return None

    # Given thread object, crawl through posts. Access thread info using 'thread.url'
    def parse_posts(self, thread):
        # number of posts in thread
        num_posts = self.extract_num_posts(thread)
        # get the highest recorded in the database for this thread
        highest_page = self.already_in_db(thread)
        if not (highest_page == 50):
            # more than 50 posts, means that we need to crawl multiple pages
            # only process if there are unprocessed pages
            if num_posts > 50 and (not highest_page or num_posts > highest_page):
                # each page contains 50 posts, finding number of pages
                num_pages = num_posts // 50
                # iterate through each page
                if not highest_page:
                    # multi-page thread that is not in db, start from beginning
                    start_page = 1
                else:
                    # partially in db, start from page after highest page in DB
                    start_page = (highest_page // 50) + 1
                for page_num in range(start_page, num_pages):
                    # build url for specific page
                    page_url = thread.url + '&start=' + str(50 * page_num)
                    # call parse_posts on each thread page
                    yield scrapy.Request(page_url, callback=self.process_posts_from_page)
                # if there are leftover posts, process the last page
                if num_pages > int(num_pages):
                    page_url = thread.url + '&start=' + str(50 * (int(num_pages) + 1))
                    yield scrapy.Request(page_url, callback=self.process_posts_from_page)
            # single page thread that hasn't been processed yet
            elif not highest_page:
                # get all post bodies in a page
                self.process_posts_from_page(thread, False)

    # check to see whether the entire thread is already stored
    def already_in_db(self, thread):
        db = MyDatabase()
        thread_id = self.extract_thread_id(thread.url)
        thread = db.query_fetch('SELECT id FROM threads WHERE thread_id=(%s) LIMIT 1',
                                (thread_id,))
        # new thread, needs to be processed
        if not thread:
            return None
        # get the highest page tracked
        highest_page = db.query_fetch('SELECT page FROM links WHERE thread_id=(%s) ORDER BY page DESC LIMIT 1',
                                      (thread,))
        # 50 refers to page 1, which is being stored in the database as null
        if not highest_page[0]:
            return 50
        return highest_page[0]

    # store all the links in a page
    def process_posts_from_page(self, thread, multi_page=True):
        # get links if they are in a post
        links = thread.css('div.content a::attr(href)').getall()
        thread_title = thread.css('h2.topic-title a *::text').get()

        for link in links:
            # collect all links other than internal
            if 'http://' in link or 'https://' in link and 'bogleheads' not in link:
                self.count += 1
                thread_id = self.extract_thread_id(thread.url)
                # extract domain
                domain = urlparse(link).netloc
                if domain in self.domains:
                    self.domains[domain] += 1
                else:
                    self.domains[domain] = 1
                page = None
                if multi_page:
                    page = self.extract_page_num(thread.url)
                link = Link(link, thread_id, thread.url, thread_title, domain, page)
                link.write_to_database()
        if self.count:
            print('Processed links', self.count)
            print('Domains', self.domains)

    # extract page number from a thread URL
    def extract_page_num(self, link):
        # extracts page number from url
        return link.rsplit('=', 1)[1]

    # extract the total number of posts in a thread
    def extract_num_posts(self, thread):
        # get pagination info so that URL's for pages can be generated
        pagination_info = thread.css('div.pagination::text').get()
        pagination_info = pagination_info.strip()
        number_of_posts = ''
        for char in pagination_info:
            # convert '### posts' to '###'
            if char not in '0123456789':
                break
            number_of_posts += char
        return int(number_of_posts)
