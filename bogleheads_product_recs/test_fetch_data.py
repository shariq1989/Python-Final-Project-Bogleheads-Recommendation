from unittest import TestCase

from fetch_data import BogleheadsRecsSpider


class TestBogleheadsRecsSpider(TestCase):
    def test_extract_thread_id_with_new_post(self):
        link = 'https://www.bogleheads.org/forum/viewtopic.php?f=11&t=181257&newpost=5582675'
        spider = BogleheadsRecsSpider()
        thread_id = spider.extract_thread_id(link)
        self.assertEqual(thread_id, '181257')

    def test_extract_thread_id(self):
        link = 'https://www.bogleheads.org/forum/viewtopic.php?t=329194'
        spider = BogleheadsRecsSpider()
        thread_id = spider.extract_thread_id(link)
        self.assertEqual(thread_id, '329194')

    def test_extract_page_number(self):
        link = 'https://www.bogleheads.org/forum/viewtopic.php?t=327733&start=200'
        spider = BogleheadsRecsSpider()
        page = spider.extract_page_num(link)
        self.assertEqual(page, '200')