from scrapy import cmdline

cmdline.execute("scrapy runspider fetch_data.py -L WARNING".split())
