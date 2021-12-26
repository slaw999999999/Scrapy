
from scrapy.linkextractors import LinkExtractor 
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings 
from w3lib.html import remove_tags
import pandas as pd
from urllib.parse import urlparse
import numpy as np
import scrapy.crawler as crawler
from multiprocessing import Process, Queue
from twisted.internet import reactor

class UniwersalscraperSpider(CrawlSpider):
        name = 'Uniwersalscraper'

        def __init__(self,url="",domain="", xpath_text="",xpath_title = "",xpath_date ="",xpath_pagination="" ,xpath_summarize="", xpath_all_articles_links_on_page="", *args, **kwargs):
            super(UniwersalscraperSpider, self).__init__(*args, **kwargs)  
            self.start_urls = [url]
            self.allowed_domains = [domain]
            self.xpath_text= xpath_text
            self.xpath_title = xpath_title
            self.xpath_date = xpath_date
            self.xpath_summarize= xpath_summarize
            self.xpath_pagination = xpath_pagination
            self.xpath_all_articles_links_on_page = xpath_all_articles_links_on_page
            
            
            self.rules = (Rule(LinkExtractor(restrict_xpaths=xpath_all_articles_links_on_page), callback='parse_item', follow=True),
                          Rule(LinkExtractor(restrict_xpaths=xpath_pagination)),
                    )

        def parse_item(self, response):         
            Ugly_text = response.xpath(self.xpath_text).getall()
            Good_text = [remove_tags(text) for text in Ugly_text]
            yield {
                "Title" : response.xpath(self.xpath_title).get(),
                "Date" : response.xpath(self.xpath_date).get(),
                "Summarize" : response.xpath(self.xpath_summarize).get(),
                "Text" : Good_text,
                "Url" : response.url       
            }
    

def run_spider(spider, Strings_and_XPaths):
    def f(q):
        try:
             runner = CrawlerProcess(get_project_settings())

             deferred = runner.crawl(spider,
                                    url = Strings_and_XPaths[1],
                                    domain = Strings_and_XPaths[2],
                                    xpath_title = Strings_and_XPaths[5],
                                    xpath_date = Strings_and_XPaths[6],
                                    xpath_summarize = Strings_and_XPaths[7],
                                    xpath_text = Strings_and_XPaths[8],
                                    xpath_all_articles_links_on_page= Strings_and_XPaths[3],
                                    xpath_pagination = Strings_and_XPaths[4]
             )
             deferred.addBoth(lambda _: reactor.stop())
             reactor.run()
             q.put(None)

             runner = crawler.CrawlerRunner()
             deferred = runner.crawl(spider)
             deferred.addBoth(lambda _: reactor.stop())
             reactor.run()
             q.put(None)
        except Exception as e:
            q.put(e)

    q = Queue()
    p = Process(target=f, args=(q,))
    p.start()
    result = q.get()
    p.join()

    if result is not None:
        raise result

#/mnt/c/Users/Admin/Desktop/Repozytorium/Hackathon-2022-web-scraping/Scrapy_project/Web_scraping.xlsx  # linux
#'C:\\Users\Admin\Desktop\Repozytorium\Hackathon-2022-web-scraping\Scrapy_project\Web_scraping.xlsx'   # windows

df = pd.read_excel(('/mnt/c/Users/Admin/Desktop/Repozytorium/Hackathon-2022-web-scraping/Scrapy_project/Web_scraping.xlsx') , sheet_name='GotoweXPath')
domains = [urlparse(domain).netloc for domain in np.array(df['url'])]
df['domain'] = domains   #creating domain list based on url

Data = [tuple(r) for r in df.to_numpy()]

for Strings_and_XPaths in Data:
    run_spider('Uniwersalscraper', Strings_and_XPaths)