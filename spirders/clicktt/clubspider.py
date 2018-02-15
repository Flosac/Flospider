import json
import logging
from multiprocessing.dummy import Manager
import random
from urllib.parse import urlparse, parse_qs
from config.config import BASEPYCURLCONFIG
from core import MySpider
from core.Spider import CrawlJob

__author__ = 'Florian'
m = Manager()
crawled = m.dict()

logger = logging.getLogger("logger")
fh = logging.FileHandler("clubs.jsonl", 'a+')
simpleFormat = logging.Formatter("%(message)s")
fh.setFormatter(simpleFormat)
fh.setLevel(logging.WARNING)
ch = logging.StreamHandler()
ch.setFormatter(simpleFormat)
ch.setLevel(logging.DEBUG)
logger.addHandler(fh)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

@MySpider.QueueInitializer.register()
def seeds():
    jobs = []
    logger = logging.getLogger("logger")
    for federation in ['WTTV','DTTB','TTVBW','TTVWH','TTVB','BTTV','FTTB','HeTTV','TTVMV','TTVN','RTTV','TTVR','PTTV','STTB','TTVSA','TTTV','TTVSH','SbTTV','BaTTV']:
        host = random.choice(['wttv', 'dttb','rttv','ttvn'])
        url = 'http://{}.click-tt.de/cgi-bin/WebObjects/nuLigaTTDE.woa/wa/clubSearch?federation={}'.format(host, federation)
        logger.debug(url)
        job = CrawlJob(url, fed=federation, typ='seed')
        jobs.append(job)
    return jobs
@MySpider.HtmlProcessors.register(crawled)
def crawlOverview(r, job, spider, crawled):
    if job._ADD['typ'] == 'seed':
        root = r.root()
        root.make_links_absolute(job.url)
        for link in root.xpath("//div[@id='content-row1']//a/@href"):
            if link not in crawled:
                crawled[link]=True
                q = urlparse(link).query
                query = parse_qs(q)
                region = query['regionName'][0]
                spider.addToQueue(CrawlJob(link, typ="region", fed=job._ADD['fed'], region=region))
    return True

@MySpider.HtmlProcessors.register(crawled)
def crawlOverview(r, job, spider, crawled):
    logger = logging.getLogger("logger")
    if job._ADD['typ'] == 'region':
        root = r.root()
        root.make_links_absolute(job.url)
        for row in root.xpath("//table[@class='result-set']//tr"):
            try:
                addresses = []
                link = row[0][0]
                if link.tag.lower() == "a":
                    href = link.get('href')
                    q = urlparse(href).query
                    query = parse_qs(q)
                    clubid = query['club'][0]
                    name = link.text_content().strip()
                    for address in row[1].xpath(".//li/text()"):
                        addresses.append(" ".join( [x.strip() for x in address.strip().splitlines()]))
                    logger.warning(json.dumps({'clubid': clubid, 'name': name, 'gyms': addresses, 'fed': job._ADD['fed'], 'region': job._ADD['region'] }))

                else:
                    logger.debug('NOT AN A TAG')
            except Exception as e:
                print(e)
    return True

MySpider.run(10,jobsPerWorker=10, **BASEPYCURLCONFIG)