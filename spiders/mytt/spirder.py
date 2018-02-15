import logging
import os
from core import MySpider
from core.Spider import CrawlJob

__author__ = 'Florian'

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
h = logging.StreamHandler()
h.setLevel(logging.DEBUG)
logger.addHandler(h)
@MySpider.QueueInitializer.register()
def init():

    job = CrawlJob("https://www.mytischtennis.de/clicktt/verbaende", sitetype="start")
    return [job]

@MySpider.HtmlProcessors.register()
def handleStartpage(response, job, spider):
    if job._ADD['sitetype'] == "start":
        root = response.root()
        base_href = root.xpath("//base/@href")
        if base_href:
            base_href = base_href[0]
            if not base_href.startswith('http'):
                base_href = "https:" + base_href
        else:
            base_href=None
        root.make_links_absolute(base_href)

        for link in root.xpath("//a[.//h5[@data-mh='federation-link-headline']]"):
            l = link.get("href")
            if not l.startswith("https"):
                l = "https:" + l
            spider.addToQueue(CrawlJob(l,sitetype="verband", fed=link.text_content().strip()))
            logger.warning(l)
    return True


@MySpider.HtmlProcessors.register()
def handleStartpage(response, job, spider):
    try:
        if job._ADD['sitetype'] == "verband":
            fed = job._ADD['fed']
            root = response.root()
            base_href = root.xpath("//base/@href")
            if base_href:
                base_href = base_href[0]
                if not base_href.startswith('http'):
                    base_href = "https:" + base_href
            else:
                base_href=None
            root.make_links_absolute(base_href)
            for link in root.xpath("//div[@data-match-height='championships']//a[@href]"):
                l = link.get("href")
                spider.addToQueue(CrawlJob(l,sitetype="region", regionname=link.text_content().strip()))
                logger.warning(l)
            try:
                link = root.xpath("//div[@class='row m-l text-center']//a[@class='btn btn-primary']")[0]
                spider.addToQueue(CrawlJob(link.get("href"),sitetype="region", regionname=link.text_content().strip(), fed=fed))
                logger.warning(link.get("href"))
            except:
                pass
        return True
    except:
        return False
@MySpider.HtmlProcessors.register()
def handleStartpage(response, job, spider):
    try:
        if job._ADD['sitetype'] == "region":
            fed = job._ADD['fed']
            regionname = job._ADD['regionname']
            root = response.root()
            base_href = root.xpath("//base/@href")
            if base_href:
                base_href = base_href[0]
                if not base_href.startswith('http'):
                        base_href = "https:" + base_href
            else:
                base_href=None
            root.make_links_absolute(base_href)
            for group in root.xpath("//div[@class='well contest-group']"):
                group_class=group.xpath(".//h5/text()")[0]
                for link in group.xpath(".//a[@href]"):
                    spider.addToQueue(CrawlJob(link.get("href"),sitetype="group", groupname=link.text_content().strip(),group_class=group_class, fed=fed, regionname=regionname ))
                    logger.warning(link.get("href"))

        return True
    except:
        return False

@MySpider.HtmlProcessors.register()
def handleStartpage(response, job, spider):
    try:
        if job._ADD['sitetype'] == "group":
            fed = job._ADD['fed']
            regionname = job._ADD['regionname']
            group_class = job._ADD['group_class']
            group_name = job._ADD['groupname']
            root = response.root()

            path = "output/{}/{}/{}/".format(fed, response, group_class)
            if not os.path.isdir(path):
                os.makedirs(path)
            with open(path + "{}.html".format(group_name), "w+") as f:
                f.write(response.content())

        return True
    except:
        return False
heaader=[
    "Accept: text/html",
    "Accept-Encoding: gzip, deflate, br",
    "Accept-Language: de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Upgrade-Insecure-Requests: 1"
]
MySpider.run(10,USERAGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",HTTPHEADER=heaader,FOLLOWLOCATION=True, SSL_VERIFYHOST=False, SSL_VERIFYPEER=False,  ENCODING="gzip, deflate")