import json
import logging
import random
import re
import lxml.html
from config.config import BASEPYCURLCONFIG
from core import MySpider
from core.Spider import CrawlJob

logger = logging.getLogger("logger")
fh = logging.FileHandler("clubs_details.jsonl", 'a+')
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
    clubids = set()
    counter = 0
    with open("clubs.jsonl", 'r') as f:
        for line in f.readlines():
            data = json.loads(line.strip())
            if data['clubid'] in clubids:
                continue
            clubids.add(data['clubid'])
            counter += 1
            host = random.choice(['wttv', 'dttb', 'rttv', 'ttvn'])
            url = 'http://{}.click-tt.de/cgi-bin/WebObjects/nuLigaTTDE.woa/wa/clubInfoDisplay?club={}'.format(host, data['clubid'])
            jobs.append(CrawlJob(url, club=data))

    print('{} Todos'.format(counter))
    return jobs


def decodeEmail(line):
    m = re.match("encodeEmail\('(.*)', '(.*)', '(.*)', '(.*)'\)", line)
    if m.group(4) == "":
        return m.group(2) +"@" + m.group(3) +"." + m.group(1)
    else:
        return m.group(2) + "." + m.group(4) + "@" + m.group(3) + "." + m.group(1)


@MySpider.HtmlProcessors.register()
def parse(r, j, spider):
    global logger
    root = r.root()
    start = root.xpath('//h2[text()="Kontaktadresse"]')
    club = j._ADD['club']
    if len(start) == 0:
        logger.warning(json.dumps(club))
        return True
    start = start[0]
    container = start.getnext()
    for href in container.xpath('.//a//@href'):
        if not "homepage" in club: club['homepage'] = []
        club['homepage'].append(href)
    data=container.text_content()
    lines = [line.strip() for line in data.splitlines() if line.strip() != ""]

    if(len(lines)<2):
        logger.warning(json.dumps(club))
        return True
    contact ={
        "name": lines[0],
        "address": lines[1]
    }
    for line in lines[2:]:
        if line.startswith('Tel ') or line.startswith('Mobile '):
            contact['tel'] = line
        elif line.startswith('encodeEmail'):
            contact['email'] = decodeEmail(line)



    club['contact'] = contact
    logger.warning(json.dumps(club))
    return True
MySpider.run(10,jobsPerWorker=10, **BASEPYCURLCONFIG)