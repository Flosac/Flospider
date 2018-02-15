import gzip
import re
import lxml.html
from io import BytesIO
from urllib.parse import urlparse, parse_qs

class WebResponse():

    def __init__(self, url):
        self.writer = BytesIO()
        self.statuscode = None
        self.requestUrl = url
        self.urlGetParams = None
        self.customFlag = 0
        self.httpheader = {}
        self.charset = 'utf8'

    def write(self, _bytes):
        self.writer.write(_bytes)

    def writeheader(self, str):
        str = str.decode('ascii')
        if ":" in str:
            key, value = str.split(":", 1)
            self.httpheader[key.strip()] = value.strip()
            if key =="Content-Type" and "charset" in value.lower():
                m = re.search('charset=(.*)$', value.lower())
                self.charset=m.group(1).strip()

        return len(str)
    def content(self):
        content  = self.writer.getvalue()
        if "Content-Encoding" in self.httpheader and self.httpheader["Content-Encoding"] == "gzip":
            try:
                content = gzip.decompress(content)
            except:
                pass
        try:
            return content.decode(self.charset)
        except Exception as e:
            pass
        return content

    def root(self):
        return lxml.html.fromstring(self.content())

    def urlGetParameter(self):
        if self.urlGetParams is None:
            if self.requestUrl is not None:
                self.urlGetParams = parse_qs(urlparse(self.requestUrl).query)
        return self.urlGetParams

    def __repr__(self):
        return "Webresponse[CODE:{}|URL:{}]".format(self.statuscode, self.requestUrl)