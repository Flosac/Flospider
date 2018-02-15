# coding=utf8

import logging
import pycurl
from urllib.parse import urlencode
import _socket
from core.WebResponse import WebResponse

class WebRequester(object):

    def __init__(self, reuseConnection=True, proxyLoader=None):
        self.reuseConnection = reuseConnection
        self.connection = None
        self.proxyLoader = proxyLoader
        self.proxy = None
        self.lastStatuscode = None
        self.requestCounter = 0
        self.statuscodeHistogramm = {}
        self.proxyHistogramm = {}
        self.logger = logging.getLogger('requests')
        self.commonLogger = logging.getLogger()

    def setupConnection(self, **kwargs):
        if self.connection is None:
            connection = pycurl.Curl()

            if self.proxyLoader is not None:
                self.proxy = self.proxyLoader(self)
                if self.proxy is not None:
                    connection.setopt(pycurl.PROXY, self.proxy.address)
                    if self.proxy.hasPassword():
                        connection.setopt(pycurl.PROXYUSERPWD, self.proxy.getUserPwd())

            for option in kwargs:
                try:
                    if hasattr(pycurl, option.upper()):
                        #self.commonLogger.warning("SET {} TO {}".format(option.upper(), kwargs[option]))
                        connection.setopt(getattr(pycurl, option.upper()), kwargs[option])
                    else:
                        self.commonLogger.warning('pycurl has no Option: {}'.format(option.upper()))
                except Exception as e:
                    self.commonLogger.exception(e)
                    self.commonLogger.debug("Option was: {}".format(option))
            self.connection=connection

    def closeConnection(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None


    def get(self, url, params=None, **kwargs):
        return self.request('GET', url, params, **kwargs)

    def post(self, url, data=None, **kwargs):
        return self.request('POST', url, data, **kwargs)

    def selfAddStatuscodeToHistogramm(self, statuscode):
        if statuscode in self.statuscodeHistogramm:
            self.statuscodeHistogramm[statuscode] += 1
        else:
            self.statuscodeHistogramm[statuscode] = 1

    def selfAddProxyToHistogramm(self, proxy):
        if proxy.address in self.statuscodeHistogramm:
            self.proxyHistogramm[proxy.address] += 1
        else:
            self.proxyHistogramm[proxy.address] = 1

    def request(self,requestType, url, params=None, **kwargs):
        response = WebResponse(url)
        if not self.reuseConnection:
            self.closeConnection()
        self.setupConnection(**kwargs)
        self.connection.setopt(pycurl.WRITEFUNCTION, response.write)
        self.connection.setopt(pycurl.HEADERFUNCTION, response.writeheader)
        if requestType == "POST":
            self.connection.setopt(pycurl.POST, True)
            if params is not None:
                self.connection.setopt(pycurl.POSTFIELDS, params)
        elif requestType == "GET":
            if params is not None:
                url += "?" + urlencode(params)
        self.connection.setopt(pycurl.URL, url)
        try:
            self.connection.perform()
            statuscode = self.connection.getinfo(pycurl.HTTP_CODE)
            requesttime = self.connection.getinfo(pycurl.TOTAL_TIME)
            downloadsize = self.connection.getinfo(pycurl.SIZE_DOWNLOAD)
            resolved_url = self.connection.getinfo(pycurl.EFFECTIVE_URL)
            self.commonLogger.warning("{} {} {}".format(statuscode, downloadsize, resolved_url))
            #self.logger.info('test', extra={"hostname":_socket.gethostname(), "proxy":self.proxy, "statuscode":statuscode, "requesttime":requesttime, "url":url, "downloadsize":downloadsize, "resolved_url":resolved_url})

        except pycurl.error as e:
            code, msg = e.args
            statuscode = code
            self.commonLogger.warning("{} {} {}".format(statuscode, "", ""))
            #self.logger.info(msg, extra={"hostname":_socket.gethostname(), "proxy":self.proxy if self.proxy is not None else "None", "statuscode":statuscode, "requesttime":0, "url":url, "downloadsize":0,"resolved_url":None})
        response.statuscode = statuscode
        self.lastStatuscode = statuscode
        self.requestCounter+=1
        return response

if __name__ == "__main__":
    resp = WebRequester().get('http://like-medizintechnik.de/Impressum')
    root = resp.root()
    print(root.xpath("//*[contains(text(),'eschäftsführer')]")[0].text_content())