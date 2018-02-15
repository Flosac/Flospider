import json
import logging
from queue import PriorityQueue
from time import sleep
from multiprocessing import cpu_count
from multiprocessing.dummy import Process
from core.WebRequester import WebRequester


class CrawlJob:
    def __lt__(self, other):
        return self.priority < other.priority

    @staticmethod
    def insertMany(jobs, tablename, db):

        j = jobs.pop()
        c = db.cursor()
        c.execute(j._createTable(tablename))

        inserts = []
        insertquery, values = j._insert(tablename)
        inserts.append(values)

        counter = 0
        for j in jobs:
            counter += 1
            q, values = j._insert(tablename)
            inserts.append(values)
        print("Values build")
        c.executemany(insertquery, inserts)
        print("Executed")

    def __init__(self, url, requestType="GET", data=None, settings=None, **kw):
        if settings is None:
            settings = {}
        self.url = url
        self.requestType = requestType
        self.data = data
        self.settings = settings
        self._ADD = kw
        self.priority = 0

    def getPriority(self):
        return int(self.priority)

    def __repr__(self):
        return "CrawlJob[URL:{url}|Type:{requestTye}|HasData:{hasdata}|Prio:{prio}]".format(url=self.url,
                                                                                            requestTye=self.requestType,
                                                                                            hasdata=self.data is not None,
                                                                                            prio=self.priority)

    def insert(self, tablename, dbhandle, createTable=True):
        c = dbhandle.cursor()
        if createTable:
            c.execute(self._createTable(tablename))
        query, values = self._insert(tablename)
        try:
            c.execute(query, values)
        except Exception as e:
            print(e)
            print(query, len(values))
            raise e

    def _createTable(self, tablename):
        return "CREATE TABLE IF NOT EXISTS {tablename} (" \
               "id INTEGER auto_increment PRIMARY KEY," \
               "url Text," \
               "requestType TEXT," \
               "data TEXT," \
               "settings TEXT," \
               "_ADD TEXT," \
               "priority INT" \
               ")".format(tablename=tablename)

    def _insert(self, tablename):
        values = (self.url, self.requestType, json.dumps(self.data), json.dumps(self.settings), json.dumps(self._ADD),
                  self.priority)
        return "INSERT IGNORE INTO {tablename}(url, requestType, data, settings, _ADD, priority ) VALUES(%s, %s, %s, %s, %s, %s)".format(
            tablename=tablename), values

class Spider:
    """
        :type queue: queue.Queue
        :type manager: multiprocessing.dummy.Manager
        :type pool: multiprocessing.dummy.Pool
    """

    def __init__(self, numProcesses=None, jobsPerWorker=15, crawlDelay=0, reuseConnection=True, responseHandler=None, errorHandlers=None, proxyLoader=None, queueFiller=None,
                 baseSettings=None):
        if errorHandlers is None:
            errorHandlers = []
        if responseHandler is None:
            responseHandler = []
        if baseSettings is None:
            baseSettings = {}
        if numProcesses is None:
            numProcesses = cpu_count()

        self.proxyLoader = proxyLoader
        self.baseSettings = baseSettings
        self.queue = PriorityQueue()
        self.crawlDelay = crawlDelay
        self.numWorker = numProcesses
        self.responseHandler = responseHandler
        self.errorHandler = errorHandlers
        self.logger = logging.getLogger()
        self.jobsPerWorker = jobsPerWorker
        self.reuseConnection = reuseConnection
        self.queueFiller = queueFiller

    def worker(self, jobs):
        requester = WebRequester(reuseConnection=self.reuseConnection, proxyLoader=self.proxyLoader)
        logger = logging.getLogger()
        logger.info("Start Worker")
        for job in jobs:
            try:
                ok = False
                settings = self.baseSettings
                if job.settings is not None:
                    settings.update(job.settings)
                while not ok:
                    response = requester.request(job.requestType, job.url, job.data, **settings)
                    ok = self.handleResponse(response, job)
                    sleep(self.crawlDelay)
            except Exception as e:
                self.logger.exception(e)
                self.handleError(e, job)
        logger.info("Close Worker")

    def handleResponse(self, response, job):
        status = self.responseHandler(response, job, self)
        return status

    def handleError(self, error, job):
        status = self.errorHandler(error, job)
        if not status:
            job.priority -= 1
            self.addToQueue(job)

    def logerror(self, e):
        self.logger.exception(e)

    def serve(self):
        logger = logging.getLogger()
        processes = []

        try:
            while True:
                self.logger.debug('Serve Forever')

                tasks = []
                task = []
                while not self.queue.empty():
                    job = self.queue.get_nowait()
                    task.append(job)
                    if len(task) == self.jobsPerWorker or self.queue.empty():
                        tasks.append(task)
                        task = []

                self.logger.debug('Loaded Tasks')
                while len(tasks)>0:
                    while len(processes) == self.numWorker:
                        newProcesses = []
                        for p in processes:
                            if not p.is_alive():
                                p.join()
                            else:
                                newProcesses.append(p)
                        processes=newProcesses
                        sleep(1)
                    self.logger.debug('{} Free Worker'.format(self.numWorker - len(processes)))

                    for i in range(min(self.numWorker-len(processes), len(tasks))):
                        task = tasks.pop(0)
                        p = Process(target=self.worker, args=(task,))
                        p.start()
                        self.logger.debug('Start Fresh Worker')
                        processes.append(p)

                sleep(5)
        except (Exception, SystemExit, KeyboardInterrupt) as e:
            logger.exception(e)

        finally:
            for p in processes:
                p.terminate()
                p.join()
            logger.fatal('Spider does not serve anymore')

    def fillQueue(self, iteratable):
        for job in iteratable:
            self.queue.put(job)
        return True

    def addToQueue(self, job):
        self.queue.put(job)
        return True
