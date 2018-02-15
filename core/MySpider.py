from core.Spider import Spider


class QueueInitializer:
    queueIniter = []
    args = {}
    @classmethod
    def register(cls, *args):
        def decorator(fn):
            cls.queueIniter.append(fn)
            cls.args[fn.__name__] = args
            return fn
        return decorator

    @classmethod
    def init(cls):
        jobs = []
        for f in cls.queueIniter:
            args = cls.args[f.__name__]
            jobs.extend(f(*args))
        return jobs


class ProxyHandler:
    proxyHandler = []
    args = {}

    @classmethod
    def register(cls, *args):
        def decorator(fn):
            cls.proxyHandler.append(fn)
            cls.args[fn.__name__] = args
            return fn

        return decorator

    @classmethod
    def handle(cls, requester):
        proxy=None
        for f in cls.proxyHandler:
            args = cls.args[f.__name__]
            proxy = f(requester, *args)
        return proxy


class ErrorHandler:
    errorHandlers = []
    args = {}

    @classmethod
    def register(cls, *args):
        def decorator(fn):
            cls.errorHandlers.append(fn)
            cls.args[fn.__name__] = args
            return fn

        return decorator

    @classmethod
    def handle(cls, exception, requester):
        status = True
        for f in cls.errorHandlers:
            args = cls.args[f.__name__]
            status = (status and f(exception, requester, *args))
        return status


class HtmlProcessors:
    htmlProcessors = []
    args = {}

    @classmethod
    def register(cls, *args):
        def decorator(fn):
            cls.htmlProcessors.append(fn)
            cls.args[fn.__name__] = args
            return fn

        return decorator

    @classmethod
    def handle(cls, response, job, requester):
        for f in cls.htmlProcessors:
            args = cls.args[f.__name__]
            if not f(response, job, requester, *args):
                return False
        return True


class QueueFiller:
    queueFiller = []
    args = {}

    @classmethod
    def register(cls, *args):
        def decorator(fn):
            cls.queueFiller.append(fn)
            cls.args[fn.__name__] = args
            return fn

        return decorator

    @classmethod
    def handle(cls, spider):
        for f in cls.queueFiller:
            args = cls.args[f.__name__]
            f(spider, *args)




def run(numProcesses=None,  crawlDelay=0, jobsPerWorker=None,reuseConnection=True, **baseSettings):

    SPIDER = Spider(numProcesses,crawlDelay=crawlDelay,jobsPerWorker=jobsPerWorker,reuseConnection=reuseConnection, responseHandler=HtmlProcessors.handle,
                    errorHandlers=ErrorHandler.handle, proxyLoader=ProxyHandler.handle, queueFiller=QueueFiller.handle, baseSettings=baseSettings)
    SPIDER.fillQueue(QueueInitializer.init())
    SPIDER.serve()

