# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.conf import settings
from stem import Signal
from stem.control import Controller
import time

class ProxyMiddleware(object):
    def __init__(self):
        self.controller = Controller.from_port(port = 9151)
        self.controller.authenticate('931005')
        self.codes = set(int(x) for x in settings.getlist('RETRY_HTTP_CODES'))
        self.count = 0
        
    def process_request(self, request, spider):
        request.meta['proxy'] = settings.get('HTTP_PROXY')
    
    def process_response(self, request, response, spider):
        self.count += 1
        if response.status in self.codes or self.count % 100 == 0:
            print 'Banned, changing circuit...'
            self.new_circuit()
            # print 'sleep 20s...'
            # time.sleep(20)
            # print 'Done!'
        
        return response
        
    def new_circuit(self):      
        self.controller.signal(Signal.NEWNYM)
        
class BiggerpocketsSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
