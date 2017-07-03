# Multithread HTTP requests
import urllib2
from multiprocessing.pool import ThreadPool


class HttpRequester(object):
    def __init__(self, max_threads = 5):
        self.max_url_allowed = max_threads
        self.thread_pool = ThreadPool(max_threads)
        self.urls = []
        self.json_result = []

    def add_url(self, url):
        if len(self.urls) == self.max_url_allowed:
            return None
        self.urls.append(url)

    def make_request(self):
        results = self.thread_pool.imap_unordered(self.get_json_from_address, self.urls)
        for url, data, error in results:
            if error is None:
                print "Request to %s successful\n".format(url)
                self.json_result.append(data)
            else:
                print "Request to %s failed with error\n".format(url)

        # clear the url array after request, for next run
        self.urls = []
        return self.json_result

    def get_json_from_address(self, address):
        req = urllib2.Request(address)
        try:
            response = urllib2.urlopen(req)
            return address, response.read(), None
        except urllib2.URLError as e:
            print e.reason
            exit(1) 

    def single_request(self, address):
        # data is positioned at index 1
        return self.get_json_from_address(address)[1]
