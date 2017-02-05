# Multithread HTTP requests

from url_gen import generate_url
import urllib2

def get_json_from_address(address):
    req = urllib2.Request(address)
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError as e:
        print e.reason
        return ''

    return response.read()

