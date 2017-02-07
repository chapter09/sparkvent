# Generate URL based on the config file

class UrlGen(object):
    def __init__(self):
        pass


def generate_url(host_addr, rest_api, options, port=18080):
    if not rest_api:
        return host_addr

    if not host_addr:
        host_addr = '142.150.208.177'

    rest_api_host = '/api/v1'
    return 'http://' + host_addr + ':' + str(port) + rest_api_host + rest_api + get_option_string(options)


def get_option_string(option):
    """
    Generate HTTP request option string from dictionaries
    :param option: a dictionary of options
    :type option: dict
    :return:a string matches the http option
    :rtype:str
    >>> option = {'status': 'suspended', 'method': 'get'}
    >>> get_option_string(option)
    'status=suspended&method=get'
    """
    return '&'.join(key + "=" + option[key] for key in option.keys())