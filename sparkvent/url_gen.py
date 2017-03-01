# Generate URL based on the config file


class UrlGen(object):
    BASE_ENDPOINT = '/api/v1/applications'

    def __init__(self):
        pass

    def get_url(self, host, rest_api='', options={}):
        if not host:
            host = '142.150.208.177:18080'

        url = 'http://' + host + self.BASE_ENDPOINT + self.get_rest_endpoint(rest_api) + self.get_option_string(options)
        return url

    def get_rest_endpoint(self, rest_api):
        if rest_api == '':
            rest_endpoint = ''
        elif rest_api[0] == '/':
            rest_endpoint = rest_api
        else:
            rest_endpoint = '/' + rest_api
        return rest_endpoint

    def get_option_string(self, option):
        """
        Generate HTTP request option string from dictionaries
        :param option: a dictionary of options
        :type option: dict
        :return:a string matches the http option
        :rtype:str
        >>> option = {'status': 'suspended', 'method': 'get'}
        >>> get_option_string(option)
        '?status=suspended&method=get'
        """
        if option:
            return '?' + '&'.join(key + "=" + option[key] for key in option.keys())
        return ''
