###  A logster parser file that can be used to count the number of different
###  messages in an nginx access_log
###
###  For example:
###  sudo ./logster --dry-run --output=zabbix NginxLog /var/log/nginx/access_log
###

import threading
import re

from logster.logster_helper import MetricObject, LogsterParser
from logster.logster_helper import LogsterParsingException


class NginxLog(LogsterParser):

    def __init__(self, option_string=None):
        '''This function should initialize any data structures or variables
        needed for the internal state of the line parser.'''

        # submitter thread will use period
        self.period = 30
        self.reset_state()
        self.lock = threading.RLock()
        # log_format  main  '$remote_addr - $remote_user [$time_local] $request_time $connection '
        #               '"$request" $status $body_bytes_sent "$http_referer" '
        #                '"$http_user_agent" "$http_cookie" $host $upstream_addr $upstream_response_time';
        self.regex = re.compile(
            r'(?P<remote_addr>[.0-9]+) - '      # remote_addr
            r'[_a-zA-Z-]+ '                     # remote_user
            r'\[[^]]+\] '                       # time_locat
            r'(?P<request_time>[.0-9]+) '       # request_time
            r'\d+ '                             # connection
            r'"[^"]+" '                         # request
            r'(?P<status>[0-9]{3}) '            # status
            r'(?P<body_size>\d+) '              # body_bytes_size
            r'"(?P<http_referer>[^"]*)" '       # http_referer
            r'"(?P<user_agent>[^"]*)" '         # user_agent
            r'"[^"]*" '                         # http_cookie
            r'[.0-9a-zA-Z-]+ '                  # http_host
            # upstream_addr @see http://nginx.org/en/docs/http/ngx_http_upstream_module.html#variables
            r'(?P<upstream_addr>[ _.,:0-9a-zA-Z-]+) '  # upstream_addr
            r'(?P<upstream_time>[.0-9-]+)'      # upstream_time
        )

    def parse_line(self, line):
        '''This function should digest the contents of one line at a time,
        updating the internal state variables.'''

        self.lock.acquire()
        self.stat['nginx_request'] += 1
        try:
            match = self.regex.match(line)

            if match:
                linebits = match.groupdict()

                # http status
                if int(linebits['status']) < 200:
                    self.stat['1xx'] += 1
                elif int(linebits['status']) < 300:
                    self.stat['2xx'] += 1
                elif int(linebits['status']) < 400:
                    self.stat['3xx'] += 1
                elif int(linebits['status']) < 500:
                    self.stat['4xx'] += 1
                else:
                    self.stat['5xx'] += 1

                if int(linebits['status']) == 429:
                    self.stat['429'] += 1
                if int(linebits['status']) == 499:
                    self.stat['499'] += 1
                if int(linebits['status']) == 500:
                    self.stat['500'] += 1
                if int(linebits['status']) == 502:
                    self.stat['502'] += 1
                if int(linebits['status']) == 503:
                    self.stat['503'] += 1

                # nginx time
                self.stat['nginx_time'] += float(linebits['request_time'])
                self.stat['nginx_body_size'] += float(linebits['body_size'])
                if linebits['upstream_time'] != '-':
                    self.stat['upstream_request'] += 1
                    self.stat['upstream_time'] += float(linebits['upstream_time'])
                self.stat['upstream_retry'] += len(linebits['upstream_addr'].split()) - 1
            else:
                raise LogsterParsingException("regmatch failed to match %s" % line)
        except Exception, e:
            self.lock.release()
            raise LogsterParsingException("regmatch or conggtents failed with %s" % e)

        self.lock.release()

    def reset_state(self):
        '''Reset stat state'''

        self.stat = {
            '1xx': 0,
            '2xx': 0,
            '3xx': 0,
            '4xx': 0,
            '5xx': 0,
            '429': 0,
            '499': 0,
            '500': 0,
            '502': 0,
            '503': 0,
            '504': 0,
            'nginx_request': 0,
            'nginx_time': 0,
            'nginx_body_size': 0,
            'upstream_retry': 0,
            'upstream_request': 0,
            'upstream_time': 0
        }

    def get_state(self):
        '''This function should acquire a lock, call deep copy, get the
        current time if necessary, call reset_state, then do its
        calculations.  It should return a list of metric objects.'''

        # get the data to work with
        self.lock.acquire()
        mydata = self.stat
        self.reset_state()
        self.lock.release()

        http_1xx = MetricObject('http_1xx', float(mydata['1xx']) / self.period)
        http_2xx = MetricObject('http_2xx', float(mydata['2xx']) / self.period)
        http_3xx = MetricObject('http_3xx', float(mydata['3xx']) / self.period)
        http_4xx = MetricObject('http_4xx', float(mydata['4xx']) / self.period)
        http_5xx = MetricObject('http_5xx', float(mydata['5xx']) / self.period)
        http_429 = MetricObject('http_429', float(mydata['429']) / self.period)
        http_499 = MetricObject('http_499', float(mydata['499']) / self.period)
        http_500 = MetricObject('http_500', float(mydata['500']) / self.period)
        http_502 = MetricObject('http_502', float(mydata['502']) / self.period)
        http_503 = MetricObject('http_503', float(mydata['503']) / self.period)
        http_504 = MetricObject('http_504', float(mydata['504']) / self.period)
        nginx_request = MetricObject('nginx_request', float(mydata['nginx_request']) / self.period)
        upstream_request = MetricObject('upstream_request', float(mydata['upstream_request']) / self.period)
        upstream_retry = MetricObject('upstream_retry', mydata['upstream_retry'])
        nginx_time = MetricObject('nginx_time', 0)
        nginx_body_size = MetricObject('nginx_body_size', 0)
        upstream_time = MetricObject('upstream_time', 0)
        if int(mydata['nginx_request']) > 0:
            nginx_time = MetricObject('nginx_time', float(mydata['nginx_time']) * 1000 / float(mydata['nginx_request']))
            nginx_body_size = MetricObject('nginx_body_size', float(mydata['nginx_body_size']) / float(mydata['nginx_request']))
        if float(mydata['upstream_request']) > 0:
            upstream_time = MetricObject('upstream_time', float(mydata['upstream_time']) * 1000 / float(mydata['upstream_request']))

        return [
            http_1xx,
            http_2xx,
            http_3xx,
            http_4xx,
            http_5xx,
            http_429,
            http_499,
            http_500,
            http_502,
            http_503,
            http_504,
            nginx_request,
            nginx_time,
            nginx_body_size,
            upstream_retry,
            upstream_request,
            upstream_time
        ]
