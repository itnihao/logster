#!/usr/bin/python

import socket
import simplejson
import struct

from logster.logster_helper import SubmitException

class ZbxSend(object):
    '''Send metric to zabbxi server. 
    see zabbix protocol https://www.zabbix.com/documentation/1.8/protocols/agent.'''

    def __init__(self, server, port=10051):
        '''Init ZbxSend'''

        self.server = server
        self.port = port
        self.send_data = ''
        self.zbx_data = { 
            u'request': u'sender data', 
            u'data':[]
        }

    def add(self, host, key, value, clock = None):
        '''Add metric data.'''

        self.add_data = {
            u'host': host, 
            u'key': key, 
            u'value': value
        }

        if clock != None:
            self.zbx_data[u'clock'] = clock
        self.zbx_data['data'].append(self.add_data)

    def reset(self):
        '''Reset metric data'''

        self.zbx_data['data'] = []
        return self.zbx_data

    def send(self):
        '''Send metric data to zabbix server'''

        # set default socket timeout 2s
        socket.setdefaulttimeout(2)
        # package metrics data
        zbx_json = simplejson.dumps(self.zbx_data, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
        self.send_data = struct.pack('<4sBq' + str(len(zbx_json)) + 's', 'ZBXD', 1, len(zbx_json), zbx_json)
        try:
            zabbix = socket.socket()
            zabbix.connect((self.server, self.port))
            zabbix.sendall(self.send_data)
            # get response header
            resp_header = _recv(zabbix, 13)

            # check response header
            if not resp_header.startswith('ZBXD\1') or len(resp_header) != 13:
                raise SubmitException, "Zabbix response wrong!"

            resp_body_len = struct.unpack('<q', resp_header[5:])[0]
            resp_body = _recv(zabbix, resp_body_len)
            zabbix.close()

            resp = simplejson.loads(resp_body)
            # check response info
            if resp.get('response') != 'success':
                raise SubmitException, "Got error from Zabbix: %s" % resp 

            return resp.get('info')
        except Exception, e:
            raise SubmitException, "%s" % e            

def _recv(sock, count):
    buf = ''
    while len(buf)<count:
        chunk = sock.recv(count-len(buf))
        if not chunk:
            return buf
        buf += chunk
    return buf