#!/usr/bin/python

import socket
import simplejson
import struct

from logster.logster_helper import SubmitException

class ZbxSend(object):
    '''Send metric to zabbxi server. 
    see zabbix protocol https://www.zabbix.com/documentation/1.8/protocols/agent.'''

    def __init__(self, host, port=10051):
        '''Init ZbxSend'''

        self.host   = host
        self.port   = port
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

        socket.setdefaulttimeout(2)
        # package metrics data
        zbx_json = simplejson.dumps(self.zbx_data, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
        self.send_data = 'ZBXD\1' + struct.pack('<Q', len(zbx_json)) + zbx_json
        try:
            zabbix = socket.socket()
            zabbix.connect((self.host, self.port))
            zabbix.sendall(self.send_data)
            resp = zabbix.recv(1024) 
            if 'success' in resp:
                return True
            else:
                raise SubmitException, "%s %s" %(resp, self.send_data)
        except Exception, e:
            raise SubmitException, "%s" % e            
