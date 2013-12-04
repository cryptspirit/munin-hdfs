#!/usr/bin/python2
#       Copyright 2013  Maksim Podlesniy <root at nightbook.info>
#       
#       This program is free software; you can redistribute it and/or modify
#       it under the terms of the GNU General Public License as published by
#       the Free Software Foundation; either version 2 of the License, or
#       (at your option) any later version.
#       
#       This program is distributed in the hope that it will be useful,
#       but WITHOUT ANY WARRANTY; without even the implied warranty of
#       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#       GNU General Public License for more details.
#       
#       You should have received a copy of the GNU General Public License
#       along with this program; if not, write to the Free Software
#       Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#       MA 02110-1301, USA.
'''
Munin plugin from Hadoop HDFS byte used.
Need add in /etc/munin/plugin-conf.d/munin-node:
[hdfs]
user hdfs
env.namenode my.namenode.some.com
hdfs - link name it script in /etc/munin/plugins/
'''

import sys
import os
from dfsadmin import dfsadmin_parser

conf = '''graph_title HDFS usage in byte
graph_vlabel Bytes
graph_category disk

all.label apps
all.draw AREA
all.info All used byte.
'''

def_conf = {'warning': 92, 'critical': 98}

def config_print(dfsadmin):
    graph_args = 'graph_args --base 1024 -l 0 --upper-limit %s' % dfsadmin['NameNode']['Present Capacity']
    s1 = ''
    for i in dfsadmin['DataNode']:
        s1 += '%s.label %s\n' % (i['Hostname'], i['Hostname'])
        s1 += '%s.draw AREA\n' % i['Hostname']
        s1 += '%s.info %s used byte\n' % (i['Hostname'], i['Hostname'])
    print '%s\n%s\n%s' % (graph_args, conf, s1)

def value_print(dfsadmin):
    s1 = 'all.value %s\n' % (dfsadmin['NameNode']['Used'])
    for i in dfsadmin['DataNode']:
        s1 += '%s.value %s\n' % (i['Hostname'], i['Used'])
    print s1
    
if __name__ == '__main__':
    dfsadmin = dfsadmin_parser()
    if len(sys.argv) > 1 and sys.argv[1] == 'config':
        config_print(dfsadmin)
    else:
        value_print(dfsadmin)
