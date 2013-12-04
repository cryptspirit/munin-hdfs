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
import re
import subprocess
from os import getenv

def get_date_f():
    f = open('dfsadmin')
    p = f.readlines()
    f.close()
    return p

def get_date():
    if not getenv('namenode'):
        print 'Need \'namenode\' env'
        exit(1)
    p = subprocess.Popen('hdfs dfsadmin  -fs hdfs://%s -report' % getenv('namenode'), shell = True, stdout = subprocess.PIPE).stdout.readlines()
    return p

def present_capacity(data):
    '''
    Present Capacity
    '''
    r = re.findall(r'Present Capacity:.*\n', data)[0]
    return re.findall(r'\d+ \(', r)[0][:-2]

def DFS_Used():
    '''
    DFS Used gen
    '''
    pass

def DFS_Used_pars(data):
    '''
    Parsing DFS Used
    '''
    rp = re.findall(r'DFS Used%:.*\n', data)[0]
    r = re.findall(r'DFS Used:.*\n', data)[0]
    return re.findall(r'\d*\.?\d+', rp)[0], re.findall(r'\d+ \(', r)[0][:-2]

def past_basic_dn_info(dfs_info):
    '''
    Fill basic info about DataNode
    '''
    for i in xrange(len(dfs_info['DataNode'])):
        data = dfs_info['DataNode'][i]['Raw']
        dfs_info['DataNode'][i]['Hostname'] = re.findall(r'Hostname: \S+', data)[0][10:].replace('.', '-')
        dfs_info['DataNode'][i]['Used P'], dfs_info['DataNode'][i]['Used'] = DFS_Used_pars(data)
    return dfs_info
    

def dfsadmin_parser():
    '''
    Parser data from 'hadoop dfsadmin -report' command
    '''
    dfsadmin_data = get_date()
    #dfsadmin_data = get_date_f()
    rz = {
        'Datanodes available row': 0,
        'Hostname row': [],
        'Last contact row': [],
        }
    dfs_info = {}
    for i in xrange(len(dfsadmin_data)):
        if dfsadmin_data[i][:5] == '-'*5:
            rz['Datanodes available row'] = i
        elif dfsadmin_data[i][:6] == 'Name: ':
            rz['Hostname row'].append(i)
        elif dfsadmin_data[i][:14] == 'Last contact: ':
            rz['Last contact row'].append(i)
    dfs_info['NameNode'] = {}
    dfs_info['NameNode']['Raw'] = '\n'.join(dfsadmin_data[:rz['Datanodes available row']])
    dfs_info['NameNode']['Present Capacity'] = present_capacity(dfs_info['NameNode']['Raw'])
    dfs_info['NameNode']['Used P'], dfs_info['NameNode']['Used'] = DFS_Used_pars(dfs_info['NameNode']['Raw'])
    dfs_info['DataNode'] = []
    for i in xrange(len(rz['Hostname row'])):
        dfs_info['DataNode'].append({'Raw': '\n'.join(dfsadmin_data[rz['Hostname row'][i]:rz['Last contact row'][i] + 1])})
    return past_basic_dn_info(dfs_info)
