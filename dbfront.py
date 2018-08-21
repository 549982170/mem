# coding:utf-8
# !/user/bin/python
import json
from db.dbentrust.dbpool import dbpool
from db.syncdata import timerGetData
from db.dbentrust.rdsclient import initRedis
from db.dbentrust.memclient import mclient

configfiles = json.load(open('config/config.json', 'r'))


class Member(object):
    def __init__(self):
        self.db = configfiles['db']
        self.redis = configfiles['redis']
        self.memcached = configfiles['memcached']

    def connection(self):
        dbpool.initPool(self.db)
        initRedis(self.redis['host'], self.redis['port'], self.redis['password'], self.redis['db'])
        mclient.connect(self.memcached['urls'], self.memcached['hostname'])


member = Member()

if __name__ == '__main__':
    member.connection()
    timerGetData()
