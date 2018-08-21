# coding:utf8

'''
redis client
@author: debuger
'''
import logging
import random
import redis


logger = logging.getLogger()

rdsclient = None

REDIS_KEYZSET="dzset"

def initRedis(host,port,password,db):
    global rdsclient
    if not rdsclient:        
        pool = redis.ConnectionPool(host=host,port=port,password=password,db=db)
        rdsclient = redis.Redis(connection_pool=pool)
        testKey="testKey%s" % random.randint(1,100)
        rdsclient.set(testKey, "test redis ok")
        logger.info(rdsclient.get(testKey))
        rdsclient.delete(testKey)
