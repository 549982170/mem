# coding:utf8
'''
Created on 2013-7-10
memcached client
@author: lan (www.9miao.com)
'''
import logging

import memcache


logger = logging.getLogger()

class MemConnError(Exception): 
    """
    """
    def __str__(self):
        return "memcache connect error"

class MemClient:
    '''memcached
    '''
    def __init__(self, timeout=0):
        '''
        '''
        self._hostname = ""
        self._urls = []
        self.connection = None
        
    def connect(self, urls, hostname):
        '''memcached connect
        '''
        self._hostname = hostname
        self._urls = urls
        self.connection = memcache.Client(self._urls, debug=0)
        
        if not self.connection.set("__testkey__", 1):
            raise MemConnError()
        
    def produceKey(self, keyname):
        '''
        '''
        if isinstance(keyname, basestring):
            return ''.join([self._hostname, ':', keyname])
        else:
            raise "type error"
        
    def get(self, key):
        '''
        '''
        key = self.produceKey(key)
        return self.connection.get(key)
    
    def gets(self, key):
        ''' with cas 机制，保证数据一至性'''
        key = self.produceKey(key)
        return self.connection.gets(key)
    
    def cas(self, key,val):
        ''' cas锁机制更新数据'''
        key = self.produceKey(key)
        return self.connection.cas(key, val)
    
    def get_multi(self, keys):
        '''
        '''
        keynamelist = [self.produceKey(keyname) for keyname in keys]
        olddict = self.connection.get_multi(keynamelist)
        newdict = dict(zip([keyname.split(':')[-1] for keyname in olddict.keys()],
                              olddict.values()))
        return newdict
        
    def set(self, keyname, value):
        '''
        '''
        key = self.produceKey(keyname)
#         logger.debug('setMem,key=%s,value=%s' % (key,value))
        result = self.connection.set(key, value)
        if not result:  # 如果写入失败
            self.connect(self._urls, self._hostname)  # 重新连接
            return self.connection.set(key, value)
        return result
    
    def set_multi(self, mapping):
        '''
        '''
#         logger.debug('setMem,set_multi %s' % mapping)
        newmapping = dict(zip([self.produceKey(keyname) for keyname in mapping.keys()],
                              mapping.values()))
        # 生成类似 'manman'为 memcache的hostname
        '''dict: {'manman:character2:_lock': 0, 'manman:character2:_name': 'character2', 'manman:character2:
         level': 1L, 'manman:character2:id': 2L, 'manman:character2:nickname': u'fage407'}
         '''
        result = self.connection.set_multi(newmapping)
#         logger.debug('set_multi %s' % mapping)
        if result:  # 如果写入失败，重试一次
            self.connect(self._urls, self._hostname)  # 重新连接
#             logger.debug('set_multi again %s' % mapping)
            return self.connection.set_multi(newmapping)
        
#         logger.debug('set_multi result %s' % result)
        return result
        
    def incr(self, key, delta):
        '''
        '''
        key = self.produceKey(key)
        return self.connection.incr(key, delta)
        
    def delete(self, key):
        '''
        '''
        key = self.produceKey(key)
        return self.connection.delete(key)
    
    def delete_multi(self, keys):
        """
        """
        keys = [self.produceKey(key) for key in keys]
        return self.connection.delete_multi(keys)
        
    def flush_all(self):
        '''
        '''
        self.connection.flush_all()
        
mclient = MemClient()


