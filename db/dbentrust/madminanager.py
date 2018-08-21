#coding:utf8
'''
Created on 2013-5-22

@author: lan (www.9miao.com)
'''
from singleton import Singleton
import logging
logger = logging.getLogger()

class MAdminManager:
    """一个单例管理器作为所有madmin的管理者"""

    __metaclass__ = Singleton
    
    def __init__(self):
        """初始化所有管理的的madmin的集合，放在self.admins中
        """
        self.admins = {}
        
    def registe(self,admin):
        """
        """
        self.admins[admin._name] = admin
        
    def dropAdmin(self,adminname):
        """
        """
        if self.admins.has_key(adminname):
            del self.admins[adminname]
    
    def getAdmin(self,adminname):
        """
        """
        return self.admins.get(adminname)
    
    def checkAdmins(self):
        """遍历所有的madmin，与数据库进行同步
        """
        for admin in self.admins.values():
            admin.checkAll()
    
    
    
        