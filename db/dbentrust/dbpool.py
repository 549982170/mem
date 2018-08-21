# coding:utf8
"""
Created on 2017-2-8

@author: yizhiwu
数据库连接池
"""
import logging
import MySQLdb
from DBUtils.PooledDB import PooledDB
from MySQLdb.cursors import DictCursor

# 创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# 创建一个handler，用于写入日志文件
fh = logging.FileHandler('log/db.log')
fh.setLevel(logging.DEBUG)
# 再创建一个handler，用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# 定义handler的输出格式
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# 给logger添加handler
logger.addHandler(fh)
logger.addHandler(ch)

logger = logging.getLogger()
DBCS = {'mysql': MySQLdb, }


class DBPool(object):

    def initPool(self, dbcfg):
        self.config = dbcfg
        creator = DBCS.get(dbcfg.get('engine', 'mysql'), MySQLdb)
        self.pool = PooledDB(creator, 5, **dbcfg)  # 2为连接池里的最少连接数
        logger.info('连接数据库成功 ...')

    def connection(self):
        return self.pool.connection()

    def execSql(self, sqlstr):
        '''执行数据库的写操作(插入,修改,删除),自动commit
        @param sqlstr: str 需要执行的sql语句
        '''
        try:
            conn = self.connection()
            cursor = conn.cursor()
            count = cursor.execute(sqlstr)
            conn.commit()
            cursor.close()
            conn.close()
            if count > 0:
                return True
            return False
        except Exception, err:
            logger.error('sqlerror: %s' % sqlstr)
            cursor.close()
            conn.close()
            raise err

    def execSql2(self, sqlstr, params=None):
        '''执行数据库的写操作(插入,修改,删除),自动commit
        @param params: SQL查询参数,tuple形式
        @param sqlstr: str 需要执行的sql语句
        '''
        try:
            conn = self.connection()
            cursor = conn.cursor()
            count = cursor.execute(sqlstr, params)
            conn.commit()
            cursor.close()
            conn.close()
            if count > 0:
                return True
            return False
        except Exception, err:
            logger.error('sqlerror: %s' % sqlstr)
            cursor.close()
            conn.close()
            raise err

    def execute(self, sqlstrList):
        '''批量处理sql语句并且支持事务回滚,自动commit
        @param sqlstrList: list(str) 需要执行的sql语句list
        '''
        try:
            conn = self.connection()
            cursor = conn.cursor()
            #           conn.autocommit(False)
            for sqlstr in sqlstrList:
                count = cursor.execute(sqlstr)
            conn.commit()
            #           conn.autocommit(True)
            cursor.close()
            conn.close()
            if count > 0:
                return True
            return False
        except Exception, err:
            conn.rollback()
            cursor.close()
            conn.close()
            logger.error('sqlerror: %s ' % sqlstrList)
            raise err

    def executemany(self, sqlstr, paramsList=None):
        '''执行单条sql语句,但是重复执行参数列表里的参数,返回值为受影响的行数性能比重复执行sql语句高
        @param sql: sql,参数统一%s占位，不管数据类型是数字还是字符
        @param paramsList: 多个元组的数组
        '''
        try:
            conn = self.connection()
            cursor = conn.cursor()
            #           conn.autocommit(False)
            count = cursor.executemany(sqlstr, paramsList)
            conn.commit()
            #           conn.autocommit(True)
            if count > 0:
                return True
            return False
        except Exception, err:
            conn.rollback()
            logger.error('sqlerror: %s' % sqlstr)
            cursor.close()
            conn.close()
            raise err

    def querySql(self, sqlstr, dictcursor=False):
        '''执行查询并返回所有结果 fetchall'''
        try:
            conn = self.connection()
            if dictcursor:
                cursor = conn.cursor(cursorclass=DictCursor)
            else:
                cursor = conn.cursor()
            cursor.execute(sqlstr)
            result = cursor.fetchall()
            conn.commit()
            cursor.close()
            conn.close()
            return result
        except Exception, err:
            logger.error(sqlstr)
            cursor.close()
            conn.close()
            raise err

    def querySql2(self, sqlstr, params=None, dictcursor=False):
        '''执行查询并返回所有结果 fetchall
        @param sql: sql,参数统一%s占位，不管数据类型是数字还是字符
        @param params: SQL查询参数,tuple形式
        @param dictcursor: 是否以字典形式返回
        '''
        try:
            conn = self.connection()
            if dictcursor:
                cursor = conn.cursor(cursorclass=DictCursor)
            else:
                cursor = conn.cursor()
            cursor.execute(sqlstr, params)
            result = cursor.fetchall()
            conn.commit()
            cursor.close()
            conn.close()
            return result
        except Exception, err:
            logger.exception(err)
            cursor.close()
            conn.close()
            raise err

    def fetchone(self, sqlstr, dictcursor=False):
        '''获取所有游标对象中的一条数据'''
        try:
            conn = self.connection()
            if dictcursor:
                cursor = conn.cursor(cursorclass=DictCursor)
            else:
                cursor = conn.cursor()
            cursor.execute(sqlstr)
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            return result
        except Exception, err:
            logger.error(sqlstr)
            cursor.close()
            conn.close()
            raise err


dbpool = DBPool()
