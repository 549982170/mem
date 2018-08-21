# coding:utf-8
# !/user/bin/python
'''
Created on 2017年8月7日
@author: yizhiwu
'''
import logging
from batchtask.dbentrust.dbpool import dbpool


logger = logging.getLogger()  # 获取dblog的日志配置

TB_SYSCFG = {}
TB_SYSCFG.clear()
queryResult = dbpool.querySql("select * from tb_syscfg", True)
for ca in queryResult:
    TB_SYSCFG[ca['Id']] = ca['content']
    
def get_sequence_val(seq_name):
    """
    获取sequence表中序列的下一个值
    @param seq_name: 表名
    """
    try:
        sql = "select nextval_safe('%s')" % seq_name
        conn = dbpool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        conn.commit()
    except Exception, err:
        conn.rollback()
        logger.error('sqlerror: %s ' % sql)
        raise err
    finally:
        cursor.close()
        conn.close()
    return result[0]


def get_next_id_manage(seq_name):
    """
    获取tb_table_id_manage表中序列的下一个值
    @param seq_name: 表名
    """
    try:
        sql = "select next_id_manage('%s')" % seq_name
        conn = dbpool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        conn.commit()
    except Exception, err:
        conn.rollback()
        logger.error('sqlerror: %s ' % sql)
        raise err
    finally:
        cursor.close()
        conn.close()
    return result[0]