# coding:utf-8
# !/user/bin/python
import logging
import traceback
from threading import Timer

logger = logging.getLogger()  # 数据库操作日志,logging是多线程安全的
rankTimer = None


def timerGetData(Time=-1):
    """定时扫描mencache到mysql"""
    try:
        from dbentrust.rdsclient import rdsclient, REDIS_KEYZSET
        from db.memmode import MAdminManager
        logger.info('SyncDataToDB start...')
        # keyName格式： tableName:keyidvalue 比如：tb_character:1000
        key_name_list = rdsclient.zrange(REDIS_KEYZSET, 0, 10000, withscores=True)
        for item in key_name_list:
            keyName = item[0]
            score = item[1]

            tmpArray = keyName.split(":")
            tableName = tmpArray[0]
            keyIdValue = tmpArray[1]
            logger.debug("keyName=%s, score=%s, tableName=%s, pkID=%s" % (keyName, score, tableName, keyIdValue))

            adminObj = MAdminManager().getAdmin(tableName)
            if not adminObj:
                logger.error("SyncDataToDB adminObj=%s not found" % tableName)
                rdsclient.zrem(REDIS_KEYZSET, keyName)
                continue

            tarObj = adminObj.getObjIncludeDel(keyIdValue)  # 包含缓存中标识了DEL状态的
            if tarObj:
                try:
                    tarObj.syncDB()
                except Exception as ex:
                    logger.error("SyncData error,tableName=%s, pkID=%s, keyName=%s, data=%s,state=%d" \
                                 % (tableName, keyIdValue, keyName, tarObj.get('data'), tarObj.get('_state')))
                    logger.error(traceback.format_exc())
                    logger.error(ex)
                    rdsclient.zrem(REDIS_KEYZSET, keyName)
                    continue
            else:
                logger.error("MemObject not Found,tableName=%s, pkID=%s, keyName=%s" % (tableName, keyIdValue, keyName))
                rdsclient.zrem(REDIS_KEYZSET, keyName)
                continue

            # 如果score没有变化，则删除掉此key；如果有变化，不删除它，下一个循环需要继续同步
            if rdsclient.zscore(REDIS_KEYZSET, keyName) == score:
                rdsclient.zrem(REDIS_KEYZSET, keyName)
    except Exception, e:
        logger.exception(e)
    finally:
        global rankTimer
        logger.info('SyncDataToDB end...')
        if Time == -1:  # 循环执行
            rankTimer = Timer(20, timerGetData)  # 60秒
            rankTimer.start()


def stopRankTimer():
    """停止线程"""
    if rankTimer:
        rankTimer.cancel()
