# coding:utf8
from dbpool import dbpool
from MySQLdb.cursors import DictCursor
from numbers import Number
import logging

logger = logging.getLogger('dblog')


def forEachPlusInsertProps(tablename, props):
    assert type(props) == dict
    #     pkeysstr = str(tuple(props.keys())).replace('\'', '`')
    #     pvaluesstr = ["%s," % val if isinstance(val, Number) else
    #                   "'%s'," % str(val).replace("'", "\\'") for val in props.values()]
    #     pvaluesstr = ''.join(pvaluesstr)[:-1]

    # ---fix bug with None insert by yizhiwu-------
    pkeyslist = []
    pvalueslist = []
    for key, val in props.items():
        if val is None:
            continue
        if isinstance(val, Number):
            pvalueslist.append("%s," % val)
        else:
            pvalueslist.append("'%s'," % str(val).replace("'", "\\'"))
        pkeyslist.append(key)
    pkeysstr = str(tuple(pkeyslist)).replace('\'', '`') if pkeyslist.__len__() > 1 else '`' + pkeyslist[0] + '`'
    pvaluesstr = ''.join(pvalueslist)[:-1]
    # -------------end-----------------------------
    sqlstr = """INSERT INTO `%s` %s values (%s);""" % (tablename, pkeysstr, pvaluesstr)
    return sqlstr


def FormatCondition(props):
    """生成查询条件字符串
    """
    items = props.items()
    itemstrlist = []
    for _item in items:
        if isinstance(_item[1], Number):
            sqlstr = " `%s`=%s AND" % _item
        else:
            sqlstr = " `%s`='%s' AND " % (_item[0], str(_item[1]).replace("'", "\\'"))
        itemstrlist.append(sqlstr)
    sqlstr = ''.join(itemstrlist)
    return sqlstr[:-4]


def FormatUpdateStr(props):
    """生成更新语句
    """
    items = props.items()
    itemstrlist = []
    for _item in items:
        if isinstance(_item[1], Number):
            sqlstr = " `%s`=%s," % _item
        else:
            # sqlstr = " `%s`='%s',"%(_item[0],str(_item[1]).replace("'", "\\'")) mod by fg
            if _item[1] is None:
                sqlstr = " `%s`=NULL," % (_item[0])  # NULL
            else:
                sqlstr = " `%s`='%s'," % (_item[0], str(_item[1]).replace("'", "\\'"))
        itemstrlist.append(sqlstr)
    sqlstr = ''.join(itemstrlist)
    return sqlstr[:-1]


def forEachUpdateProps(tablename, props, prere):
    '''遍历所要修改的属性，以生成sql语句'''
    assert type(props) == dict
    pro = FormatUpdateStr(props)
    pre = FormatCondition(prere)
    sqlstr = """UPDATE `%s` SET %s WHERE %s""" % (tablename, pro, pre)
    return sqlstr


def EachQueryProps(props):
    '''遍历字段列表生成sql语句
    '''
    sqlstr = ""
    if props == '*':
        return '*'
    elif type(props) == type([0]):
        for prop in props:
            sqlstr = sqlstr + prop + ','
        sqlstr = sqlstr[:-1]
        return sqlstr
    else:
        raise Exception('props to query must be dict')
        return


def forEachQueryProps(sqlstr, props):
    '''遍历所要查询属性，以生成sql语句'''
    if props == '*':
        sqlstr += ' *'
    elif type(props) == type([0]):
        i = 0
        for prop in props:
            if (i == 0):
                sqlstr += ' ' + prop
            else:
                sqlstr += ', ' + prop
            i += 1
    else:
        raise Exception('props to query must be list')
        return
    return sqlstr


def GetTableIncrValue(tablename):
    """不支持mainlog库
    """
    database = dbpool.config.get('db')
    sql = """SELECT AUTO_INCREMENT FROM information_schema.`TABLES` \
    WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'""" % (database, tablename)
    conn = dbpool.connection()
    cursor = conn.cursor()
    logger.debug(sql)
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if result:
        return result[0]
    return result


def ReadDataFromDB(tablename):
    """
    """
    sql = """select * from %s""" % tablename
    conn = dbpool.connection()
    cursor = conn.cursor(cursorclass=DictCursor)
    logger.debug(sql)
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


def DeleteFromDB(tablename, props):
    '''从数据库中删除
    '''
    prers = FormatCondition(props)
    sql = """DELETE FROM %s WHERE %s """ % (tablename, prers)
    conn = dbpool.connection()
    cursor = conn.cursor()
    count = 0
    try:
        logger.debug(sql)
        count = cursor.execute(sql)
        conn.commit()
    except Exception, e:
        logger.exception(e)
        logger.error(sql)
    cursor.close()
    conn.close()
    return bool(count)


def InsertIntoDB(tablename, data):
    """写入数据库
    """
    sql = forEachPlusInsertProps(tablename, data)
    conn = dbpool.connection()
    cursor = conn.cursor()
    count = 0
    try:
        logger.debug(sql)
        count = cursor.execute(sql)
        conn.commit()
    except Exception, e:
        logger.exception(e)
        logger.error(sql)
    cursor.close()
    conn.close()
    return bool(count)


def UpdateWithDict(tablename, props, prere):
    """更新记录
    """
    sql = forEachUpdateProps(tablename, props, prere)
    conn = dbpool.connection()
    cursor = conn.cursor()
    count = 0
    try:
        logger.debug(sql)
        count = cursor.execute(sql)
        conn.commit()
    except Exception, e:
        logger.exception(e)
        logger.error(sql)
    cursor.close()
    conn.close()
    if (count >= 1):
        return True
    return False


def getAllPkByFkInDB(tablename, pkname, props):
    """根据所有的外键获取主键ID
    """
    props = FormatCondition(props)
    sql = """Select `%s` from `%s` where %s""" % (pkname, tablename, props)
    logger.debug(sql)
    result = dbpool.querySql(sql, False)
    return [key[0] for key in result]


def GetOneRecordInfo(tablename, props):
    '''获取单条数据的信息
    '''
    props = FormatCondition(props)
    sql = """Select * from `%s` where %s""" % (tablename, props)
    #     logger.debug(sql)
    result = dbpool.fetchone(sql, True)
    return result


def GetRecordList(tablename, pkname, pklist):
    """
    """
    pkliststr = ""
    for pkid in pklist:
        pkliststr += "%s," % pkid
    pkliststr = "(%s)" % pkliststr[:-1]
    sql = """SELECT * FROM `%s` WHERE `%s` IN %s""" % (tablename, pkname, pkliststr)
    logger.debug(sql)
    result = dbpool.querySql(sql, True)
    return result


def GetRecordListByProps(tablename, props):
    ''''add by meng.xf'''
    props = FormatCondition(props)
    sql = 'select * from %s where %s' % (tablename, props)
    logger.debug(sql)
    return dbpool.querySql(sql, True)


def GetRecordListByCondition(tablename, conditionStr):
    ''''add by xiaopao'''
    sql = 'select * from %s where %s' % (tablename, conditionStr)
    return dbpool.querySql(sql, True)


def getallkeys(key, mem):
    itemsinfo = mem.get_stats('items')

    itemindex = []
    for items in itemsinfo:
        itemindex += [_key.split(':')[1] for _key in items[1].keys()]
    s = set(itemindex)
    itemss = [mem.get_stats('cachedump %s 0' % i) for i in s]
    allkeys = set([])
    for item in itemss:
        for _item in item:
            nowlist = set([])
            for _key in _item[1].keys():
                try:
                    keysplit = _key.split(':')
                    pk = keysplit[2]
                except:
                    continue
                if _key.startswith(key) and not pk.startswith('_'):
                    nowlist.add(pk)
            allkeys = allkeys.union(nowlist)
    return allkeys
