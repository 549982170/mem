# coding:utf8

import logging
import time

from rdsclient import REDIS_KEYZSET
from memclient import mclient
from memobject import MemObject
from rdsclient import rdsclient
import util

logger = logging.getLogger()

MMODE_STATE_ORI = 0  # 未变更
MMODE_STATE_NEW = 1  # 创建
MMODE_STATE_UPDATE = 2  # 更新
MMODE_STATE_DEL = 3  # 删除

TIMEOUT = 36000


def _insert(args):
    record, pkname, mmname, cls = args
    pk = record[pkname]
    mm = cls(mmname + ':%s' % pk, pkname, data=record)
    mm.insert()
    return pk


class PKValueError(ValueError):
    """
    """

    def __init__(self, data):
        ValueError.__init__(self)
        self.data = data

    def __str__(self):
        return "new record has no 'PK': %s" % (self.data)


class MMode(MemObject):
    """内存数据模型
    """

    def __init__(self, name, pk, data={}):
        """
        """
        MemObject.__init__(self, name, mclient)
        self._state = MMODE_STATE_ORI  # 对象的状态 0未变更  1新建 2更新 3删除
        self._pk = pk
        self.data = data
        self.dszset = REDIS_KEYZSET
        self._time = time.time()

    def update(self, key, values):
        data = self.get_multi(['data', '_state'])
        ntime = time.time()
        data['data'].update({key: values})
        if data.get('_state') == MMODE_STATE_NEW:
            props = {'data': data.get('data'), '_time': ntime}
        else:
            props = {'_state': MMODE_STATE_UPDATE, 'data': data.get('data'), '_time': ntime}

        pkval = data.get('data').get(self._pk)
        rdsclient.zadd(self.dszset, self.produceRedisKey(pkval), ntime)
        return MemObject.update_multi(self, props)

    def update_multi(self, mapping, isSyncNow=False):  # 如果改用redis将不允许非全字段更新，必须全表更新。
        if len(mapping) == 0:
            return
        ntime = time.time()
        data = self.get_multi(['data', '_state'])
        data['data'].update(mapping)
        if data.get('_state') == MMODE_STATE_NEW:
            props = {'data': data.get('data'), '_time': ntime}
            result = MemObject.update_multi(self, props)
            if isSyncNow:
                self.syncDB()
        else:
            props = {'_state': MMODE_STATE_UPDATE, 'data': data.get('data'), '_time': ntime}
            result = MemObject.update_multi(self, props)
            if isSyncNow:
                self.syncDB()

        if not isSyncNow:
            pkval = data.get('data').get(self._pk)
            rdsclient.zadd(self.dszset, self.produceRedisKey(pkval), ntime)
        return result

    def produceRedisKey(self, pkval):
        tablename = self._name.split(':')[0]
        keyName = "%s:%s" % (tablename, pkval)
        return keyName

    def get(self, key):
        ntime = time.time()
        MemObject.update(self, "_time", ntime)
        return MemObject.get(self, key)

    def get_multi(self, keys):
        ntime = time.time()
        MemObject.update(self, "_time", ntime)
        return MemObject.get_multi(self, keys)

    def delete(self, isSyncNow=True):
        '''删除对象
        '''
        if isSyncNow:
            MemObject.update(self, '_state', MMODE_STATE_DEL)  # add by fage
            self.syncDB()
        else:
            MemObject.update(self, '_state', MMODE_STATE_DEL)
            pkval = self.get('data').get(self._pk)
            rdsclient.zadd(self.dszset, self.produceRedisKey(pkval), time.time())

    def mdelete(self):
        """清理对象
        """
        MemObject.mdelete(self)

    def IsEffective(self):
        '''检测对象是否有效
        '''
        if self.get('_state') == MMODE_STATE_DEL:
            return False
        return True

    def syncDB(self):
        """同步到数据库
        """

        state = self.get('_state')
        tablename = self._name.split(':')[0]
        if state == MMODE_STATE_ORI:
            return
        elif state == MMODE_STATE_NEW:
            props = self.get('data')
            pk = self.get('_pk')
            util.InsertIntoDB(tablename, props)
        #             result = util.InsertIntoDB(tablename, props)
        elif state == MMODE_STATE_UPDATE:
            props = self.get('data')
            pk = self.get('_pk')
            prere = {pk: props.get(pk)}
            util.UpdateWithDict(tablename, props, prere)
        #             result = True
        else:
            pk = self.get('_pk')
            props = self.get('data')
            if props:
                prere = {pk: props.get(pk)}
                util.DeleteFromDB(tablename, prere)
                #             result = util.DeleteFromDB(tablename, prere)
                self.mdelete()
            else:
                logging.error('syncDB pk:%s is not data' % str(pk))

        #         logger.debug('syncDB,tbname:%s,%s:%s' %(tablename, pk, props.get(pk)))
        #         if result:  平台缺陷，2014-6-20修改 http://bbs.9miao.com/thread-49154-1-1.html
        if state != MMODE_STATE_DEL:
            MemObject.update(self, '_state', MMODE_STATE_ORI)

    def checkSync(self, timeout=TIMEOUT):
        """检测同步
        """
        ntime = time.time()
        objtime = MemObject.get(self, '_time')
        if ntime - objtime >= timeout and timeout:
            self.mdelete()
        else:
            self.syncDB()


class MFKMode(MemObject):
    """内存数据模型
    """

    def __init__(self, name, pklist=None):
        MemObject.__init__(self, name, mclient)
        if pklist is None:
            self.pklist = []
        else:
            self.pklist = pklist


class MAdmin(MemObject):
    """MMode对象管理，同一个MAdmin管理同一类的MMode，对应的是数据库中的某一种表
    """

    def __init__(self, name, pk, timeout=TIMEOUT, **kw):
        MemObject.__init__(self, name, mclient)
        self._pk = pk
        self._fk = kw.get('fk', '')
        self._incrkey = kw.get('incrkey', '')
        self._incrvalue = kw.get('incrvalue', 0)
        self._timeout = timeout

    def insert(self):
        """将MAdmin配置的信息写入memcached中保存。\n当在其他的进程中实例化相同的配置的MAdmin，可以使得数据同步。
        """
        if self._incrkey and not self.get("_incrvalue"):
            self._incrvalue = util.GetTableIncrValue(self._name)
        MemObject.insert(self)

    def load(self):
        '''读取数据到数据库中
        '''
        mmname = self._name
        recordlist = util.ReadDataFromDB(mmname)
        for record in recordlist:
            pk = record[self._pk]
            mm = MMode(self._name + ':%s' % pk, self._pk, data=record)
            mm.insert()

    @property
    def madmininfo(self):
        """作为一个特性属性。可以获取这个madmin的相关信息
        """
        keys = self.__dict__.keys()
        info = self.get_multi(keys)
        return info

    def getAllPkByFk(self, fk):
        '''根据外键获取主键列表
        '''
        name = '%s_fk:%s' % (self._name, fk)
        fkmm = MFKMode(name)
        pklist = fkmm.get('pklist')
        if pklist is not None:  # 区别第一次加载
            return list(set(pklist))
        props = {self._fk: fk}
        dbkeylist = util.getAllPkByFkInDB(self._name, self._pk, props)
        name = '%s_fk:%s' % (self._name, fk)
        fkmm = MFKMode(name, pklist=dbkeylist)
        fkmm.insert()
        return list(set(dbkeylist))

    #         name = '%s_fk:%s' % (self._name, fk)
    #         fkmm = MFKMode(name)
    #         pklist = fkmm.get('pklist')
    #         props = {self._fk:fk}
    #         dbkeylist = util.getAllPkByFkInDB(self._name, self._pk, props)
    #         if pklist is not None:
    #             pkset = set(pklist)
    #             dbkeyset = set(dbkeylist)
    #             morekey = dbkeyset-pkset
    #             if morekey:
    #                 map(lambda x: pkset.add(x) if self.getObj(x) else "", morekey)  # 修复主键丢失bug by y_zw
    #                 rep_klist = list(pkset)
    #                 fkmm = MFKMode(name)
    #                 pklist = fkmm.get('pklist')
    #                 fkmm.update('pklist', rep_klist)
    #             return list(pkset)
    #         name = '%s_fk:%s' % (self._name, fk)
    #         fkmm = MFKMode(name, pklist=dbkeylist)
    #         fkmm.insert()
    #         return list(set(dbkeylist))

    def addFK(self, fk, pk):
        """根据外键增加主键,防止主键列表丢失"""
        pklist = self.getAllPkByFk(fk)
        if pk not in pklist:
            pklist.append(pk)
            name = '%s_fk:%s' % (self._name, fk)
            fkmm = MFKMode(name)
            fkmm.update('pklist', pklist)

    def getObj(self, pk):
        '''根据主键，可以获得mmode对象的实例.\n
        >>> m = madmin.getObj(1)
        '''
        mm = MMode(self._name + ':%s' % pk, self._pk)
        if not mm.IsEffective():
            return None
        if mm.get('data'):
            return mm
        props = {self._pk: pk}
        record = util.GetOneRecordInfo(self._name, props)
        if not record:
            return None
        mm = MMode(self._name + ':%s' % pk, self._pk, data=record)
        mm.insert()
        if self._fk:
            fk = record[self._fk]
            self.addFK(fk, pk)
        return mm

    def getObjIncludeDel(self, pk):
        '''根据主键，可以获得mmode对象的实例.\n
        >>> m = madmin.getObj(1)
                            包含缓存中标识了DEL状态的
        '''
        mm = MMode(self._name + ':%s' % pk, self._pk)

        if mm.get('data'):
            return mm
        props = {self._pk: pk}
        record = util.GetOneRecordInfo(self._name, props)
        if not record:
            return None
        mm = MMode(self._name + ':%s' % pk, self._pk, data=record)
        mm.insert()
        return mm

    def getObjData(self, pk):
        '''根据主键，可以获得mmode对象的实例的数据.\n
        >>> m = madmin.getObjData(1)
        '''
        mm = MMode(self._name + ':%s' % pk, self._pk)
        if not mm.IsEffective():
            return None
        data = mm.get('data')
        if data:
            return data
        props = {self._pk: pk}
        record = util.GetOneRecordInfo(self._name, props)
        if not record:
            return None
        mm = MMode(self._name + ':%s' % pk, self._pk, data=record)
        mm.insert()
        return record

    def getObjList(self, pklist):
        '''根据主键列表获取mmode对象的列表.\n
        >>> m = madmin.getObjList([1,2,3,4,5])
        '''
        _pklist = []
        objlist = []
        for pk in pklist:
            mm = MMode(self._name + ':%s' % pk, self._pk)
            if not mm.IsEffective():
                continue
            if mm.get('data'):
                objlist.append(mm)
            else:
                _pklist.append(pk)
        if _pklist:
            recordlist = util.GetRecordList(self._name, self._pk, _pklist)
            for record in recordlist:
                pk = record[self._pk]
                mm = MMode(self._name + ':%s' % pk, self._pk, data=record)
                mm.insert()
                objlist.append(mm)
        return objlist

    def getPkListByProps(self, props):
        '''
        此方法优先查数据库，再对比缓存中是否存在明细记录，如果有该明细记录则用缓存的
           适用于初次加载如多条记录，如初次加载道具表信息到缓存时，而getObjList方法需要先取到pklist,再查询一次in操作，性能低
        @param props:{} where 条件字典
        '''
        pklist = []
        recordlist = util.GetRecordListByProps(self._name, props)
        for record in recordlist:
            pk = record[self._pk]
            mm = MMode(self._name + ':%s' % pk, self._pk)
            if not mm.IsEffective() or not mm.get('data'):  #
                mm = MMode(self._name + ':%s' % pk, self._pk, data=record)
                mm.insert()
            if self._fk:
                fk = record[self._fk]
                self.addFK(fk, pk)
            pklist.append(pk)
        return pklist

    def getObjListByProps(self, props):
        '''
        此方法优先查数据库，再对比缓存中是否存在明细记录，如果有该明细记录则用缓存的
           适用于初次加载如多条记录，如初次加载道具表信息到缓存时，而getObjList方法需要先取到pklist,再查询一次in操作，性能低
        @param props:{} where 条件字典
        '''
        objlist = []
        t0 = time.time()
        recordlist = util.GetRecordListByProps(self._name, props)
        logger.debug('recordlist, %.3fs' % (time.time() - t0))
        for record in recordlist:
            pk = record[self._pk]
            mm = MMode(self._name + ':%s' % pk, self._pk)
            if mm.IsEffective() and mm.get('data'):
                objlist.append(mm)
                continue
            else:
                mm = MMode(self._name + ':%s' % pk, self._pk, data=record)
                mm.insert()
                objlist.append(mm)
        logger.debug('finish getObjListByProps, %.3fs' % (time.time() - t0))
        return objlist

    def deleteMode(self, pk, is_sync_now=False):
        '''根据主键删除内存中的某条记录信息，\n这里只是修改内存中的记录状态_state为删除状态.\n
        >>> m = madmin.deleteMode(1)
        '''
        mm = self.getObj(pk)
        if mm:
            #             print 'deleteMode pk=%s' %pk
            if self._fk:
                data = mm.get('data')
                if data:
                    fk = data.get(self._fk, 0)
                    name = '%s_fk:%s' % (self._name, fk)
                    fkmm = MFKMode(name)
                    pklist = fkmm.get('pklist')
                    if pklist is None:
                        pklist = self.getAllPkByFk(fk)
                    if pklist and pk in pklist:
                        pklist.remove(pk)
                    fkmm.update('pklist', pklist)
            mm.delete(is_sync_now)  # is_sync_now为True时,时候对象连外键也删除,同步到数据库 add by yizhiwu
        return True

    def checkAll(self):
        """同步内存中的数据到对应的数据表中。\n
        >>> m = madmin.checkAll()
        """
        key = '%s:%s:' % (mclient._hostname, self._name)
        _pklist = util.getallkeys(key, mclient.connection)
        for pk in _pklist:
            mm = MMode(self._name + ':%s' % pk, self._pk)
            if not mm.IsEffective():
                mm.mdelete()
                continue
            if not mm.get('data'):
                continue
            mm.checkSync(timeout=self._timeout)
        self.deleteAllFk()

    def deleteAllFk(self):
        """删除所有的外键
        """
        key = '%s:%s_fk:' % (mclient._hostname, self._name)
        _fklist = util.getallkeys(key, mclient.connection)
        for fk in _fklist:
            name = '%s_fk:%s' % (self._name, fk)
            fkmm = MFKMode(name)
            fkmm.mdelete()

    def new(self, data, isSyncNow=False):
        """创建一个新的对象
        """
        incrkey = self._incrkey
        if incrkey:
            incrvalue = self.incr('_incrvalue', 1)
            data[incrkey] = incrvalue - 1
            pk = data.get(self._pk)
            if pk is None:
                raise PKValueError(data)
            mm = MMode(self._name + ':%s' % pk, self._pk, data=data)
            setattr(mm, incrkey, pk)
        else:
            pk = data.get(self._pk)
            mm = MMode(self._name + ':%s' % pk, self._pk, data=data)
        if self._fk:
            fk = data.get(self._fk, 0)
            name = '%s_fk:%s' % (self._name, fk)
            fkmm = MFKMode(name)
            pklist = fkmm.get('pklist')
            if pklist is None:
                pklist = self.getAllPkByFk(fk)
            if pk not in pklist:
                pklist.append(pk)
            fkmm.update('pklist', pklist)
        setattr(mm, '_state', MMODE_STATE_NEW)
        mm.insert()

        if isSyncNow:
            mm.syncDB()  # 即时同步到数据库
        else:
            rdsclient.zadd(mm.dszset, mm.produceRedisKey(pk), time.time())

        return mm
