# coding:utf-8
# !/user/bin/python
"""
Created on 2017年2月10日
@author: yizhiwu
内存对象集,可直接添加内存对象
"""
from dbentrust.madminanager import MAdminManager
from dbentrust.mmode import MAdmin

# ---------------添加的内存库表----------------
tb_test = MAdmin('tb_test', 'id', fk='fid')  # 用户表
tb_test.insert()
MAdminManager().registe(tb_test)
