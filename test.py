# coding:utf-8
# !/user/bin/python
"""缓存测试"""
import time
from dbfront import member


# 连接缓存
member.connection()


def run():
    from db.memmode import tb_test
    obj = tb_test.getObj(1)
    for ca in range(1, 10):
        tmp_dict = {"name": "id" + str(ca)}
        print "updata:%s" % str(tmp_dict)
        obj.update_multi(tmp_dict)
        print "now:%s" % str(obj.get("data"))
        time.sleep(1)


if __name__ == '__main__':
    run()
