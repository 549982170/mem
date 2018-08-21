# coding:utf8


class DBException(Exception):
    """自定义的异常处理类"""

    def __init__(self, code, msg):
        """
        异常处理类
        @param code:
        @param msg:
        """
        Exception.__init__(self)
        self.code = code
        self.msg = msg

    def __str__(self):
        logstr = "code:%s,msg:%s" % (self.code, self.msg)
        return logstr

    def getResponse(self):
        return {'result': self.code, 'msg': self.msg}
