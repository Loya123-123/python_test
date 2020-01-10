# -*- coding: utf-8 -*-
import pymysql

class mysqlUtil(object):
    config = {
        "host":"rm-2ze872u430u6olqz7ro.mysql.rds.aliyuncs.com",
        "user":"oam",
        "password":"Sunac_qjy^2019_",
        "db":"paas",
        "charset":"t28000_member"
    }

    def __init__(self):
        self.connect = None
        self.cursor = None

    def login(self):
        try:
            # self.connect = pymysql.connect("localhost","root","12345678","test")
            self.connect = pymysql.connect(mysqlUtil.config['host'],
                                           mysqlUtil.config['user'],
                                           mysqlUtil.config['password'],
                                           mysqlUtil.config['db'])
            self.cursor = self.connect.cursor()
        except Exception as ex :
            print(ex)

    def getOne(self,sql):
        try:
            self.login()
            self.cursor.execute(sql)
            return self.cursor.fetchone()
        except Exception as ex :
            print(ex,ex)

        finally:
            self.close()

    def getAll(self,sql,*args):
        try:
            self.login()
            self.cursor.execute(sql,args)
            return self.cursor.fetchall()
        except Exception as ex :
            print(ex,ex)

        finally:
            self.close()

    def executeDML(self,sql):
        try:
            self.login()
            num = self.cursor.execute(sql)
            self.connect.commit()
            return num
        except Exception as ex :
            print(ex)
            self.connect.rollback()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connect:
            self.connect.close()

# if __name__ == '__main__':
#     db = mysqlUtil()
#     data = db.getAll("desc t28000_member.person;")
#     print(data)
#     a = list(data)
#     c=[]
#     for i in a :
#         b=list(i)
#         c.append(b)
#     for i in c :
#         print(i)

