#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 10/24/2018 10:59 PM
# @Author  : mayuanlin
# @Mail    : ma.vindictu@gmail.com
# @Site    :
# @File    : mysql_handler.py
# @Software: PyCharm

import pymysql
from pymysql.cursors import DictCursor
from DBUtils.PooledDB import PooledDB
import cmd_sql
# from PooledDB import PooledDB


DBHOST = '10.113.128.191'
DBPORT = 3306
DBNAME = 'nova'
DBUSER = 'root'
DBPWD = 'DMS@NaT3'
DBCHAR = 'utf8'
mincached = 1
maxcached = 10
maxshared = 20
maxconnecyions = 100
blocking = True
maxusage = 0
setsession = []


class Mysql(object):
    """
    MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现获取连接对象：conn = Mysql.getConn()
            释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None

    def __init__(self):
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self._conn = Mysql.__getConn()
        if self._conn:
            self._cursor = self._conn.cursor()
        else:
            print('Can not get connect!')

    @staticmethod
    def __getConn():
        """
        @summary: 静态方法，从连接池中取出连接
        @return MySQLdb.connection
        """
        if Mysql.__pool is None:
            __pool = PooledDB(creator=pymysql, mincached=1, maxcached=20,
                              host=DBHOST, port=DBPORT, user=DBUSER, passwd=DBPWD,
                              db=DBNAME, charset=DBCHAR, cursorclass=DictCursor, connect_timeout=10)
        else:
            __pool = None
        return __pool.connection()

    def getAll(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchall()
        else:
            result = False
        self.close()
        return result

    def getOne(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchone()
        else:
            result = False
        self.close()
        return result

    def getMany(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchmany(num)
        else:
            result = False
        self.close()
        return result

    def insertOne(self, sql, value, commit=1):
        """
        @summary: 向数据表插入一条记录
        @param sql:要插入的ＳＱＬ格式
        @param value:要插入的记录数据tuple/list
        @return: insertId 受影响的行数
        """
        self._cursor.execute(sql, value)
        return self.__getInsertId(commit)

    def insertMany(self, sql, values, commit=1):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        count = self._cursor.executemany(sql, values)
        self.dispose(isend=commit)
        return count

    def __getInsertId(self, commit):
        """
        获取当前连接最后一次插入操作生成的id,如果没有则为０
        """
        self._cursor.execute("SELECT @@IDENTITY AS id")
        result = self._cursor.fetchall()
        self.dispose(isend=commit)
        return result[0]['id']

    def __query(self, sql, commit=1, param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        self.dispose(isend=commit)
        return count

    def update(self, sql, commit=1, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, commit, param)

    def delete(self, sql, commit=1, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, commit, param)

    def begin(self):
        """
        @summary: 开启事务
        """
        self._conn.autocommit(0)

    def end(self, option='commit'):
        """
        @summary: 结束事务
        """
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    def dispose(self, isend=1):
        """
        @summary: 释放连接池资源
        """
        if isend == 1:
            self.end('commit')
        else:
            self.end('rollback')
        self._cursor.close()
        self._conn.close()

    def close(self):
        """
        @summary: 释放连接池资源无需提交相关
        """
        self._cursor.close()
        self._conn.close()


if __name__ == '__main__':
    m = Mysql()
    result = m.getAll(cmd_sql.Operating_statistics)