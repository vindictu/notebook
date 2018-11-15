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
import xlsxwriter
import xlrd
from xlutils.copy import copy
import collections
# from PooledDB import PooledDB


DBHOST = '10.113.128.191'
DBPORT = 3306
DBNAME = 'cd_nova'
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


xlsx_title_seq = {
    'domain_name': 1,
    'project_name': 2,
    'instance_num': 3,
    'allot_mem': 4,
    'total_mem': 5,
    'mem_usage_rate': 6,
    'allot_cpu': 7,
    'total_cpu': 8,
    'cpu_usage_rate': 9,
}


class ExeclRW(object):

    def __init__(self, f_name, deal_data, type='write', start_row=1):
        self.__f_name = f_name
        self.__deal_data = deal_data
        self.operation_book = None
        self.start_row = start_row
        if type == 'write':
            self.operation_book = getattr(self, 'write')()
        elif type == 'read':
            self.operation_book = getattr(self, 'read')()
        elif type == 'update':
            self.operation_book = getattr(self, 'update')()
        else:
            print('type error')
        func = getattr(self, 'do_{0}'.format(type))
        self.end_row = func()

    def write(self):
         w_book = xlsxwriter.Workbook(self.__f_name)
         return w_book

    def read(self):
        pass

    def update(self):
        wb = xlrd.open_workbook(self.__f_name)
        newb = copy(wb)
        return newb

    def do_write(self):
        merge_format = self.operation_book.add_format({
            'bold': True,          # 是否边框
            'border': 2,
            'align': 'center',     # 水平居中
            'valign': 'vcenter',   # 垂直居中
            'fg_color': '#D7E4BC', # 填充颜色
        })
        worksheet = self.operation_book.add_worksheet('运营统计')
        row_title_tag = 0
        merge_dict = {
            'enable_write_domain': 0,
            'merge': False,
            'merge_num': 0,
            'row': self.start_row,
        }
        allot_total_mem = 0
        allot_total_cpu = 0
        total_mem = 0
        total_cpu = 0
        for num in range(0, len(self.__deal_data)):
            dict_data = self.__deal_data[num]
            try:
                next_domain = self.__deal_data[num+1]['domain_name']
                domain = self.__deal_data[num]['domain_name']
                if next_domain == domain:
                    merge_dict['merge'] = True
                    merge_dict['merge_num'] += 1
                    merge_dict['enable_write_domain'] = 0
                else:
                    merge_dict['enable_write_domain'] = 1
            except Exception as e:
                merge_dict['enable_write_domain'] = 1
            for k, v in dict_data.items():
                if row_title_tag == 0:
                    worksheet.write(row_title_tag, xlsx_title_seq[k], k, merge_format)
                if k == 'domain_name':
                    if merge_dict['enable_write_domain'] == 1 and merge_dict['merge']:
                        worksheet.merge_range('B{0}:B{1}'.format(str(merge_dict['row'] + 1 - merge_dict['merge_num']),
                                                                 str(merge_dict['row'] + 1)), v, merge_format)
                        merge_dict['merge_num'] = 0
                        merge_dict['merge'] = False

                    elif merge_dict['enable_write_domain'] == 1 and not merge_dict['merge']:
                        worksheet.write(merge_dict['row'], xlsx_title_seq[k], v, merge_format)
                    else:
                        pass
                elif k == 'total_mem':
                    if row_title_tag == 0:
                        total_mem = v
                        worksheet.merge_range(1, xlsx_title_seq[k], len(self.__deal_data), xlsx_title_seq[k], v, merge_format)
                    else:
                        pass
                elif k == 'total_cpu':
                    if row_title_tag == 0:
                        total_cpu = v
                        worksheet.merge_range(1, xlsx_title_seq[k], len(self.__deal_data), xlsx_title_seq[k], v, merge_format)
                    else:
                        pass
                elif k == 'cpu_usage_rate':
                    pass
                elif k == 'mem_usage_rate':
                    pass
                else:
                    if k == 'allot_mem':
                        allot_total_mem += v
                    elif k == 'allot_cpu':
                        allot_total_cpu += v
                    worksheet.write(merge_dict['row'], xlsx_title_seq[k], v)
            row_title_tag += 1
            merge_dict['row'] += 1
        worksheet.merge_range('A{0}:A15'.format(self.start_row+1), '松江', merge_format)
        worksheet.merge_range(1, xlsx_title_seq['mem_usage_rate'], len(self.__deal_data), xlsx_title_seq['mem_usage_rate'], allot_total_mem/total_mem, merge_format)
        worksheet.merge_range(1, xlsx_title_seq['cpu_usage_rate'], len(self.__deal_data), xlsx_title_seq['cpu_usage_rate'], allot_total_cpu/total_cpu, merge_format)
        return merge_dict['row']

    def do_read(self):
        pass

    def do_update(self):
        worksheet = self.operation_book.get_sheet('成都')
        for k, v in self.__deal_data[0].items():
            worksheet.write(0, xlsx_title_seq[k], k)
            worksheet.write_merge(1, self.start_row - 1, xlsx_title_seq[k], xlsx_title_seq[k], v)
        self.operation_book.save('C:\\Users\\mayuanlin\\Desktop\\new.xls')

    def close(self):
        self.operation_book.close()


if __name__ == '__main__':
    m = Mysql()
    result = m.getAll(cmd_sql.Operating_statistics)
    total_c_m = m.getAll(cmd_sql.total_cpu_mem)
    for line in result:
        line.update(total_c_m[0])
        line.update({'mem_usage_rate': 1})
        line.update({'cpu_usage_rate': 1})
    e = ExeclRW('C:\\Users\\mayuanlin\\Desktop\\运营统计.xlsx', result)
    end_row = e.end_row
    e.close()
