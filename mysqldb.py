#!/usr/bin/python

import MySQLdb
import logging
_LOGGER = logging.getLogger("mysql")


class Mysql(object):
    def __init__(self, user, password, db, host='localhost', port=3306):
        self.user = user
        self.passwd = password
        self.dbname = db
        self.host = host
        self.port = int(port)
        self.getCon()

    def getCon(self):
        self.con = MySQLdb.connect(
                host=self.host, port=self.port,
                user=self.user, passwd=self.passwd, db=self.dbname)
        self.con.set_character_set('utf8')
        self.cur = self.con.cursor(MySQLdb.cursors.DictCursor)

    def disconnect(self):
        if self.con:
            self.cur.close()
            self.con.close()
            self.con = None

    def execute(self, sql_str, update=False):
        try:
            ret = self.cur.execute(sql_str)
            if update:
                self.con.commit()
            return ret
        except Exception as e:
            self.disconnect()
            self.getCon()
            ret = self.cur.execute(sql_str)
            if update:
                self.con.commit()
            return ret

    def executemany(self, sql_str, data, update=False):
        try:
            ret = self.cur.executemany(sql_str, data)
            if update:
                self.con.commit()
            return ret
        except Exception as e:
            ret = self.cur.executemany(sql_str, data)
            if update:
                self.con.commit()
            return ret

    def insert_batch(self, table, datas, update=True):
        '''
        insert data by batch using a list.
        '''
        fields = ''
        values_list = []
        if not datas or not isinstance(datas, list):
            return False, "data is null or data format error"
        keys = datas[0].keys()
        for data in datas:
            if not fields:
                fields = self.list2fields(keys)
            tmp = []
            for field in keys:
                tmp.append(data.get(field))
            values_list.append(tmp)
        values = ''
        for i in range(len(keys)):
            if values:
                values = '%s, %s' % (values, '%s')
            else:
                values = '%s'
        sql = 'insert into %s(%s) values(%s)' % (table, fields, values)
        print("insert_batch:%s" % sql)
        ret = self.executemany(sql, values_list, update)
        return ret, "OK"

    def dict2cond(self, cond, data=0):
        '''
        translate dict to mysql condition or set data.
        data:
        0  ---  mysql where condition.
        1  ---  mysql data condition.
        '''
        where = ''
        for key, value in cond.items():
            tmp = ''
            if isinstance(value, (int, long)):
                tmp = '`%s`=%d' % (key, value)
            elif isinstance(value, list):
                list_tmp = self.list2values(value)
                if list_tmp:
                    tmp = '`%s` in (%s)' % (key, list_tmp)
            elif isinstance(value, dict):
                tmp = ''
                for op, data_v in value.items():
                    if isinstance(data_v, (int, long)):
                        tmp_dict = '`%s`%s%d' % (key, op, data_v)
                    else:
                        tmp_dict = "`%s`%s'%s'" % (key, op, data_v)
                    if not tmp:
                        tmp = tmp_dict
                    else:
                        tmp = '%s and %s' % (tmp, tmp_dict)
            else:
                tmp = "`%s`='%s'" % (key, value)
            if where and not data:
                where = '%s and %s' % (where, tmp)
            elif where:
                where = '%s, %s' % (where, tmp)
            else:
                where = tmp
        return where

    def list2values(self, values):
        value_str = ''
        for value in values:
            if value_str:
                if isinstance(value, int):
                    value_str = '%s, %d' % (value_str, value)
                else:
                    value_str = "%s, '%s'" % (value_str, value)
            else:
                if isinstance(value, int):
                    value_str = '%d' % (value)
                else:
                    value_str = "'%s'" % value
        return value_str

    def list2fields(self, fields):
        field_str = ''
        for field in fields:
            if field_str:
                field_str = '%s, `%s`' % (field_str, field)
            else:
                if field == '*':
                    field_str = '%s' % field
                else:
                    field_str = '`%s`' % field
        return field_str

    def get(self, table, cond={}, fields=['*'], sort={}):
        '''
        mysql select;
        '''
        field_str = self.list2fields(fields)
        where = self.dict2cond(cond)
        if where:
            sql_str = 'select %s from `%s` where %s'\
                            % (field_str, table, where)
        else:
            sql_str = 'select %s from `%s`' % (field_str, table)
        if sort:
            sort_sql = ''
            sort_key = sort.keys()[0]
            reverse = sort.values()[0]
            if reverse >= 1:
                sort_sql = 'order by %s ASC' % sort_key
            else:
                sort_sql = 'order by %s DESC' % sort_key
            sql_str = '%s %s' % (sql_str, sort_sql)
        print(sql_str)
        ret = self.execute(sql_str)
        if not ret:
            return []
        return self.cur.fetchall()

    def get_one(self, table, cond={}, fields=['*'], sort={}):
        '''
        mysql select ... order by ... limit 1;
        '''
        self.disconnect()
        self.getCon()
        field_str = self.list2fields(fields)
        where = self.dict2cond(cond)
        if where:
            sql_str = 'select %s from `%s` where %s'\
                            % (field_str, table, where)
        else:
            sql_str = 'select %s from `%s`' % (field_str, table)
        print("get_one:%s" % sql_str)
        if sort:
            sort_sql = ''
            sort_key = sort.keys()[0]
            reverse = sort.values()[0]
            if reverse >= 1:
                sort_sql = 'order by %s ASC limit 1' % sort_key
            else:
                sort_sql = 'order by %s DESC limit 1' % sort_key
            sql_str = '%s %s' % (sql_str, sort_sql)
        ret = self.execute(sql_str)
        if not ret:
            self.disconnect()
            return {}
        return self.cur.fetchone()

    def update(self, table, cond, data, update=True):
        '''
        mysql update;
        '''
        where = self.dict2cond(cond)
        set_data = self.dict2cond(cond=data, data=1)
        if where:
            sql_str = 'update `%s` set %s where %s' % (table, set_data, where)
        else:
            sql_str = 'update `%s` set %s' % (table, set_data)
        print("update: %s" % sql_str)
        ret = self.execute(sql_str, update)
        if not ret:
            return False
        return ret

    def insert(self, table, data, update=True):
        '''
        mysql insert one record;
        '''
        SQL_FORMAT = "insert into `%s` (%s) values (%s)"
        keys = data.keys()
        values = data.values()
        field_str = self.list2fields(keys)
        value_str = self.list2values(values)
        sql_str = SQL_FORMAT % (table, field_str, value_str)
        print("insert: %s" % sql_str)
        try:
            ret = self.execute(sql_str, update)
            if not ret:
                return False, "unknow Error"
            if update:
                self.con.commit()
            return ret, "OK"
        except Exception as e:
            return False, e

    def return_error(self):
        self.con.rollback()
        self.disconnect()

    def increaseMutex(
            self, table, cond, field, count=1, greater=0, check=0, data={}):
        '''
        increase the record's field by count and update the data to the record
        if the check has been set and the original data is greater the the
        value of greater;

        table   <--->  the table name;                  str
        cond    <--->  the condition of the data;       dict
        field   <--->  the field should be increased;   str
        count   <--->  the increase number;             int
        greater <--->  the compared data;               int
        check   <--->  the switch of comparing or not;  bool
        data    <--->  the updated data;                dict
        '''
        self.disconnect()
        self.getCon()
        where = self.dict2cond(cond)
        if where:
            sql_str = "select * from %s where %s for update" % (table, where)
        else:
            sql_str = "select * from %s for update" % table
        print('increaseMutex: %s' % sql_str)
        try:
            ret = self.execute(sql_str)
            if not ret:
                self.return_error()
                return False, 'unknow Error execute:%s!' % sql_str
            record = self.cur.fetchone()
            if not record:
                #self.con.rollback()
                self.return_error()
                return False, 'Not Found Record %s' % where
            original = record.get(field)
            try:
                original = long(original)
            except Exception as e:
                self.return_errro()
                return False, '%s type is not int' % field
            comp = original + count
            if check and comp < greater:
                self.return_error()
                return False, '%d is less than %d' % (original, -count)
            if data:
                data.update({field: comp})
            else:
                data = {field: comp}
            ret = self.update(table, cond, data, False)
            if not ret:
                self.return_error()
                return False, 'unknow Error execute:%s' % 'update'
            self.con.commit()
            self.disconnect()
            return True, record
        except Exception as e:
            #self.con.rollback()
            self.disconnect()
            return False, e

    def updateMutexCheck(self, table, cond, data, status, fields=['*']):
        '''
        update the record with data according to the original value
        of status with mutex.

        parameters:

        table <---> table name.
        cond  <---> query condition.
        data  <---> update data.
        status<---> check the status, if not match, then did not update.
        '''
        self.disconnect()
        self.getCon()
        SQL_STR = "select %s from `%s` where %s for update"
        where = self.dict2cond(cond)
        fields_str = self.list2fields(data.keys())
        values_str = self.list2values(data.values())
        sql_str = SQL_STR % (fields_str, table, where)
        print("updateMutexCheck: %s" % sql_str)
        result = False
        msg = ''
        try:
            ret = self.execute(sql_str)
            record = self.cur.fetchone()
            if not record:
                self.con.rollback()
                result = False
                msg = "Not Found Record By %s" % where
            else:
                flag = 1
                for check_key, check_value in status.items():
                    value = record.get(check_key)
                    if isinstance(check_value, (int, str, unicode)):
                        if value != check_value:
                            flag = 0
                            break
                    elif isinstance(check_value, list):
                        if value not in check_value:
                            flag = 0
                    elif isinstance(check_value, dict):
                        for op, c_value in check_value.items():
                            express = "%s %s %s" % (value, op, c_value)
                            if not eval(express):
                                flag = 0
                                break
                    else:
                        flag = 0
                    if not flag:
                        break
                if flag == 0:
                    self.con.rollback()
                    result = False
                    msg = "status check did not match"
                else:
                    ret = self.update(table, cond, data)
                    self.con.commit()
                    result = ret
                    msg = "OK"
        except Exception as e:
            self.con.rollback()
            result = False
            msg = e
        finally:
            self.disconnect()
        return result, msg

    def transaction(self, **kwargs):
        '''
        mysql transaction operation.

        kwargs parameters:
        table_name=data, table_name1=data1, ....
        '''
        self.disconnect()
        self.getCon()
        result = True
        msg = ''
        try:
            for table, data in kwargs.items():
                if isinstance(data, list):
                    successed, msg = self.insert_batch(
                                        table, data, update=False)
                    if not successed:
                        result = False
                        msg = 'insert into %s error: %s' % (table, msg)
                        break
                    else:
                        msg += 'save to %s, %d records' % (table, successed)
                else:
                    successed, msg = self.insert(table, data, update=False)
                    if not successed:
                        result = False
                        msg = 'insert into %s error: %s' % (table, msg)
                        break
                    else:
                        msg += 'save to %s, %d records' % (table, successed)
            if result:
                print('commit')
                self.con.commit()
            else:
                self.con.rollback()
        except Exception as e:
            self.con.rollback()
            result = False
            msg = "Exception happened: %s" % e
        finally:
            self.disconnect()
        return result, msg

    def increaseMultiMutex(
            self, table, cond, field, count=1,
            greater=0, check=0, data={}, uniqKey=""):
        self.disconnect()
        self.getCon()
        where = self.dict2cond(cond)
        if not where or not uniqKey:
            return False, "cond is null or uniqKey is null"
        sql_str = "select * from %s where %s for update" % (table, where)
        print('increaseMultiMutex: %s' % sql_str)
        try:
            ret = self.execute(sql_str)
            if not ret:
                self.return_error()
                return False, 'unknow Error execute:%s!' % sql_str
            records = self.cur.fetchall()
            for record in records:
                original = record.get(field)
                try:
                    original = long(original)
                except Exception as e:
                    self.return_errro()
                    return False, '%s type is not int' % field
                comp = original + count
                if comp <= 0:
                    count = comp
                #if check and comp < greater:
                #    self.return_error()
                #    return False, '%d is less than %d' % (original, -count)
                update_count = 0 if comp < 0 else comp
                if data:
                    data.update({field: update_count})
                else:
                    data = {field: comp}
                uniq = record.get(uniqKey)
                update_cond = {uniqKey: uniq}
                ret = self.update(table, update_cond, data, False)
                if not ret:
                    self.return_error()
                    return False, 'unknow Error execute:%s' % 'update'
                if comp >= 0:
                    break
            self.con.commit()
            self.disconnect()
            return True, record
        except Exception as e:
            #self.con.rollback()
            self.disconnect()
            return False, e


if __name__ == '__main__':
    mysqldb = Mysql('root', '123456', 'test')
    print mysqldb.update('user', {'uid': 4, 'status': 27}, {'nickname': 'gfshi1', 'desc': 'gfshi test'})
