#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: QiBin
# Date : 20170309
"""
Usage:

"""
import psycopg2
import json
from logger import logger


def parse_conf(filename, dbname):
    """
    According to the conf fileanme, get the param of database.
    :param filename: database.json
    :return:
    """
    with open(filename) as jsonfile:
        json_data = json.load(jsonfile)
        paramList = ['ip', 'port', 'user', 'password', 'database']
        outList = []
        sess = json_data.keys()
        if dbname in sess:
            for db_value in json_data[dbname]:
                opts = db_value.keys()
                for opt in paramList:
                    if opt in opts:
                        outList.append(db_value[opt])
                    else:
                        outList.append(None)
                return outList


def select_mode(ip, port, user, passwd, dbname, sql):
    try:
        conn = psycopg2.connect(database=dbname, user=user, password=passwd, host=ip, port=port)
    except:
        print "Error:数据库连接错误"
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    return data


def read_table(filename):
    tn = []
    with open(filename, 'r') as l:
        lines = l.readlines()
        for line in lines:
            line = line.strip('\n')
            tn.append(line)
        return tn


def main(file, db1, db2, sql):
    ip1, port1, user1, password1, dbname1 = parse_conf(file, db1)
    ip2, port2, user2, password2, dbname2 = parse_conf(file, db2)
    data_test = select_mode(ip1, port1, user1, password1, dbname1, sql)
    data_dm = select_mode(ip2, port2, user2, password2, dbname2, sql)
    data_intersection = [val for val in data_dm if val in data_test]
    data_dm_diff = list(set(data_dm).difference(set(data_test)))
    if len(data_intersection) == len(data_dm) and len(data_dm_diff) is 0:
        logger.info("")
        print "Success!"
    else:
        print "False!"
        print data_dm_diff
        print data_intersection


if __name__ == '__main__':
    file = "database.json"
    db_test = 'test_result'
    db_bak = 'dm_result'
    tn_list = read_table('table.txt')
    #tn = raw_input("Please input U table name:\n")
    #sql = "select * from %s where date >= 2017030900 and bid =1 order by date;" % tn
    for i in range(len(tn_list)):
        tn = tn_list[i]
        print tn
        sql = "select * from %s where date >= 2017031300 and date < 2017031400 and bid =1 order by date;" % tn
        main(file, db_test, db_bak, sql)
