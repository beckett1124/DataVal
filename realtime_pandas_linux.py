#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: QiBin
# Date : 20170309
"""
Usage:

"""
import psycopg2
import json
import os
import threading
import pandas as pd
import csv
import shutil


threads = []

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
    tn = sql.split('from')[1].split('where')[0].strip() + '_' + dbname + '.csv'
    tn = os.path.join('/home/qibin/realtime', tn)
    f = open(tn, 'w')
    w = csv.writer(f)
    cur.execute(sql)
    while True:
        row = cur.fetchone()
        if not row:
            break
        w.writerow(row)
    f.close()
    cur.close()
    conn.close()
    return tn


def table_val(file, db, sql):
    ip, port, user, passwd, dbname = parse_conf(file, db)
    try:
        conn = psycopg2.connect(database=dbname, user=user, password=passwd, host=ip, port=port)
    except Exception, e:
        print e.args[0]
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


def data_merge_bak(file, db1, db2, sql):

    ip1, port1, user1, password1, dbname1 = parse_conf(file, db1)
    ip2, port2, user2, password2, dbname2 = parse_conf(file, db2)
    data_test = select_mode(ip1, port1, user1, password1, dbname1, sql)
    data_dm = select_mode(ip2, port2, user2, password2, dbname2, sql)
    data_intersection = [val for val in data_dm if val in data_test]
    data_dm_diff = list(set(data_dm).difference(set(data_test)))
    tn = sql.split('from')[1].split('where')[0]
    if len(data_intersection) == len(data_dm) and len(data_dm_diff) is 0:
        print "%s Success!" % tn
    else:
        print "%s False!" % tn


def data_merge(file, db1, db2, sql):
    p_flag = False
    ip1, port1, user1, password1, dbname1 = parse_conf(file, db1)
    ip2, port2, user2, password2, dbname2 = parse_conf(file, db2)
    f1 = select_mode(ip1, port1, user1, password1, dbname1, sql)
    f2 = select_mode(ip2, port2, user2, password2, dbname2, sql)
    tn = sql.split('from')[1].split('where')[0]
    rf1 = os.path.join('/home/qibin/realtime', 'bak' + '_' + f1.split('/')[-1])
    rf2 = os.path.join('/home/qibin/realtime', 'bak' + '_' + f2.split('/')[-1])
    ff1 = os.path.join('/home/qibin/realtime', f1)
    ff2 = os.path.join('/home/qibin/realtime', f2)
    if os.path.getsize(ff1) == 0 and os.path.getsize(ff2) == 0:
        p_flag = True
        print "%s Success!" % tn
    elif os.path.getsize(ff1) == 0 and os.path.getsize(ff2) > 0:
        p_flag = False
        print "%s False!" % tn
    elif os.path.getsize(ff2) == 0 and os.path.getsize(ff1) > 0:
        p_flag = False
        print "%s False!" % tn
    else:
        right = pd.read_csv(ff1, header=None, sep=',', low_memory=False)
        left = pd.read_csv(ff2, header=None, sep=',', low_memory=False)

        if len(right) != len(left):
            p_flag = False
            print "%s False!" % tn
        else:
            right[1] = 'addon'
            left[1] = 'addon'
            right.sort_values([0]).to_csv(rf1)
            left.sort_values([0]).to_csv(rf2)
            fa = read_in_chunks(rf1)
            fb = read_in_chunks(rf2)
            df1 = difference(fa, fb, 0)
            df2 = difference(fb, fa, 0)

            if df1.equals(df2) and df1.empty:
                p_flag = True
                print "%s Success!" % tn
            else:
                p_flag = False
                print "%s False!" % tn
    file_list = [f1, f2, ff1, ff2, rf1, rf2]
    for file in file_list:
        if os.path.exists(file):
            if p_flag:
                os.remove(file)
            else:
                shutil.move(file, os.path.join('/home/qibin/realtime/bak', file))



def difference(left, right, on):
    """
    difference of two dataframes
    :param left: left dataframe
    :param right: right dataframe
    :param on: join key
    :return: difference dataframe
    """
    df = pd.merge(left, right, how='left', on=on)
    #df = pd.concat([left, right])
    left_columns = left.columns
    #col_y = df.columns[left_columns.size - 1]
    col_y = df.columns[left_columns.size]
    df = df[df[col_y].isnull()]
    df = df.ix[:, 0:left_columns.size]
    df.columns = left_columns
    return df


def read_in_chunks(filePath, chunk_size=1024*1024):
    """
    Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1M
    You can set your own chunk size
    """
    file_object = pd.read_csv(filePath, header=None, sep=',', chunksize=1024*1024)
    for chunk in file_object:
        if chunk.empty:
            break
        return chunk


def main():
    file = "database.json"
    db_test = 'test_result'
    db_bak = 'dm_result'
    tn_list = read_table('table.txt')
    #tn = "uv_pid_chtype_ch_playtype_hour"
    #sql = "select * from %s where date >= 2017031100 and date < 2017031200 and bid =1 order by date;" % tn
    #data_merge(file, db_test, db_bak, sql)
    #t = threading.Thread(target=data_merge, args=(file, db_test, db_bak, sql))
    #t.setDaemon(True)
    #t.start()
    #threads.append(t)


    for i in range(len(tn_list)):
        tn = tn_list[i]
        #tn = "uv_pid_chtype_ch_playtype_hour"
        #sql_test = "select count(*) from pg_class where relname = '%s';" % tn
        #test_result = table_val(file, db_test, sql_test)
        #dm_result = table_val(file, db_bak, sql_test)
        #if test_result != dm_result or dm_result != 1:
        #    tn = tn_list[i+1]
        sql = "select * from %s where date >= 2017031100 and date < 2017031200 and bid =1 order by date;" % tn
        data_merge(file, db_test, db_bak, sql)
      #  t = threading.Thread(target=data_merge, args=(file, db_test, db_bak, sql))
      #  t.setDaemon(True)
      #  t.start()
      #  threads.append(t)

    #for i in range(len(tn_list)):
    #    threads[i].join()


if __name__ == '__main__':
    main()
