#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: QiBin
# Date : 20170411
"""
Usage:

"""
import psycopg2
import json
import logging
import os
import csv
import pandas as pd
from collections import defaultdict
from random import choice


remove_list = ['uv', 'vv', 'pt', 'chtype', 'day', 'hour', 'ap', 'cpn', 'sub', 'site', 'r', 'border', 'success', 'device', 'dau']
other_pa = ['cpn', 'device', 'border', 'success', 'product', 'sub', 'uv', 'vv', 'r',
            'dau', 'pv', 'sitetype', 'chtype']
"""
bid_list = [4]
bdid_val = [100000914]
pid_val = [11753, 167446, 51832, 54593, 291540]
sid_val = [55524, 85599, 80103]
#vid_val = [3916463, 3386268, 1002886]
vid_val = [3782665, 3782812]
cid_val = [98, 88, 87]
city_val = [309, 122, 261]
isp_val = [14]
version_val = [7005985, 7005962, 7006195]
country_val = [1]
province_val = [28]
chtype_val = [-1]
#ch_val = [106667, 106662]
ch_val = [47154, 54893]
#subch_val = [109613, 109601, 113191, 109616, 109605, 112342]
subch_val = [88803, 47134]
isfull_val = [1]
bigver_val = [1]
playtype_val = [0]
def_val = [0, 2]
duration_val = [1,2,3,4,5,6]
bid_val = str(choice(bid_list))
"""
"""
bid_list = [1]
bdid_val = [100006689]
pid_val = [11753, 167446, 51832, 54593, 291540]
sid_val = [55524, 85599, 80103]
#vid_val = [3916463, 3386268, 1002886]
vid_val = [3965452]
cid_val = [86]
city_val = [309, 122, 261]
isp_val = [14]
version_val = [7005985, 7005962, 7006195]
country_val = [1]
province_val = [24]
chtype_val = [-1]
#ch_val = [106667, 106662]
ch_val = [97, 94]
#subch_val = [109613, 109601, 113191, 109616, 109605, 112342]
subch_val = [88803, 47134]
isfull_val = [1]
bigver_val = [5]
playtype_val = [0]
def_val = [0, 2]
duration_val = [1,2,3,4,5,6]
bid_val = str(choice(bid_list))

"""
bid_list = [1]
bdid_val = [100007915]
pid_val = [11753, 167446, 51832, 54593, 291540]
sid_val = [55524, 85599, 80103]
#vid_val = [3916463, 3386268, 1002886]
vid_val = [3856628]
cid_val = [86,2000002]
city_val = [161, 330]
isp_val = [14]
version_val = [901284, 901484, 901483]
country_val = [1]
province_val = [17,18]
chtype_val = [1]
#ch_val = [106667, 106662]
ch_val = [94]
#subch_val = [109613, 109601, 113191, 109616, 109605, 112342]
subch_val = [441, 434]
isfull_val = [1]
bigver_val = [-1]
playtype_val = [0]
def_val = [0, 2]
duration_val = [1,2,3,4,5,6]
bid_val = str(choice(bid_list))

"""
bid_list = [8]
bdid_val = [100007976]
pid_val = [11753, 167446, 51832, 54593, 291540]
sid_val = [55524, 85599, 80103]
#vid_val = [3916463, 3386268, 1002886]
vid_val = [4006643, 4006624]
cid_val = [2]
city_val = [309, 122, 261]
isp_val = [14]
version_val = [7005985, 7005962, 7006195]
country_val = [1]
province_val = [28]
chtype_val = [-1]
#ch_val = [106667, 106662]
ch_val = [69170, 69172]
#subch_val = [109613, 109601, 113191, 109616, 109605, 112342]
subch_val = [-1]
isfull_val = [1]
bigver_val = [5]
playtype_val = [0]
def_val = [0, 2]
duration_val = [1,2,3,4,5,6]
bid_val = str(choice(bid_list))
"""

"""
bid_list = [9]
bdid_val = [100007850]
pid_val = [11753, 167446, 51832, 54593, 291540]
sid_val = [55524, 85599, 80103]
#vid_val = [3916463, 3386268, 1002886]
vid_val = [3875675]
cid_val = [6]
city_val = [309, 122, 261]
isp_val = [14]
version_val = [7005985, 7005962, 7006195]
country_val = [1]
province_val = [1]
chtype_val = [-1]
#ch_val = [106667, 106662]
ch_val = [284]
#subch_val = [109613, 109601, 113191, 109616, 109605, 112342]
subch_val = [6]
isfull_val = [1]
bigver_val = [4]
playtype_val = [0]
def_val = [0, 2]
duration_val = [1,2,3,4,5,6]
bid_val = str(choice(bid_list))
"""

"""
bid_list = [11]
bdid_val = [99994696]
pid_val = [11753, 167446, 51832, 54593, 291540]
sid_val = [55524, 85599, 80103]
#vid_val = [3916463, 3386268, 1002886]
vid_val = [3528772, 3523206, 3523339]
cid_val = [10]
city_val = [309, 122, 261]
isp_val = [14]
version_val = [7005985, 7005962, 7006195]
country_val = [1]
province_val = [17]
chtype_val = [-1]
#ch_val = [106667, 106662]
ch_val = [-1]
#subch_val = [109613, 109601, 113191, 109616, 109605, 112342]
subch_val = [6]
isfull_val = [1]
bigver_val = [4]
playtype_val = [0]
def_val = [0, 2]
duration_val = [1,2,3,4,5,6]
bid_val = str(choice(bid_list))
"""

"""
bid_list = [12]
bdid_val = [100008013]
pid_val = [11753, 167446, 51832, 54593, 291540]
sid_val = [55524, 85599, 80103]
#vid_val = [3916463, 3386268, 1002886]
vid_val = [3528772, 3523206, 3523339]
cid_val = [1]
city_val = [309, 122, 261]
isp_val = [14]
version_val = [7005985, 7005962, 7006195]
country_val = [1]
province_val = [19]
chtype_val = [-1]
#ch_val = [106667, 106662]
ch_val = [-1]
#subch_val = [109613, 109601, 113191, 109616, 109605, 112342]
subch_val = [304]
isfull_val = [1]
bigver_val = [4]
playtype_val = [0]
def_val = [0, 2]
duration_val = [1,2,3,4,5,6]
bid_val = str(choice(bid_list))
"""



def get_tname(file, db1):
    t_list = []
    ip1, port1, user1, password1, dbname1 = parse_conf(file, db1)
    tb_sql = "select tablename from pg_tables where tablename like 'pt%hour' " \
             "and tablename not like 'pt%chtype%hour' and tablename not like 'pt%subch%hour' " \
             "and tablename not like 'pt_vid%_hour';"
    tb_list = select_func(ip1, port1, user1, password1, dbname1, tb_sql)
    for i in tb_list:
        c = "".join(i)
        t_list.append(c)
    logging.info("User info:%s" % t_list)
    return t_list


def addword(index, word, number):
    """
    Add item for dict.
    :param index:
    :param word:
    :param number:
    :return:
    """
    index.setdefault(word, []).append(number)
    return index


def get_column_type(file, tb, db1):
    """
    Get the data type of list from rtb_list.
    :param file:
    :param tb:
    :param db1:
    :return:
    """
    data = {}
    ip1, port1, user1, password1, dbname1 = parse_conf(file, db1)
    rtb_list, t_type = result_dtable(tb)
    t_sql = "select column_name, data_type from information_schema.columns where table_name = \'%s\';" % tb
    print t_sql
    tb_list = select_func(ip1, port1, user1, password1, dbname1, t_sql)
    d = defaultdict(list)
    for i in rtb_list:
        if i in other_pa:
            rtb_list.remove(i)
    print rtb_list

    for k, v in tb_list:
        d[k].append(v)
    for i in rtb_list:
        if i in d.keys():
            data = addword(data, i, d[i][0])
    return data


def get_sql_result(file, tb, db1):
    """
    :param file:
    :param tb:
    :param db1:
    :return:
    """
    stra = sql = ""
    tl = get_column_type(file, tb, db1)
    for i in range(len(tl.keys())):
        key = tl.keys()[i]
        val = tl[key][0]
        if val in ['bigint', 'integer', 'smallint']:
            sql = "and %s != -1" % key
        elif val in ['character varying']:
            sql = "and %s != \'\'" % key
        if i != len(tl.keys()) - 1:
            sql += ' '
        stra += sql
    return stra


def get_sql_fact(file, tb, db1):
    """
    get the str
    :param tl:
    :return:
    """
    stra = sql = ""
    tl = get_column_type(file, tb, db1)
    for i in range(len(tl.keys())):
        key = tl.keys()[i]
        val = tl[key][0]
        if val in ['bigint', 'integer', 'smallint']:
            sql = "and %s is not null" % key
        elif val in ['character varying']:
            sql = "and %s != \'\'" % key
        if i != len(tl.keys()) - 1:
            sql += ' '
        stra += sql
    return stra


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


def select_func(ip, port, user, passwd, dbname, sql):
    """
    Use the select mode to get the select result.
    :param ip:
    :param port:
    :param user:
    :param passwd:
    :param dbname:
    :param sql:
    :return:
    """
    try:
        conn = psycopg2.connect(database=dbname, user=user, password=passwd, host=ip, port=port)
    except Exception:
        print ("Connection Database %s Error!" % dbname)
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data


def select_mode(ip, port, user, passwd, dbname, sql, tb):
    """
    Use the select mode to get the data and write data into a file.
    :param ip:
    :param port:
    :param user:
    :param passwd:
    :param dbname:
    :param sql:
    :param tb:
    :return:
    """
    try:
        conn = psycopg2.connect(database=dbname, user=user, password=passwd, host=ip, port=port)
    except Exception:
        print("Connection Database %s Error!" % dbname)
    cur = conn.cursor()
    tn = tb + '_' + dbname + '.csv'
    tn = os.path.join('/data1/qibin/workspace/mofang/data', tn)
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


def read_table(filename):
    """
    Get the table list
    :param filename:
    :return:
    """
    tn = []
    with open(filename, 'r') as l:
        lines = l.readlines()
        for line in lines:
            line = line.strip('\r\n')
            tn.append(line)
        return tn


def difference(left, right, on):
    """
    difference of two dataframes
    :param left: left dataframe
    :param right: right dataframe
    :param on: join key
    :return: difference dataframe
    """
    df = pd.merge(left, right, how='left', on=on)
    left_columns = left.columns
    col_y = df.columns[left_columns.size]
    df = df[df[col_y].isnull()]
    df = df.ix[:, 0:left_columns.size]
    df.columns = left_columns
    return df


def read_in_chunks(filePath):
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


def result_dtable(tb):
    """
    :param tb:
    :return:t_list:table field
    """
    src_list = [x for x in tb.rstrip('\r').split('_')]
    c_type = src_list[len(src_list)-1]
    return src_list, c_type


def date_d(ip, port, user, password, dbname, c_type):
    """
    Get time.
    :param ip:
    :param port:
    :param user:
    :param password:
    :param dbname:
    :param c_type:
    :return:
    """
    d_sql = ""
    if c_type == 'hour':
        d_sql = "select to_char(now() - interval '24hours','YYYYMMDDhh24');"
    elif c_type == 'day':
        d_sql = "select to_char(now() - interval '24hours','YYYYMMDD');"
    d_da = select_func(ip, port, user, password, dbname, d_sql)
    c = str(d_da[0]).split(',')[0].lstrip('\(')
    d_date = "".join(c)
    return d_date


def get_fact_table(tb, bid_val):
    """
    Get the right fact table for each result table.
    :param tb:
    :param bid_val:
    :return:
    """
    fact_t = ""
    if 'uv' in tb or 'vv' in tb:
        fact_t = "vv_fact"
    elif 'dau' in tb:
        if 'day' in tb:
            if bid_val in ['9', '12']:
                fact_t = "v2_pv_fact"
            else:
                fact_t = "pv_fact"
        else:
            fact_t = "pv_fact_min_hour"
    elif 'carousel' in tb and 'pt' in tb:
        fact_t = "live_lb_vv_fact"
    elif 'carousel' not in tb and 'pt' in tb:
        fact_t = "playtime_fact"
    elif 'device' in tb:
        fact_t = "device_fact"
    elif 'carousel' in tb:
        fact_t = "live_lb_vv_fact"
    elif 'border' in tb:
        fact_t = "border_fact_border_success"
    return fact_t


def get_first_val(tb, rtb_val):
    """
    :param rtb_val:
    :return:
    """
    any_val = ""
    #if rtb_val in tb:
    if 'sid' == rtb_val:
        any_val = str(choice(sid_val))
    elif 'pid' == rtb_val:
        any_val = str(choice(pid_val))
    elif 'bdid' == rtb_val:
        any_val = str(choice(bdid_val))
    elif 'cid' == rtb_val:
        any_val = str(choice(cid_val))
    elif 'vid' == rtb_val:
        any_val = str(choice(vid_val))
    elif 'bid' == rtb_val:
        any_val = bid_val
    elif 'isfull' == rtb_val:
        any_val = 1
    elif 'bigver' == rtb_val:
        any_val = 1
    elif 'city' == rtb_val:
        any_val = str(choice(city_val))
    elif 'isp' == rtb_val:
        any_val = str(choice(isp_val))
    elif 'chtype' == rtb_val:
        any_val = str(choice(chtype_val))
    elif 'ch' == rtb_val:
        any_val = str(choice(ch_val))
    elif 'version' == rtb_val:
        any_val = str(choice(version_val))
    elif 'subch' == rtb_val:
        any_val = str(choice(subch_val))
    elif 'playtype' == rtb_val:
        any_val = str(choice(playtype_val))
    elif 'fbdid' == rtb_val:
        any_val = str(choice(bdid_val))
    elif 'def' == rtb_val:
        any_val = str(choice(def_val))
    elif 'duration' == rtb_val:
	    any_val = str(choice(duration_val))
    return any_val


def get_param_val(tb, t_list):
    """
    According to the length of list, we get each value of param!
    :param tb:
    :param t_list:
    :return:
    """
    print "value of t_list is %s" % t_list
    t_list = [item for item in t_list if item not in remove_list]
    #t_list = list(set(t_list).difference(set(remove_list)))
    print "value in len of t_list is %s" % len(t_list)
    any_val = bny_val = cny_val = dny_val = eny_val = fny_val = ''
    if len(t_list) == 1:
        any_val = get_first_val(tb, t_list[0])
        bny_val = ''
        cny_val = ''
        dny_val = ''
        eny_val = ''
        fny_val = ''
    elif len(t_list) == 2:
        any_val = get_first_val(tb, t_list[0])
        bny_val = get_first_val(tb, t_list[1])
        cny_val = ''
        dny_val = ''
        eny_val = ''
        fny_val = ''
    elif len(t_list) == 3:
        any_val = get_first_val(tb, t_list[0])
        bny_val = get_first_val(tb, t_list[1])
        cny_val = get_first_val(tb, t_list[2])
        dny_val = ''
        eny_val = ''
        fny_val = ''
    elif len(t_list) == 4:
        any_val = get_first_val(tb, t_list[0])
        bny_val = get_first_val(tb, t_list[1])
        cny_val = get_first_val(tb, t_list[2])
        dny_val = get_first_val(tb, t_list[3])
        eny_val = ''
        fny_val = ''
    elif len(t_list) == 5:
        any_val = get_first_val(tb, t_list[0])
        bny_val = get_first_val(tb, t_list[1])
        cny_val = get_first_val(tb, t_list[2])
        dny_val = get_first_val(tb, t_list[3])
        eny_val = get_first_val(tb, t_list[4])
        fny_val = ''
    elif len(t_list) == 6:
        any_val = get_first_val(tb, t_list[0])
        bny_val = get_first_val(tb, t_list[1])
        cny_val = get_first_val(tb, t_list[2])
        dny_val = get_first_val(tb, t_list[3])
        eny_val = get_first_val(tb, t_list[4])
        fny_val = get_first_val(tb, t_list[5])
    return any_val, bny_val, cny_val, dny_val, eny_val, fny_val

