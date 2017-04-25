#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: QiBin
# Date : 20170411
"""
Usage:

"""
import psycopg2
import json
import logger
import logging
from random import choice
import os
import csv
import pandas as pd


data_list = ['pid', 'sid', 'cid', 'bdid', 'city', 'chtype', 'ch', 'duration', 'playtype', 'isfull']
bid_list = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
chtype_list = [0, -1, 1, 2]
playtype_list = [0, 3]
isfull_list = [0, 1, -1, 3, 4]
duration_list = [0, 1, 2, 3, 4, 5, 6]


def param_get(ip1, port1, user1, password1, dbname1):
    """
    Get information for pid/cid/bdid/vip and so on
    :param ip1:
    :param port1:
    :param user1:
    :param password1:
    :param dbname1:
    :return:
    """
    pid = cid = sid = bdid = vid = []
    pid_sql = "select distinct(pid) from vv_pid_day where date in (select to_char(now() - interval '24hours','YYYYMMDD')) limit 100;"
    cid_sql = "select distinct(cid) from vv_cid_day where date in (select to_char(now() - interval '24hours','YYYYMMDD')) limit 37;"
    sid_sql = "select distinct(sid) from vv_sid_day where date in (select to_char(now() - interval '24hours','YYYYMMDD')) limit 100;"
    bdid_sql = "select distinct(bdid) from vv_bdid_day where date in (select to_char(now() - interval '24hours','YYYYMMDD')) limit 100;"
    vid_sql = "select distinct(vid) from vv_vid_day where date in (select to_char(now() - interval '24hours','YYYYMMDD')) limit 100;"

    pid_list = select_func(ip1, port1, user1, password1, dbname1, pid_sql)
    cid_list = select_func(ip1, port1, user1, password1, dbname1, cid_sql)
    sid_list = select_func(ip1, port1, user1, password1, dbname1, sid_sql)
    bdid_list = select_func(ip1, port1, user1, password1, dbname1, bdid_sql)
    vid_list = select_func(ip1, port1, user1, password1, dbname1, vid_sql)
    for i in pid_list:
        c = str(i).split(',')[0].lstrip('\(')
        c = "".join(c)
        pid.append(c)
    for j in cid_list:
        k = str(j).split(',')[0].lstrip('\(')
        k = "".join(k)
        cid.append(k)
    for m in sid_list:
        l = str(m).split(',')[0].lstrip('\(')
        l = "".join(l)
        sid.append(l)
    for n in bdid_list:
        g = str(n).split(',')[0].lstrip('\(')
        g = "".join(g)
        bdid.append(g)
    for s in vid_list:
        z = str(s).split(',')[0].lstrip('\(')
        z = "".join(z)
        vid.append(z)
    #return pid, cid, sid, bdid, vid
    return vid


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
    try:
        conn = psycopg2.connect(database=dbname, user=user, password=passwd, host=ip, port=port)
    except:
        print "Database connect Error!"
    cur = conn.cursor()
    tn = tb + '_' + dbname + '.csv'
    tn = os.path.join('/home/qibin/pt/city/data', tn)
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


def get_tname(file, db1, p, t):
    t_list = []
    ip1, port1, user1, password1, dbname1 = parse_conf(file, db1)
    tb_sql = ""
    if p == 'uv':
        if t == "hour":
            tb_sql = "select tablename from pg_tables where tablename like 'uv%city%hour';"
        else:
            tb_sql = "select tablename from pg_tables where tablename like 'uv%city%day';"
    elif p == 'vv':
        if t == "hour":
            tb_sql = "select tablename from pg_tables where tablename like 'vv%city%hour';"
        else:
            tb_sql = "select tablename from pg_tables where tablename like 'vv%city%day';"
    elif p == 'pt':
        if t == "hour":
            tb_sql = "select tablename from pg_tables where tablename like 'pt%city%hour';"
        else:
            tb_sql = "select tablename from pg_tables where tablename like 'pt%city%day';"
    elif p == 'dau':
        if t == "hour":
            tb_sql = "select tablename from pg_tables where tablename like 'dau%city%hour';"
        else:
            tb_sql = "select tablename from pg_tables where tablename like 'dau%city%day';"
    tb_list = select_func(ip1, port1, user1, password1, dbname1, tb_sql)
    for i in tb_list:
        c = "".join(i)
        t_list.append(c)
    logging.info("User info:%s" % t_list)
    return t_list


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
        d_sql = "select to_char(now() - interval '25hours','YYYYMMDDhh24');"
    elif c_type == 'day':
        d_sql = "select to_char(now() - interval '25hours','YYYYMMDD');"
    d_da = select_func(ip, port, user, password, dbname, d_sql)
    c = str(d_da[0]).split(',')[0].lstrip('\(')
    d_date = "".join(c)
    return d_date


def data_uv_sql(tb, rtb_list, date_data, t_type, d_type, pid_list, cid_list, sid_list, bdid_list, vid_list):
    """

    :param tb:
    :param rtb_list:
    :param date_data:
    :param t_type:
    :param d_type:
    :param pid_list:
    :param cid_list:
    :param sid_list:
    :param bdid_list:
    :param vid_list:
    :return:
    """
    r_sql = ""
    if t_type in ['hour', 'day']:
        rtb_list.remove(t_type)
    if d_type in ['uv', 'vv', 'dau', 'pt']:
        rtb_list.remove(d_type)
    if 'city' in rtb_list:
        rtb_list.remove('city')

    bid_val = str(choice(bid_list))
    if bid_val == '1':
        cid_list = [83, 84, 85, 86, 87, 88, 97, 98]
    else:
        cid_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    if rtb_list[0] == 'cid':
        if len(rtb_list) == 1:
            r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and cid = \'%s\' group by " \
                    "city, %s order by city, %s desc;" \
                    % (d_type, tb, date_data, bid_val, str(choice(cid_list)), d_type, d_type)
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' group by city, %s order by city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(cid_list)), str(choice(chtype_list)), d_type, d_type)
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                    "playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                         str(choice(cid_list)), str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                    "isfull =\'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                       str(choice(cid_list)),
                                                                       str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                    "duration =\'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                       str(choice(cid_list)),
                                                                       str(choice(duration_list)), d_type, d_type)
            else:
                raise Exception("cid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(cid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                     playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(cid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                    r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and cid = \'%s\'and " \
                            "chtype = \'%s\' and duration = \'%s\'group by city, %s order by city, %s desc;" % (d_type, tb,
                                                                                                        date_data,
                                                                                                 bid_val,
                                                                                                 str(choice(cid_list)),
                                                                                                 str(choice(chtype_list)),
                                                                                                 str(choice(
                                                                                                     duration_list)), d_type, d_type)
            else:
                raise Exception("cid has no such table!")
        elif len(rtb_list) == 4:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull' and rtb_list[3] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' and playtype = \'%s\' group by city, %s order by " \
                        "city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(cid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration' and rtb_list[3] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' and playtype = \'%s\' group by city, %s order by city " \
                        ", %s desc;" % (d_type, tb, date_data, bid_val, str(choice(cid_list)), str(choice(chtype_list)),
                                   str(choice(duration_list)), str(choice(playtype_list)), d_type, d_type)
            else:
                raise Exception("cid has no such table!")
    elif rtb_list[0] == 'pid':
        if len(rtb_list) == 1:
            r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and pid = \'%s\' group by " \
                    "city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val, str(choice(pid_list)), d_type, d_type)
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(pid_list)), str(choice(chtype_list)), d_type, d_type)
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(pid_list)), str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "isfull = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, str(choice(bid_list)),
                                                                           str(choice(pid_list)), str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "duration = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, str(choice(bid_list)),
                                                                           str(choice(pid_list)), str(choice(duration_list)), d_type, d_type)
            else:
                raise Exception("pid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(pid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city, %s order by city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(pid_list)), str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' group by city, %s order by city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(pid_list)), str(choice(chtype_list)),
                           str(choice(duration_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull' and rtb_list[2] == 'chtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city, %s order by city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(pid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), d_type, d_type)
            else:
                raise Exception("pid has no such table!")
        elif len(rtb_list) == 4:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull' and rtb_list[3] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(pid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration' and rtb_list[3] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\'and pid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' and playtype = \'%s\' group by city, %s order by " \
                        "city, %s desc; " \
                        % (d_type, tb, date_data, bid_val, str(choice(pid_list)),str(choice(chtype_list)),
                           str(choice(duration_list)), str(choice(playtype_list)), d_type, d_type)
            else:
                raise Exception("pid has no such table!")

    elif rtb_list[0] == 'sid':
        if len(rtb_list) == 1:
            r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and sid = \'%s\' group by " \
                    "city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val, str(choice(sid_list)), d_type, d_type)
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(sid_list)),
                                                                                       str(choice(chtype_list)), d_type, d_type)
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and sid = \'%s\'and " \
                        "playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(sid_list)),
                                                                             str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "isfull = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(sid_list)),
                                                                           str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "duration = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(sid_list)),
                                                                           str(choice(duration_list)), d_type, d_type)
            else:
                raise Exception("sid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(sid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                     playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(sid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(sid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                                 str(choice(
                                                                                                     duration_list)), d_type, d_type)
            else:
                raise Exception("sid has no such table!")
        elif len(rtb_list) == 4:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull' and rtb_list[3] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(sid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration' and rtb_list[3] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' and playtype = \'%s\' group by city, %s order by " \
                        "city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(sid_list)), str(choice(chtype_list)),
                           str(choice(duration_list)), str(choice(playtype_list)), d_type, d_type)
            else:
                raise Exception("sid has no such table!")

    elif rtb_list[0] == 'bdid':
        if len(rtb_list) == 1:
            r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\' group by " \
                    "city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val, str(choice(bdid_list)),
                                                          d_type, d_type)
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(bdid_list)),
                                                                           str(choice(chtype_list)), d_type, d_type)
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\'and bdid = \'%s\' and " \
                        "playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(bdid_list)),
                                                                             str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\'and " \
                        "isfull = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(bdid_list)),
                                                                           str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\'and " \
                        "duration = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(bdid_list)),
                                                                           str(choice(duration_list)), d_type, d_type)
            else:
                raise Exception("bdid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(bdid_list)),
                                                                                                 str(choice(chtype_list)),
                                                                                                 str(choice(
                                                                                                     playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                 r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(bdid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\'and bdid = \'%s\'and " \
                        "chtype = \'%s\' and duration = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(bdid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                     duration_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull' and rtb_list[2] == 'chtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\'and bdid = \'%s\'and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(bdid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                     isfull_list)), d_type, d_type)
            else:
                raise Exception("fbdid has no such table!")
        elif len(rtb_list) == 4:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull' and rtb_list[3] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(bdid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration' and rtb_list[3] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(bdid_list)), str(choice(chtype_list)),
                                   str(choice(duration_list)), str(choice(playtype_list)), d_type, d_type)
            else:
                raise Exception("fbdid has no such table!")
    elif rtb_list[0] == 'bid':
        if len(rtb_list) == 1:
            r_sql = "select city, %s from %s where date = %s and bid = \'%s\' group by city, %s order by " \
                    "city, %s desc;" % (d_type, tb, date_data, bid_val, d_type, d_type)
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and playtype = \'%s\' " \
                        "group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                               str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and chtype = \'%s\' " \
                        "group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                               str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and isfull = \'%s\' group " \
                        "by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and " \
                        "duration = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(duration_list)), d_type, d_type)
        else:
                raise Exception("bid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'playtype' and rtb_list[2] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and isfull = \'%s\' and playtype = \'%s\' " \
                        "group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val, choice(isfull_list),
                                                               str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull' and rtb_list[2] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and isfull = \'%s\' and playtype = \'%s\' " \
                        "group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val, choice(isfull_list),
                                                               str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull' and rtb_list[2] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and duration = \'%s\' and isfull = \'%s\' " \
                        "group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val, choice(duration_list),
                                                               str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'playtype' and rtb_list[2] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and duration = \'%s\' and playtype = \'%s\' " \
                        "group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val, choice(duration_list),
                                                               str(choice(playtype_list)), d_type, d_type)
    elif rtb_list[0] == 'fbdid':
        if len(rtb_list) == 1:
            r_sql = "select city, %s from %s where date = %s and bid = %s and bdid = %s group by " \
                    "city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val, str(choice(bdid_list)), d_type, d_type)
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\'and " \
                        "chtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(bdid_list)),
                                                                           str(choice(chtype_list)), d_type, d_type)
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(bdid_list)),
                                                                             str(choice(playtype_list)), d_type, d_type)
            if rtb_list[1] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\'and " \
                        "chtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(bdid_list)),
                                                                           str(choice(duration_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "isfull = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(bdid_list)),
                                                                             str(choice(isfull_list)), d_type, d_type)
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb,
                                                                                             date_data,
                                                                                                     bid_val,
                                                                                             str(choice(sid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(playtype_list)), d_type, d_type)
            else:
                raise Exception("fbdid has no such table!")
    elif rtb_list[0] == 'vid':
        if len(rtb_list) == 1:
            r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and vid = \'%s\' group by " \
                    "city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val, str(choice(vid_list)), d_type, d_type)
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(vid_list)), str(choice(chtype_list)), d_type, d_type)
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(vid_list)), str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "isfull = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(vid_list)), str(choice(isfull_list)), d_type, d_type)
            else:
                raise Exception("pid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(vid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(vid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' group by city, %s order by city, %s desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(vid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                     duration_list)), d_type, d_type)
            else:
                raise Exception("pid has no such table!")
        elif len(rtb_list) == 4:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull' and rtb_list[3] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' and playtype = \'%s\' group by city, %s order by city, %s desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(vid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), str(choice(playtype_list)), d_type, d_type)
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration' and rtb_list[3] == 'playtype':
                r_sql = "select city, %s from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' and playtype = \'%s\' group by city, %s  order by city, %s  " \
                        "desc;" % (d_type, tb, date_data, str(choice(bid_list)), str(choice(vid_list)), str(choice(chtype_list)),
                                   str(choice(duration_list)), str(choice(playtype_list)), d_type, d_type)
            else:
                raise Exception("pid has no such table!")

    return r_sql


def data_r_sql(tb, rtb_list, date_data, t_type, d_type, pid_list, cid_list, sid_list, bdid_list, vid_list):
    """
    copy the data for uv
    :param tb:
    :param rtb_list:
    :param date_data:
    :param t_type:
    :return:
    """
    r_sql = ""
    if t_type in ['hour', 'day']:
        rtb_list.remove(t_type)
    if d_type in ['vv', 'pt', 'dau']:
        rtb_list.remove(d_type)
    if 'city' in rtb_list:
        rtb_list.remove('city')

    bid_val = str(choice(bid_list))
    if bid_val == '1':
        cid_list = [83, 84, 85, 86, 87, 88, 97, 98]
    else:
        cid_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    if rtb_list[0] == 'cid':
        if len(rtb_list) == 1:
            r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\' group by " \
                    "city order by city desc;" % (d_type, tb, date_data, bid_val, str(choice(cid_list)))
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                               str(choice(cid_list)), str(choice(chtype_list)))
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                    "playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                         str(choice(cid_list)), str(choice(playtype_list)))
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                    "isfull =\'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                       str(choice(cid_list)),
                                                                       str(choice(isfull_list)))
            elif rtb_list[1] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                    "duration =\'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                       str(choice(cid_list)),
                                                                       str(choice(duration_list)))
            else:
                raise Exception("cid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(cid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                     playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(cid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                    r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\'and " \
                            "chtype = \'%s\' and duration = \'%s\'group by city order by city desc;" % (d_type, tb,
                                                                                                        date_data,
                                                                                                 bid_val,
                                                                                                 str(choice(cid_list)),
                                                                                                 str(choice(chtype_list)),
                                                                                                 str(choice(
                                                                                                     duration_list)))
            elif rtb_list[1] == 'isfull' and rtb_list[2] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(cid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)))
            else:
                raise Exception("cid has no such table!")
        elif len(rtb_list) == 4:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull' and rtb_list[3] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' and playtype = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(cid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), str(choice(playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration' and rtb_list[3] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and cid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' and playtype = \'%s\' group by city order by city " \
                        "desc;" % (d_type, tb, date_data, bid_val, str(choice(cid_list)), str(choice(chtype_list)),
                                   str(choice(duration_list)), str(choice(playtype_list)))
            else:
                raise Exception("cid has no such table!")
    elif rtb_list[0] == 'pid':
        if len(rtb_list) == 1:
            r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and pid = \'%s\' group by " \
                    "city order by city desc;" % (d_type, tb, date_data, bid_val, str(choice(pid_list)))
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(pid_list)), str(choice(chtype_list)))
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(pid_list)), str(choice(playtype_list)))
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, str(choice(bid_list)),
                                                                           str(choice(pid_list)), str(choice(isfull_list)))
            elif rtb_list[1] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "duration = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, str(choice(bid_list)),
                                                                           str(choice(pid_list)), str(choice(duration_list)))
            else:
                raise Exception("pid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(pid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(pid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' group by city order by city desc;" % (d_type, tb,
                                                                                                     date_data,
                                                                                                     bid_val,
                                                                                             str(choice(pid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                     duration_list)))
            elif rtb_list[1] == 'isfull' and rtb_list[2] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(pid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)))
            else:
                raise Exception("pid has no such table!")
        elif len(rtb_list) == 4:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull' and rtb_list[3] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and pid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' and playtype = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(pid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), str(choice(playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration' and rtb_list[3] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\'and pid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' and playtype = \'%s\' group by city order by city " \
                        "desc;" % (d_type, tb, date_data, bid_val, str(choice(pid_list)),str(choice(chtype_list)),
                                   str(choice(duration_list)), str(choice(playtype_list)))
            else:
                raise Exception("pid has no such table!")

    elif rtb_list[0] == 'sid':
        if len(rtb_list) == 1:
            r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\' group by " \
                    "city order by city desc;" % (d_type, tb, date_data, bid_val, str(choice(sid_list)))
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(sid_list)), str(choice(chtype_list)))
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\'and " \
                        "playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(sid_list)),
                                                                             str(choice(playtype_list)))
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(sid_list)),
                                                                           str(choice(isfull_list)))
            elif rtb_list[1] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "duration = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(sid_list)),
                                                                           str(choice(duration_list)))
            else:
                raise Exception("sid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(sid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                     playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(sid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(sid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                                 str(choice(
                                                                                                     duration_list)))
            elif rtb_list[1] == 'isfull' and rtb_list[2] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(sid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)))
            else:
                raise Exception("sid has no such table!")
        elif len(rtb_list) == 4:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull' and rtb_list[3] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' and playtype = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(sid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), str(choice(playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration' and rtb_list[3] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and sid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' and playtype = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(sid_list)), str(choice(chtype_list)),
                           str(choice(duration_list)), str(choice(playtype_list)))
            else:
                raise Exception("sid has no such table!")

    elif rtb_list[0] == 'bdid':
        if len(rtb_list) == 1:
            r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' group by " \
                    "city order by city desc;" % (d_type, tb, date_data, bid_val, str(choice(bdid_list)))
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(bdid_list)),
                                                                           str(choice(chtype_list)))
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\'and bdid = \'%s\' and " \
                        "playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(bdid_list)),
                                                                             str(choice(playtype_list)))
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\'and " \
                        "isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(bdid_list)),
                                                                           str(choice(isfull_list)))
            elif rtb_list[1] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\'and " \
                        "duration = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(bdid_list)),
                                                                           str(choice(duration_list)))
            else:
                raise Exception("bdid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(bdid_list)),
                                                                                                 str(choice(chtype_list)),
                                                                                                 str(choice(
                                                                                                     playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                 r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(bdid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\'and bdid = \'%s\'and " \
                        "chtype = \'%s\' and duration = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(bdid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                     duration_list)))
            elif rtb_list[1] == 'isfull' and rtb_list[2] == 'chtype':
                 r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(bdid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)))
            else:
                raise Exception("fbdid has no such table!")
        elif len(rtb_list) == 4:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull' and rtb_list[3] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' and playtype = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(bdid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), str(choice(playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration' and rtb_list[3] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' and playtype = \'%s\' group by city order by city " \
                        "desc;" % (d_type, tb, date_data, bid_val, str(choice(bdid_list)), str(choice(chtype_list)),
                                   str(choice(duration_list)), str(choice(playtype_list)))
            else:
                raise Exception("fbdid has no such table!")
    elif rtb_list[0] == 'bid':
        if len(rtb_list) == 1:
            r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' group by city order by " \
                    "city desc;" % (d_type, tb, date_data, bid_val)
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and playtype = \'%s\' " \
                        "group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                               str(choice(playtype_list)))
            elif rtb_list[1] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and chtype = \'%s\' " \
                        "group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                               str(choice(chtype_list)))
            elif rtb_list[1] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and duration = \'%s\' " \
                        "group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                               str(choice(duration_list)))
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and isfull = \'%s\' " \
                        "group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                               str(choice(isfull_list)))
        else:
                raise Exception("bid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(chtype_list)), str(choice(playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(chtype_list)), str(choice(isfull_list)))
            elif rtb_list[1] == 'isfull' and rtb_list[2] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and " \
                        "isfull = \'%s\' and chtype = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(isfull_list)), str(choice(chtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(chtype_list)), str(choice(duration_list)))
            if rtb_list[1] == 'playtype' and rtb_list[2] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and " \
                        "playtype = \'%s\' and duration = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(playtype_list)), str(choice(duration_list)))
    elif rtb_list[0] == 'fbdid':
        if len(rtb_list) == 1:
            r_sql = "select city,sum(%s) from %s where date = %s and bid = %s and bdid = %s group by " \
                    "city order by city desc;" % (d_type, tb, date_data, bid_val, str(choice(bdid_list)))
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\'and " \
                        "chtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(bdid_list)),
                                                                           str(choice(chtype_list)))
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(bdid_list)),
                                                                             str(choice(playtype_list)))
            elif rtb_list[1] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "duration = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(bdid_list)),
                                                                             str(choice(duration_list)))
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(bdid_list)),
                                                                             str(choice(isfull_list)))
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and bdid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city order by city desc;" % (d_type, tb,
                                                                                             date_data,
                                                                                                     bid_val,
                                                                                             str(choice(bdid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(playtype_list)))
            else:
                raise Exception("fbdid has no such table!")
    elif rtb_list[0] == 'vid':
        if len(rtb_list) == 1:
            r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and vid = \'%s\' group by " \
                    "city order by city desc;" % (d_type, tb, date_data, bid_val, str(choice(vid_list)))
        elif len(rtb_list) == 2:
            if rtb_list[1] == 'chtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(vid_list)), str(choice(chtype_list)))
            elif rtb_list[1] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                             str(choice(vid_list)), str(choice(playtype_list)))
            elif rtb_list[1] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(vid_list)), str(choice(isfull_list)))
            elif rtb_list[1] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "duration = \'%s\' group by city order by city desc;" % (d_type, tb, date_data, bid_val,
                                                                           str(choice(vid_list)), str(choice(duration_list)))
            else:
                raise Exception("pid has no such table!")
        elif len(rtb_list) == 3:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' and playtype = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(vid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                   bid_val,
                                                                                           str(choice(vid_list)),
                                                                                           str(choice(chtype_list)),
                                                                                           str(choice(isfull_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' group by city order by city desc;" % (d_type, tb, date_data,
                                                                                                     bid_val,
                                                                                             str(choice(vid_list)),
                                                                                             str(choice(chtype_list)),
                                                                                             str(choice(
                                                                                                     duration_list)))
            else:
                raise Exception("pid has no such table!")
        elif len(rtb_list) == 4:
            if rtb_list[1] == 'chtype' and rtb_list[2] == 'isfull' and rtb_list[3] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' and isfull = \'%s\' and playtype = \'%s\' group by city order by city desc;" \
                        % (d_type, tb, date_data, bid_val, str(choice(vid_list)), str(choice(chtype_list)),
                           str(choice(isfull_list)), str(choice(playtype_list)))
            elif rtb_list[1] == 'chtype' and rtb_list[2] == 'duration' and rtb_list[3] == 'playtype':
                r_sql = "select city, sum(%s) from %s where date = %s and bid = \'%s\' and vid = \'%s\' and " \
                        "chtype = \'%s\' and duration = \'%s\' and playtype = \'%s\' group by city order by city " \
                        "desc;" % (d_type, tb, date_data, str(choice(bid_list)), str(choice(vid_list)), str(choice(chtype_list)),
                                   str(choice(duration_list)), str(choice(playtype_list)))
            else:
                raise Exception("pid has no such table!")

    return r_sql


def result_data(ip1, port1, user1, password1, dbname1, tb, rtb_list, date_data):
    """
    :param ip1:
    :param port1:
    :param user1:
    :param password1:
    :param dbname1:
    :param tb:
    :return:
    """
    #pid_list, cid_list, sid_list, bdid_list, vid_list = param_get(ip1, port1, user1, password1, "dm_result")
    vid_list = param_get(ip1, port1, user1, password1, "dm_result")
    pid_list = [308921, 308710, 312289, 310102, 309556, 312733, 312289, 312727, 308734, 314044]
    cid_list = []
    sid_list = [80795, 55466, 51680, 50632, 818542, 55507, 55466, 19033, 71146, 82204, 54764, 67496]
    bdid_list = [100001273, 100004660, 100002990, 100000826, 100003295, 100002989, 100004654, 100001668,
                 100000966, 100000966, 100000937, 100004343, 100004644]

    r_sql = ""
    if 'hour' in rtb_list:
        if 'uv' in rtb_list:
            r_sql = data_uv_sql(tb, rtb_list, date_data, 'hour', 'uv', pid_list, cid_list, sid_list, bdid_list, vid_list)
        elif 'vv' in rtb_list:
            r_sql = data_uv_sql(tb, rtb_list, date_data, 'hour', 'vv', pid_list, cid_list, sid_list, bdid_list, vid_list)
        elif 'pt' in rtb_list:
            r_sql = data_r_sql(tb, rtb_list, date_data, 'hour', 'pt', pid_list, cid_list, sid_list, bdid_list, vid_list)
        elif 'dau' in rtb_list:
            r_sql = data_uv_sql(tb, rtb_list, date_data, 'hour', 'dau', pid_list, cid_list, sid_list, bdid_list, vid_list)
    else:
        if 'uv' in rtb_list:
            r_sql = data_uv_sql(tb, rtb_list, date_data, 'day', 'uv', pid_list, cid_list, sid_list, bdid_list, vid_list)
        elif 'vv' in rtb_list:
            r_sql = data_uv_sql(tb, rtb_list, date_data, 'day', 'vv', pid_list, cid_list, sid_list, bdid_list, vid_list)
        elif 'pt' in rtb_list:
            r_sql = data_r_sql(tb, rtb_list, date_data, 'day', 'pt', pid_list, cid_list, sid_list, bdid_list, vid_list)
        elif 'dau' in rtb_list:
            r_sql = data_uv_sql(tb, rtb_list, date_data, 'day', 'dau', pid_list, cid_list, sid_list, bdid_list, vid_list)

    r_data = select_mode(ip1, port1, user1, password1, dbname1, r_sql, tb)
    return r_data, r_sql


def result_dtable(tb):
    """
    :param tb:
    :return:t_list:table field
    """
    ap_list = ['country', 'province']
    src_list = [x for x in tb.split('_')]
    c_type = src_list[len(src_list)-1]
    if 'ap' in src_list:
        c = src_list.index('ap')
        src_list.pop(c)
        for j in ap_list:
            src_list.append(j)
    return src_list, c_type


def data_f_sql(tb, sql, d_date, s_first):
    """
    Get the fact database sql
    :param tb:
    :param sql:
    :param d_date:
    :return:
    """
    f_sql = ""
    and_count = sql.count('and')
    bid_val = sql.split("and")[1].split("bid")[1].split("=")[1]
    if 'cid' in tb:
        if and_count == 2:
            cid_val = sql.split("and")[2].split(";")[0].split("cid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' " \
                        "and cid = %s group by city order by city desc;" \
                        % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]), cid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and cid = %s " \
                        "group by city order by city desc;" \
                        % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]), cid_val)
        elif and_count == 3:
            cid_val = sql.split("and")[2].split("cid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                            "hour = \'%s\' and cid = %s and chtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]),
                               cid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                            "cid = %s and chtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]),
                               cid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and cid = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]),
                               cid_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and cid = %s " \
                            "and playtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                             str(d_date[7:-1]), cid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and cid = %s and isfull = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                             str(d_date[7:9]), str(d_date[9:-1]), cid_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and isfull = %s group " \
                            "by city order by city desc;" % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                             str(d_date[7:-1]), cid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and cid = %s and vts = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                             str(d_date[7:9]), str(d_date[9:-1]), cid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and vts = %s group " \
                            "by city order by city desc;" % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                             str(d_date[7:-1]), cid_val, duration_val)
        elif and_count == 4:
            cid_val = sql.split("and")[2].split("cid")[1].split("=")[1]
            if 'chtype' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                playtype_val = sql.split("and")[4].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                            "hour = \'%s\' and cid = %s and chtype = %s and playtype = %s group by " \
                            "city order by city desc;" % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                                      str(d_date[7:9]), str(d_date[9:-1]), cid_val,
                                                                      chtype_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and chtype = %s and playtype = " \
                            "%s group by city order by city desc;" % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                                      str(d_date[7:-1]), cid_val, chtype_val,
                                                                          playtype_val)
            elif 'chtype' in tb and 'isfull' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and cid = %s and chtype = %s " \
                            "and isfull = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]),
                               cid_val, chtype_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and chtype = %s and isfull = " \
                            "%s group by city order by city desc;" % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                                      str(d_date[7:-1]), cid_val, chtype_val,
                                                                          isfull_val)
            elif 'chtype' in tb and 'duration' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and cid = %s and chtype = %s and " \
                            "vts = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                                      str(d_date[7:9]), str(d_date[9:-1]), cid_val,
                                                                          chtype_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and chtype = %s and vts = " \
                            "%s group by city order by city desc;" % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                                      str(d_date[7:-1]), cid_val, chtype_val,
                                                                          duration_val)
        elif and_count == 5:
            cid_val = sql.split("and")[2].split("cid")[1].split("=")[1]
            if 'chtype' in tb and 'isfull' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and cid = %s and chtype = %s and isfull = " \
                            "%s and playtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]),
                               cid_val, chtype_val, isfull_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and chtype = %s and isfull = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, str(d_date[1:5]),
                                                                                        str(d_date[5:7]), str(d_date[7:-1]),
                                                                                        cid_val, chtype_val,
                                                                                        isfull_val, playtype_val)
            elif 'chtype' in tb and 'duration' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and cid = %s and chtype " \
                            "= %s and vts = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]), cid_val,
                               chtype_val, duration_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and chtype = %s and vts = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, str(d_date[1:5]),
                                                                                        d_date[5:7], d_date[7:-1],
                                                                                        cid_val, chtype_val,
                                                                                        duration_val, playtype_val)
    elif 'pid' in tb:
        if and_count == 2:
            pid_val = sql.split("and")[2].split(";")[0].split("pid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], pid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and pid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], pid_val)

        elif and_count == 3:
            pid_val = sql.split("and")[2].split("pid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], pid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          pid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and playtype " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], pid_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and playtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          pid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and isfull " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], pid_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and isfull = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          pid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and vts " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], pid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and vts = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          pid_val, duration_val)
        elif and_count == 4:
            pid_val = sql.split("and")[2].split("pid")[1].split("=")[1]
            if 'chtype' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                playtype_val = sql.split("and")[4].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype " \
                            "= %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], pid_val, chtype_val,
                               playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s and playtype = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], pid_val, chtype_val,
                                                                      playtype_val)
            elif 'chtype' in tb and 'isfull' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype " \
                            "= %s and isfull = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], pid_val,
                                                                      chtype_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s and isfull = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], pid_val, chtype_val,
                                                                      isfull_val)
            elif 'chtype' in tb and 'duration' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype " \
                            "= %s and vts = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], pid_val, chtype_val,
                                                                      duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s and vts = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], pid_val, chtype_val,
                                                                      duration_val)
        elif and_count == 5:
            pid_val = sql.split("and")[2].split("pid")[1].split("=")[1]
            if 'chtype' in tb and 'isfull' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype " \
                            "= %s and isfull = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], pid_val,
                                                                                        chtype_val, isfull_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s and isfull " \
                            "= %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                 d_date[5:7], d_date[7:-1],
                                                                                 pid_val, chtype_val,
                                                                                 isfull_val, playtype_val)
            elif 'chtype' in tb and 'duration' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype" \
                            " = %s and vts = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], pid_val,
                                                                                        chtype_val, duration_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s and vts = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:-1],
                                                                                        pid_val, chtype_val,
                                                                                        duration_val,
                                                                                        playtype_val)
    elif 'sid' in tb:
        if and_count == 2:
            sid_val = sql.split("and")[2].split(";")[0].split("sid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' " \
                        "and seriesid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], sid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and seriesid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], sid_val)
        elif and_count == 3:
            sid_val = sql.split("and")[2].split("sid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and seriesid = %s and chtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], sid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\'" \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          sid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s and playtype " \
                            "= %s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                   d_date[7:9], d_date[9:-1], sid_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and seriesid " \
                            "= %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                     sid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and seriesid = %s and isfull = %s " \
                            "group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                   d_date[7:9], d_date[9:-1], sid_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and isfull = %s group by city " \
                            "order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                     sid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and seriesid = %s and vts = %s " \
                            "group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                   d_date[7:9], d_date[9:-1], sid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and vts = %s group by city " \
                            "order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                     sid_val, duration_val)
        elif and_count == 4:
            sid_val = sql.split("and")[2].split("sid")[1].split("=")[1]
            if 'chtype' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                playtype_val = sql.split("and")[4].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\'" \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s " \
                            "and chtype = %s and playtype = %s group by city order by city desc;"\
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], sid_val,
                                                                      chtype_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s and playtype = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], sid_val, chtype_val,
                                                                      playtype_val)
            elif 'chtype' in tb and 'isfull' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s and chtype " \
                            "= %s and isfull = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], sid_val,
                                                                      chtype_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s and isfull = " \
                            "%s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], sid_val, chtype_val,
                                                                      isfull_val)
            elif 'chtype' in tb and 'duration' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s and " \
                            "chtype = %s and vts = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], sid_val,
                                                                      chtype_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s and vts = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], sid_val, chtype_val,
                                                                      duration_val)
        elif and_count == 5:
            sid_val = sql.split("and")[2].split("sid")[1].split("=")[1]
            if 'chtype' in tb and 'isfull' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s and chtype " \
                            "= %s and isfull = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], sid_val,
                                                                                        chtype_val, isfull_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s and isfull = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                        d_date[5:7],
                                                                                        d_date[7:-1],
                                                                                        sid_val, chtype_val,
                                                                                        isfull_val,
                                                                                        playtype_val)
            elif 'chtype' in tb and 'duration' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s and chtype " \
                            "= %s and vts = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], sid_val,
                                                                                        chtype_val, duration_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s and vts = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                        d_date[5:7],
                                                                                        d_date[7:-1],
                                                                                        sid_val,
                                                                                        chtype_val,
                                                                                        duration_val,
                                                                                        playtype_val)
    elif 'bdid' in tb:
        if and_count == 2:
            bdid_val = sql.split("and")[2].split(";")[0].split("bdid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                        "= \'%s\' and bdid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], bdid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and bdid = %s group by city order by city " \
                        "desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], bdid_val)
        elif and_count == 3:
            bdid_val = sql.split("and")[2].split("bdid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:9], d_date[9:-1], bdid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s group " \
                            "by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:-1], bdid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and " \
                            "playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:9], d_date[9:-1], bdid_val,
                                                             playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and playtype = %s group " \
                            "by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:-1], bdid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and " \
                            "isfull = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:9], d_date[9:-1], bdid_val,
                                                             isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and isfull = %s group " \
                            "by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:-1], bdid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and " \
                            "vts = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:9], d_date[9:-1], bdid_val,
                                                             duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and vts = %s group " \
                            "by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:-1], bdid_val, duration_val)
        elif and_count == 4:
            bdid_val = sql.split("and")[2].split("bdid")[1].split("=")[1]
            if 'chtype' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                playtype_val = sql.split("and")[4].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and bdid = %s and chtype = %s and " \
                            "playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                 d_date[5:7], d_date[7:9],
                                                                                 d_date[9:-1], bdid_val,
                                                                                 chtype_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s and " \
                            "playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                 d_date[5:7], d_date[7:-1],
                                                                                 bdid_val, chtype_val,
                                                                                 playtype_val)
            elif 'chtype' in tb and 'isfull' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s and isfull = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                               d_date[5:7], d_date[7:9],
                                                                               d_date[9:-1], bdid_val, chtype_val,
                                                                               isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s and " \
                            "isfull = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                             bdid_val, chtype_val, isfull_val)
            elif 'chtype' in tb and 'duration' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s and vts = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], bdid_val,
                                                                      chtype_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = %s and bdid = %s and chtype = %s and vts = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], bdid_val, chtype_val,
                                                                      duration_val)
        elif and_count == 5:
            bdid_val = sql.split("and")[2].split("bdid")[1].split("=")[1]
            if 'chtype' in tb and 'isfull' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s and isfull = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], bdid_val,
                                                                                        chtype_val, isfull_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s and isfull = " \
                            "%s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7],
                                                                                        d_date[7:-1],
                                                                                        bdid_val, chtype_val,
                                                                                        isfull_val,
                                                                                        playtype_val)
            elif 'chtype' in tb and 'duration' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = %s " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s and vts = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], bdid_val,
                                                                                        chtype_val, duration_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s and vts = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                        d_date[5:7],
                                                                                        d_date[7:-1],
                                                                                        bdid_val,
                                                                                        chtype_val,
                                                                                        duration_val,
                                                                                        playtype_val)
    elif 'bid' in tb:
        if and_count == 1:
            bid_val_r = sql.split("and")[1].split("bid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' group by city order by city desc;" % (bid_val_r,
                                                                                                     d_date[1:5],
                                                                                                     d_date[5:7],
                                                                                                     d_date[7:9],
                                                                                                     d_date[9:-1])
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' group by city order by city desc;" % (bid_val_r,
                                                                                       d_date[1:5], d_date[5:7],
                                                                                       d_date[7:-1])
        elif and_count == 2:
            if 'playtype' in tb:
                playtype_val = sql.split("and")[2].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and playtype = %s group by city order " \
                        "by city desc; " \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and playtype = %s group by city order by city " \
                        "desc; " % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], playtype_val)
            elif 'chtype' in tb:
                chtype_val = sql.split("and")[2].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and chtype = %s group by city order " \
                        "by city desc; " \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and chtype = %s group by city order by city " \
                        "desc; " % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], chtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[2].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and isfull = %s group by city order " \
                        "by city desc; " \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and isfull = %s group by city order by city " \
                        "desc; " % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[2].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and vts = %s group by city order " \
                        "by city desc; " \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and vts = %s group by city order by city " \
                        "desc; " % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], duration_val)
    elif 'fbdid' in tb:
        if and_count == 2:
            bdid_val = sql.split("and")[2].split(";")[0].split("bdid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s group by city order " \
                        "by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], bdid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and bdid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], bdid_val)
        elif and_count == 3:
            bdid_val = sql.split("and")[2].split("bdid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s' group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], bdid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          bdid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid " \
                            "= %s' and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], bdid_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and playtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          bdid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and bdid = %s and isfull = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], bdid_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and isfull = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          bdid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and bdid = %s and vts = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], bdid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and vts = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          bdid_val, duration_val)
    elif 'vid' in tb:
        if and_count == 2:
            vid_val = sql.split("and")[2].split(";")[0].split("vid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s group by city order " \
                        "by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], vid_val)
            else:
                f_sql = "where bid = %s and year = %s and " \
                        "month = %s and day = %s and vid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], vid_val)

        elif and_count == 3:
            vid_val = sql.split("and")[2].split("vid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], vid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          vid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and playtype " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], vid_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and playtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          vid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and isfull " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], vid_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and isfull = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          vid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and vts " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], vid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and vts = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          vid_val, duration_val)
        elif and_count == 4:
            vid_val = sql.split("and")[2].split("vid")[1].split("=")[1]
            if 'chtype' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                playtype_val = sql.split("and")[4].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype " \
                            "= %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], vid_val, chtype_val,
                                                                      playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s and playtype = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], vid_val, chtype_val,
                                                                      playtype_val)
            elif 'chtype' in tb and 'isfull' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype = %s and isfull = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], vid_val,
                                                                      chtype_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s and isfull = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], vid_val, chtype_val,
                                                                      isfull_val)
            elif 'chtype' in tb and 'duration' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype " \
                            "= %s and vts = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], vid_val, chtype_val,
                                                                      duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s and vts = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], vid_val, chtype_val,
                                                                      duration_val)
        elif and_count == 5:
            vid_val = sql.split("and")[2].split("vid")[1].split("=")[1]
            if 'chtype' in tb and 'isfull' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype " \
                            "= %s and isfull = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], vid_val,
                                                                                        chtype_val, isfull_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s and isfull " \
                            "= %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                 d_date[5:7], d_date[7:-1],
                                                                                 vid_val, chtype_val,
                                                                                 isfull_val, playtype_val)
            elif 'chtype' in tb and 'duration' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype " \
                            "= %s and vts = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], vid_val,
                                                                                        chtype_val, duration_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s and vts = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:-1],
                                                                                        vid_val, chtype_val,
                                                                                        duration_val,
                                                                                        playtype_val)

    f_s = s_first + " " + f_sql
    return f_s


def data_pt_sql(tb, sql, d_date, s_first):
    """
    Get the fact database sql
    :param tb:
    :param sql:
    :param d_date:
    :return:
    """
    f_sql = ""
    and_count = sql.count('and')
    bid_val = sql.split("and")[1].split("bid")[1].split("=")[1]
    if 'cid' in tb:
        if and_count == 2:
            cid_val = sql.split("and")[2].split(";")[0].split("cid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' " \
                        "and cid = %s group by city order by city desc;" \
                        % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]), cid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and cid = %s " \
                        "group by city order by city desc;" \
                        % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]), cid_val)
        elif and_count == 3:
            cid_val = sql.split("and")[2].split("cid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                            "hour = \'%s\' and cid = %s and chtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]),
                               cid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                            "cid = %s and chtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]),
                               cid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and cid = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]),
                               cid_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and cid = %s " \
                            "and playtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                             str(d_date[7:-1]), cid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and cid = %s and isfull = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                             str(d_date[7:9]), str(d_date[9:-1]), cid_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and isfull = %s group " \
                            "by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                             str(d_date[7:-1]), cid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and cid = %s and vts = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                             str(d_date[7:9]), str(d_date[9:-1]), cid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and vts = %s group " \
                            "by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                             str(d_date[7:-1]), cid_val, duration_val)
        elif and_count == 4:
            cid_val = sql.split("and")[2].split("cid")[1].split("=")[1]
            if 'chtype' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                playtype_val = sql.split("and")[4].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                            "hour = \'%s\' and cid = %s and chtype = %s and playtype = %s group by " \
                            "city order by city desc;" % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                                      str(d_date[7:9]), str(d_date[9:-1]), cid_val,
                                                                      chtype_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and chtype = %s and playtype = " \
                            "%s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                                      str(d_date[7:-1]), cid_val, chtype_val,
                                                                          playtype_val)
            elif 'chtype' in tb and 'isfull' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and cid = %s and chtype = %s " \
                            "and isfull = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]),
                               cid_val, chtype_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and chtype = %s and isfull = " \
                            "%s group by city order by city desc;" % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                                      str(d_date[7:-1]), cid_val, chtype_val,
                                                                          isfull_val)
            elif 'chtype' in tb and 'duration' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and cid = %s and chtype = %s and " \
                            "vts = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                                      str(d_date[7:9]), str(d_date[9:-1]), cid_val,
                                                                          chtype_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and chtype = %s and vts = " \
                            "%s group by city order by city desc;" % (bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                                                      str(d_date[7:-1]), cid_val, chtype_val,
                                                                          duration_val)
        elif and_count == 5:
            cid_val = sql.split("and")[2].split("cid")[1].split("=")[1]
            if 'chtype' in tb and 'isfull' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and cid = %s and chtype = %s and isfull = " \
                            "%s and playtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]),
                               cid_val, chtype_val, isfull_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and chtype = %s and isfull = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, str(d_date[1:5]),
                                                                                        str(d_date[5:7]), str(d_date[7:-1]),
                                                                                        cid_val, chtype_val,
                                                                                        isfull_val, playtype_val)
            elif 'chtype' in tb and 'duration' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and cid = %s and chtype " \
                            "= %s and vts = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]), cid_val,
                               chtype_val, duration_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and cid = %s and chtype = %s and vts = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, str(d_date[1:5]),
                                                                                        d_date[5:7], d_date[7:-1],
                                                                                        cid_val, chtype_val,
                                                                                        duration_val, playtype_val)
    elif 'pid' in tb:
        if and_count == 2:
            pid_val = sql.split("and")[2].split(";")[0].split("pid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s group by city " \
                        "order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], pid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and pid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], pid_val)

        elif and_count == 3:
            pid_val = sql.split("and")[2].split("pid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], pid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s group by " \
                            "city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          pid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and playtype " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], pid_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and playtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          pid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and isfull " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], pid_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and isfull = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          pid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and vts " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], pid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and vts = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          pid_val, duration_val)
        elif and_count == 4:
            pid_val = sql.split("and")[2].split("pid")[1].split("=")[1]
            if 'chtype' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                playtype_val = sql.split("and")[4].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype " \
                            "= %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], pid_val, chtype_val,
                               playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s and playtype = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], pid_val, chtype_val,
                                                                      playtype_val)
            elif 'chtype' in tb and 'isfull' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype " \
                            "= %s and isfull = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], pid_val,
                                                                      chtype_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s and isfull = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], pid_val, chtype_val,
                                                                      isfull_val)
            elif 'chtype' in tb and 'duration' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype " \
                            "= %s and vts = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], pid_val, chtype_val,
                                                                      duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s and vts = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], pid_val, chtype_val,
                                                                      duration_val)
        elif and_count == 5:
            pid_val = sql.split("and")[2].split("pid")[1].split("=")[1]
            if 'chtype' in tb and 'isfull' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype " \
                            "= %s and isfull = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], pid_val,
                                                                                        chtype_val, isfull_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s and isfull " \
                            "= %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                 d_date[5:7], d_date[7:-1],
                                                                                 pid_val, chtype_val,
                                                                                 isfull_val, playtype_val)
            elif 'chtype' in tb and 'duration' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s and chtype" \
                            " = %s and vts = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], pid_val,
                                                                                        chtype_val, duration_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and pid = %s and chtype = %s and vts = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:-1],
                                                                                        pid_val, chtype_val,
                                                                                        duration_val,
                                                                                        playtype_val)
    elif 'sid' in tb:
        if and_count == 2:
            sid_val = sql.split("and")[2].split(";")[0].split("sid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' " \
                        "and seriesid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], sid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and seriesid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], sid_val)
        elif and_count == 3:
            sid_val = sql.split("and")[2].split("sid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and seriesid = %s and chtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], sid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\'" \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          sid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s and playtype " \
                            "= %s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                   d_date[7:9], d_date[9:-1], sid_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and seriesid " \
                            "= %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                     sid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and seriesid = %s and isfull = %s " \
                            "group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                   d_date[7:9], d_date[9:-1], sid_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and isfull = %s group by city " \
                            "order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                     sid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and seriesid = %s and vts = %s " \
                            "group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                   d_date[7:9], d_date[9:-1], sid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and vts = %s group by city, " \
                            "playtime order by city, playtime desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                     sid_val, duration_val)
        elif and_count == 4:
            sid_val = sql.split("and")[2].split("sid")[1].split("=")[1]
            if 'chtype' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                playtype_val = sql.split("and")[4].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\'" \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s " \
                            "and chtype = %s and playtype = %s group by city order by city desc;"\
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], sid_val,
                                                                      chtype_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s and playtype = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], sid_val, chtype_val,
                                                                      playtype_val)
            elif 'chtype' in tb and 'isfull' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s and chtype " \
                            "= %s and isfull = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], sid_val,
                                                                      chtype_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s and isfull = " \
                            "%s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], sid_val, chtype_val,
                                                                      isfull_val)
            elif 'chtype' in tb and 'duration' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s and " \
                            "chtype = %s and vts = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], sid_val,
                                                                      chtype_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s and vts = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], sid_val, chtype_val,
                                                                      duration_val)
        elif and_count == 5:
            sid_val = sql.split("and")[2].split("sid")[1].split("=")[1]
            if 'chtype' in tb and 'isfull' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s and chtype " \
                            "= %s and isfull = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], sid_val,
                                                                                        chtype_val, isfull_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s and isfull = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                        d_date[5:7],
                                                                                        d_date[7:-1],
                                                                                        sid_val, chtype_val,
                                                                                        isfull_val,
                                                                                        playtype_val)
            elif 'chtype' in tb and 'duration' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and seriesid = %s and chtype " \
                            "= %s and vts = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], sid_val,
                                                                                        chtype_val, duration_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and seriesid = %s and chtype = %s and vts = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                        d_date[5:7],
                                                                                        d_date[7:-1],
                                                                                        sid_val,
                                                                                        chtype_val,
                                                                                        duration_val,
                                                                                        playtype_val)
    elif 'bdid' in tb:
        if and_count == 2:
            bdid_val = sql.split("and")[2].split(";")[0].split("bdid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                        "= \'%s\' and bdid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], bdid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and bdid = %s group by city order by city " \
                        "desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], bdid_val)
        elif and_count == 3:
            bdid_val = sql.split("and")[2].split("bdid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:9], d_date[9:-1], bdid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s group " \
                            "by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:-1], bdid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and " \
                            "playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:9], d_date[9:-1], bdid_val,
                                                             playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and playtype = %s group " \
                            "by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:-1], bdid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and " \
                            "isfull = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:9], d_date[9:-1], bdid_val,
                                                             isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and isfull = %s group " \
                            "by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:-1], bdid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and " \
                            "vts = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:9], d_date[9:-1], bdid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and vts = %s group " \
                            "by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                             d_date[7:-1], bdid_val, duration_val)
        elif and_count == 4:
            bdid_val = sql.split("and")[2].split("bdid")[1].split("=")[1]
            if 'chtype' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                playtype_val = sql.split("and")[4].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and bdid = %s and chtype = %s and " \
                            "playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                 d_date[5:7], d_date[7:9],
                                                                                 d_date[9:-1], bdid_val,
                                                                                 chtype_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s and " \
                            "playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                 d_date[5:7], d_date[7:-1],
                                                                                 bdid_val, chtype_val,
                                                                                 playtype_val)
            elif 'chtype' in tb and 'isfull' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s and isfull = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                               d_date[5:7], d_date[7:9],
                                                                               d_date[9:-1], bdid_val, chtype_val,
                                                                               isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s and " \
                            "isfull = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                             bdid_val, chtype_val, isfull_val)
            elif 'chtype' in tb and 'duration' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s and vts = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], bdid_val,
                                                                      chtype_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = %s and bdid = %s and chtype = %s and vts = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], bdid_val, chtype_val,
                                                                      duration_val)
        elif and_count == 5:
            bdid_val = sql.split("and")[2].split("bdid")[1].split("=")[1]
            if 'chtype' in tb and 'isfull' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s and isfull = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], bdid_val,
                                                                                        chtype_val, isfull_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s and isfull = " \
                            "%s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7],
                                                                                        d_date[7:-1],
                                                                                        bdid_val, chtype_val,
                                                                                        isfull_val,
                                                                                        playtype_val)
            elif 'chtype' in tb and 'duration' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = %s " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s and vts = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], bdid_val,
                                                                                        chtype_val, duration_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s and vts = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                        d_date[5:7],
                                                                                        d_date[7:-1],
                                                                                        bdid_val,
                                                                                        chtype_val,
                                                                                        duration_val,
                                                                                        playtype_val)
    elif 'bid' in tb:
        if and_count == 1:
            bid_val_r = sql.split("and")[1].split("bid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' group by city order by city " \
                        "desc;" % (bid_val_r, d_date[1:5], d_date[5:7], d_date[7:9],d_date[9:-1])
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' group by city order by city desc;" % (bid_val_r,
                                                                                       d_date[1:5], d_date[5:7],
                                                                                       d_date[7:-1])
        elif and_count == 2:
            if 'playtype' in tb:
                playtype_val = sql.split("and")[2].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and playtype = %s group by city " \
                        "order by city desc; " \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and playtype = %s group by city order by city " \
                        "desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], playtype_val)
            elif 'chtype' in tb:
                chtype_val = sql.split("and")[2].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and chtype = %s group by city " \
                        "order by city desc; " \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and chtype = %s group by city order by city " \
                        "desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], chtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[2].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and isfull = %s group by city " \
                        "order by city desc; " \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and isfull = %s group by city order by city " \
                        "desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[2].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and vts = %s group by city " \
                        "order by city desc; " \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and vts = %s group by city order by city " \
                        "desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], duration_val)
        elif and_count == 3:
            if 'playtype' in tb and 'isfull' in tb:
                playtype_val = sql.split("and")[2].split(";")[0].split("playtype")[1].split("=")[1]
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' " \
                            "and playtype = %s  and isfull = %s group by city order by city desc; " \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], playtype_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and playtype = %s " \
                            "and isfull_val = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], playtype_val, isfull_val)
            elif 'chtype' in tb and 'isfull' in tb:
                isfull_val = sql.split("and")[2].split(";")[0].split("isfull")[1].split("=")[1]
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                            "hour = \'%s\' and chtype = %s and isfull = %s group by city order by city desc; " \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], chtype_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and chtype = %s and " \
                            "isfull = %s group by city order by city desc;" % \
                            (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], chtype_val, isfull_val)
            elif 'duration' in tb and 'isfull' in tb:
                isfull_val = sql.split("and")[2].split(";")[0].split("isfull")[1].split("=")[1]
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                            "hour = \'%s\' and duration = %s and isfull = %s group by city order by city desc; " \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], duration_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and duration = %s and " \
                            "isfull = %s group by city order by city desc;" % \
                            (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], duration_val, isfull_val)
    elif 'fbdid' in tb:
        if and_count == 2:
            bdid_val = sql.split("and")[2].split(";")[0].split("bdid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s group by city " \
                        "order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], bdid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and bdid = %s group by city order by city " \
                        "desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], bdid_val)
        elif and_count == 3:
            bdid_val = sql.split("and")[2].split("bdid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid = %s and chtype " \
                            "= %s' group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], bdid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and chtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          bdid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and bdid " \
                            "= %s' and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], bdid_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and playtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          bdid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and bdid = %s and isfull = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], bdid_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and isfull = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          bdid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and hour " \
                            "= \'%s\' and bdid = %s and vts = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], bdid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and bdid = %s and vts = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          bdid_val, duration_val)
    elif 'vid' in tb:
        if and_count == 2:
            vid_val = sql.split("and")[2].split(";")[0].split("vid")[1].split('group')[0].split("=")[1]
            if 'hour' in tb:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and hour = \'%s\' and pid = %s group by city " \
                        "order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9], d_date[9:-1], vid_val)
            else:
                f_sql = "where bid = %s and year = \'%s\' and " \
                        "month = \'%s\' and day = \'%s\' and vid = %s group by city order by city desc;" \
                        % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1], vid_val)

        elif and_count == 3:
            vid_val = sql.split("and")[2].split("vid")[1].split("=")[1]
            if 'chtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], vid_val, chtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          vid_val, chtype_val)
            elif 'playtype' in tb:
                playtype_val = sql.split("and")[3].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and playtype " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], vid_val, playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and playtype = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          vid_val, playtype_val)
            elif 'isfull' in tb:
                isfull_val = sql.split("and")[3].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and isfull " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], vid_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and isfull = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          vid_val, isfull_val)
            elif 'duration' in tb:
                duration_val = sql.split("and")[3].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and vts " \
                            "= %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7], d_date[7:9],
                                                          d_date[9:-1], vid_val, duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and vts = %s group by " \
                            "city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7], d_date[7:-1],
                                                          vid_val, duration_val)
        elif and_count == 4:
            vid_val = sql.split("and")[2].split("vid")[1].split("=")[1]
            if 'chtype' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                playtype_val = sql.split("and")[4].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype " \
                            "= %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], vid_val, chtype_val,
                                                                      playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s and playtype = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], vid_val, chtype_val,
                                                                      playtype_val)
            elif 'chtype' in tb and 'isfull' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype = %s and isfull = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], vid_val,
                                                                      chtype_val, isfull_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s and isfull = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], vid_val, chtype_val,
                                                                      isfull_val)
            elif 'chtype' in tb and 'duration' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype " \
                            "= %s and vts = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:9], d_date[9:-1], vid_val, chtype_val,
                                                                      duration_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s and vts = " \
                            "%s group by city order by city desc;" % (bid_val, d_date[1:5], d_date[5:7],
                                                                      d_date[7:-1], vid_val, chtype_val,
                                                                      duration_val)
        elif and_count == 5:
            vid_val = sql.split("and")[2].split("vid")[1].split("=")[1]
            if 'chtype' in tb and 'isfull' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                isfull_val = sql.split("and")[4].split(";")[0].split("isfull")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype " \
                            "= %s and isfull = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], vid_val,
                                                                                        chtype_val, isfull_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s and isfull " \
                            "= %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                 d_date[5:7], d_date[7:-1],
                                                                                 vid_val, chtype_val,
                                                                                 isfull_val, playtype_val)
            elif 'chtype' in tb and 'duration' in tb and 'playtype' in tb:
                chtype_val = sql.split("and")[3].split(";")[0].split("chtype")[1].split("=")[1]
                duration_val = sql.split("and")[4].split(";")[0].split("duration")[1].split("=")[1]
                playtype_val = sql.split("and")[5].split(";")[0].split("playtype")[1].split('group')[0].split("=")[1]
                if 'hour' in tb:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and hour = \'%s\' and vid = %s and chtype " \
                            "= %s and vts = %s and playtype = %s group by city order by city desc;" \
                            % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:9],
                                                                                        d_date[9:-1], vid_val,
                                                                                        chtype_val, duration_val,
                                                                                        playtype_val)
                else:
                    f_sql = "where bid = %s and year = \'%s\' " \
                            "and month = \'%s\' and day = \'%s\' and vid = %s and chtype = %s and vts = " \
                            "%s and playtype = %s group by city order by city desc;" % (bid_val, d_date[1:5],
                                                                                        d_date[5:7], d_date[7:-1],
                                                                                        vid_val, chtype_val,
                                                                                        duration_val,
                                                                                        playtype_val)

    f_s = s_first + " " + f_sql
    return f_s


def fact_data(ip2, port2, user2, password2, dbname2, tb, sql, d_date):
    """
    :param ip2:
    :param port2:
    :param user2:
    :param password2:
    :param dbname2:
    :param tb:
    :return:
    """
    sql_first = ""
    if 'ap' in tb:
        return
    if 'uv' in tb:
        sql_first = "select city,count(distinct did) as uv from vv_fact"
    elif 'vv' in tb:
        sql_first = "select city,count(1) as vv from vv_fact"
    elif 'dau' in tb:
        sql_first = "select city,count(istinct did) as dau from pv_fact"
    elif 'pt' in tb:
        sql_first = "select city, sum(playtime) as playtime from playtime_fact"

    if 'pt' in tb:
        f_sql = data_pt_sql(tb, sql, d_date, sql_first)
    else:
        f_sql = data_f_sql(tb, sql, d_date, sql_first)

    #f_sql = data_f_sql(tb, sql, d_date, sql_first)
    logging.info("Fact SQL info: %s" % f_sql)
    print f_sql
    f_data = select_mode(ip2, port2, user2, password2, dbname2, f_sql, tb)
    return f_data


def data_merge_bak(file, re_db, db2, p, time):
    """
    Merge result table data and fact table data.
    :param file:
    :param re_db:
    :param p:
    :param time:
    :return:
    """
    ip2, port2, user2, password2, dbname2 = parse_conf(file, db2)
    ip1, port1, user1, password1, dbname1 = parse_conf(file, re_db)
    re_tb = get_tname(file, re_db, p, time)
    #logging.info("User info: %s,%s,%s,%s,%s,%s" % (ip1, port1, user1, password1, dbname1, re_tb))

    for i in range(len(re_tb)):
        d_date = ""
        tb = re_tb[i]
        print tb
        rtb_list, rtb_type = result_dtable(tb)
        if rtb_type == 'hour':
            d_date = date_d(ip1, port1, user1, password1, dbname1, 'hour')
        elif rtb_type == 'day':
            d_date = date_d(ip1, port1, user1, password1, dbname1, 'day')
        r_data, r_sql = result_data(ip1, port1, user1, password1, dbname1, tb, rtb_list, d_date)
        print r_sql
        logging.info("Result SQL info: %s" % r_sql)
        f_data = fact_data(ip2, port2, user2, password2, dbname2, tb, r_sql, d_date)
        return r_data, f_data


def main(file, re_db, db2):
    logger.init_log('./log/city', level=logging.DEBUG)
    pamram_list = ['vv']
    time_list = ['hour', 'day']
    ip2, port2, user2, password2, dbname2 = parse_conf(file, db2)
    ip1, port1, user1, password1, dbname1 = parse_conf(file, re_db)

    for pa in pamram_list:
        for time in time_list:
            re_tb = get_tname(file, re_db, pa, time)
            #logging.info("User info: %s,%s,%s,%s,%s,%s" % (ip1, port1, user1, password1, dbname1, re_tb))
            for i in range(len(re_tb)):
                d_date = ""
                tb = re_tb[i]
                logging.info("Result Table info: %s" % tb)
                print tb
                rtb_list, rtb_type = result_dtable(tb)
                if rtb_type == 'hour':
                    d_date = date_d(ip1, port1, user1, password1, dbname1, 'hour')
                elif rtb_type == 'day':
                    d_date = date_d(ip1, port1, user1, password1, dbname1, 'day')
                r_data, r_sql = result_data(ip1, port1, user1, password1, dbname1, tb, rtb_list, d_date)
                print r_sql
                logging.info("Result SQL info: %s" % r_sql)
                f_data = fact_data(ip2, port2, user2, password2, dbname2, tb, r_sql, d_date)

                ff1 = os.path.join('/home/qibin/pt/city/data', r_data)
                ff2 = os.path.join('/home/qibin/pt/city/data', f_data)
                rf1 = os.path.join('/home/qibin/pt/city/data', 'bak' + '_' + r_data.split('/')[-1])
                rf2 = os.path.join('/home/qibin/pt/city/data', 'bak' + '_' + f_data.split('/')[-1])
                if os.path.getsize(ff1) == 0 and os.path.getsize(ff2) == 0:
                    print "%s Success!" % tb
                    logging.info(" %s Success!" % tb)
                elif os.path.getsize(ff1) == 0 and os.path.getsize(ff2) > 0:
                    print "%s False!" % tb
                    logging.info(" %s False!" % tb)
                elif os.path.getsize(ff2) == 0 and os.path.getsize(ff1) > 0:
                    print "%s False!" % tb
                    logging.info(" %s False!" % tb)
                else:
                    right = pd.read_csv(ff1, header=None, sep=',', low_memory=False)
                    left = pd.read_csv(ff2, header=None, sep=',', low_memory=False)
                    if len(right) != len(left):
                        print "%s False!" % tb
                        logging.info(" %s False!" % tb)
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
                            print "%s Success!" % tb
                            logging.info(" %s Success!" % tb)
                        else:
                            print "%s False!" % tb
                            logging.info(" %s False!" % tb)


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


if __name__ == '__main__':
    file = "database.json"
    re_db = "test_result"
    online_db = "dm_pv_fact"
    main(file, re_db, online_db)

