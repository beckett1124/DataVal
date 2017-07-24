#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: QiBin
# Date : 20170411
"""
Usage:

"""
from base.base import *
bid_list = [1]
mei_list = ['bid', 'cid', 'pid', 'vid', 'bdid', 'sid']


def vv_sql(tb, sql, rtb_list, d_date, pa_list):
    """
    The fact database sql.
    :param tb:
    :param sql:
    :param rtb_list:
    :param d_date:
    :return:
    """""
    f_sql_f = f_sql_e = ""
    bid_val = sql.split("and")[1].split("group")[0].split("bid")[1].split("=")[1]
    fact_table = get_fact_table(tb, bid_val)
    print "the name of table name in fact is %s" % fact_table
    any_val = pa_list[0]
    bny_val = pa_list[1]
    cny_val = pa_list[2]
    dny_val = pa_list[3]
    eny_val = pa_list[4]
    fny_val = pa_list[5]
    print "***********value of rtb_list len is %s" % len(rtb_list)
    if len(rtb_list) == 3:
        if 'ap' not in rtb_list:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,count(1) as vv from %s where bid = %s and year = \'%s\' " \
                              "and month = \'%s\' and day = \'%s\'"\
                              % (fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]))
                    f_sql_e = "group by bid order by bid asc;"
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s"\
                              % (rtb_list[1], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:-1]), rtb_list[1], any_val)
                    f_sql_e = "group by bid,%s order by bid,%s asc;" % (rtb_list[1], rtb_list[1])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,hour,count(1) as vv from %s where bid = %s and year = \'%s\' " \
                              "and month = \'%s\' and day = \'%s\' and hour = \'%s\'"\
                              % (fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]))
                    f_sql_e = "group by bid,hour order by bid,hour asc;"
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\'and %s = %s"\
                              % (rtb_list[1], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:9]), str(d_date[9:-1]), rtb_list[1], any_val)
                    f_sql_e = "group by bid,hour,%s order by bid,hour,%s asc;"\
                              % (rtb_list[1], rtb_list[1])
    elif len(rtb_list) == 4:
        if 'ap' not in rtb_list:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s"\
                              % (rtb_list[2], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:-1]), rtb_list[2], bny_val)
                    f_sql_e = "group by bid,%s order by bid,%s asc;"\
                              % (rtb_list[2], rtb_list[2])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:-1]), rtb_list[1], any_val, rtb_list[2], bny_val)
                    f_sql_e = "group by bid,%s,%s order by bid,%s,%s asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[1], rtb_list[2])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' and %s = %s"\
                              % (rtb_list[2], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:9]), str(d_date[9:-1]), rtb_list[2], bny_val)
                    f_sql_e = "group by bid,hour,%s order by bid,hour,%s asc;" % (rtb_list[2], rtb_list[2])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' " \
                              "and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:9]), str(d_date[9:-1]), rtb_list[1], any_val, rtb_list[2], bny_val)
                    f_sql_e = "group by bid,hour,%s,%s order by bid,hour,%s,%s asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[1], rtb_list[2])
        else:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,country,province,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\'"\
                              % (fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:-1]))
                    f_sql_e = "group by bid,country,province order by bid,country,province asc;"
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,country,province,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s" \
                              % (rtb_list[1], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:-1]), rtb_list[1], any_val)
                    f_sql_e = "group by bid,%s,country,province order by bid,%s,country,province asc;"\
                              % (rtb_list[1], rtb_list[1])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,country,province,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\'"\
                              % (fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:9]), str(d_date[9:-1]))
                    f_sql_e = "group by bid,hour,country,province order by bid,hour,country,province asc;"
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,country,province,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' " \
                              "and %s = %s" \
                              % (rtb_list[1], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:9]), str(d_date[9:-1]), rtb_list[1], any_val)
                    f_sql_e = "group by bid,hour,country,province,%s order by bid,hour,country,province,%s asc;"\
                              % (rtb_list[1], rtb_list[1])
    elif len(rtb_list) == 5:
        if 'ap' not in rtb_list:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:-1]), rtb_list[1], any_val, rtb_list[2], bny_val)
                    f_sql_e = "group by bid,%s,%s order by bid,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[1], rtb_list[2])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s" \
                              " and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]), rtb_list[1],
                                 any_val, rtb_list[2], bny_val, rtb_list[3], cny_val)
                    f_sql_e = "group by bid,%s,%s,%s order by bid,%s,%s,%s asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[1], rtb_list[2], rtb_list[3])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour =  \'%s\' " \
                              "and %s = %s and %s = %s " \
                              % (rtb_list[2], rtb_list[3], fact_table, bid_val, str(d_date[1:5]),
                                 str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]), rtb_list[2], bny_val,
                                 rtb_list[3], cny_val)
                    f_sql_e = "group by bid,hour,%s,%s order by bid,hour,%s,%s asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[2], rtb_list[3])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' and %s " \
                              "= %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], fact_table, bid_val, str(d_date[1:5]),
                                 str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]), rtb_list[1], any_val,
                                 rtb_list[2], bny_val, rtb_list[3], cny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s order by bid,hour,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[1], rtb_list[2], rtb_list[3])
        else:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,country,province,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s"\
                              % (rtb_list[2], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:-1]), rtb_list[2], bny_val)
                    f_sql_e = "group by bid,%s,country,province order by bid,%s,country,province asc;"\
                              % (rtb_list[2], rtb_list[2])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,country,province,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:-1]), rtb_list[1], any_val, rtb_list[2], bny_val)
                    f_sql_e = "group by bid,%s,%s,country,province order by bid,%s,%s,country,province asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[1], rtb_list[2])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,country,province,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' and %s = %s"\
                              % (rtb_list[2], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:9]), str(d_date[9:-1]), rtb_list[2], bny_val)
                    f_sql_e = "group by bid,hour,%s,country,province order by bid,hour,%s,country,province asc;"\
                              % (rtb_list[2], rtb_list[2])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,country,province,hour,count(1) as vv from %s " \
                              "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                              "hour = \'%s\' and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:9]), str(d_date[9:-1]), rtb_list[1], any_val, rtb_list[2], bny_val)
                    f_sql_e = "group by bid,hour,%s,country,province,%s order by bid,hour,%s,country," \
                              "province,%s asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[1], rtb_list[2])
    elif len(rtb_list) == 6:
        if 'ap' not in rtb_list:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s " \
                              "and %s = %s and %s = %s " \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]), rtb_list[1],
                                 any_val, rtb_list[2], bny_val, rtb_list[3], cny_val)
                    f_sql_e = "group by bid,%s,%s,%s order by bid,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[1], rtb_list[2], rtb_list[3])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s" \
                              " and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]), rtb_list[1],
                                 any_val, rtb_list[2], bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val)
                    f_sql_e = "group by bid,%s,%s,%s,%s order by bid,%s,%s,%s,%s asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour =  \'%s\' " \
                              "and %s = %s and %s = %s  and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], fact_table, bid_val, str(d_date[1:5]),
                                 str(d_date[5:7]), str(d_date[7:9]), str(d_date[9:-1]), rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s order by bid,hour,%s,%s,%s asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[2], rtb_list[3], rtb_list[4])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' and %s " \
                              "= %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], fact_table,
                                 bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]), rtb_list[1], any_val,rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s,%s order by bid,hour,%s,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4])
        else:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,country,province,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s"\
                              % (rtb_list[2], rtb_list[3], fact_table, bid_val, str(d_date[1:5]),
                                 str(d_date[5:7]), str(d_date[7:-1]), rtb_list[2], bny_val,
                                 rtb_list[3], cny_val)
                    f_sql_e = "group by bid,%s,%s,country,province order by bid,%s,%s,country,province asc;"\
                              % (rtb_list[2], rtb_list[3], rtb_list[2], rtb_list[3])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,country,province,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s" \
                              " and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]),
                                 rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3], cny_val)
                    f_sql_e = "group by bid,%s,%s,%s,country,province order by bid,%s,%s,%s,country,province asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[1], rtb_list[2], rtb_list[3])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,country,province,hour,count(1) as vv from %s where " \
                              "bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' " \
                              "and hour = \'%s\' and %s = %s and %s = %s"\
                              % (rtb_list[2], rtb_list[3], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:9]), str(d_date[9:-1]), rtb_list[2], bny_val, rtb_list[3], cny_val)
                    f_sql_e = "group by bid,hour,%s,%s,country,province order by bid,hour,%s,%s," \
                              "country,province asc;"\
                              % (rtb_list[2], rtb_list[3], rtb_list[2], rtb_list[3])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,country,province,hour,count(1) as vv from %s " \
                              "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                              "hour = \'%s\' and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]), rtb_list[1], any_val, rtb_list[2],
                                 bny_val, rtb_list[3], cny_val)
                    f_sql_e = "group by bid,hour,%s,%s,country,province,%s order by bid,hour,%s,%s,country," \
                              "province,%s asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[1], rtb_list[2], rtb_list[3])
    elif len(rtb_list) == 7:
        if 'ap' not in rtb_list:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,%s,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s " \
                              "and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]), rtb_list[1],
                                 any_val, rtb_list[2], bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val)
                    f_sql_e = "group by bid,%s,%s,%s,%s order by bid,%s,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4],
                                 rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,%s,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s" \
                              " and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]), rtb_list[1],
                                 any_val, rtb_list[2], bny_val, rtb_list[3], cny_val,
                                 rtb_list[4], dny_val, rtb_list[5], eny_val)
                    f_sql_e = "group by bid,%s,%s,%s,%s,%s order by bid,%s,%s,%s,%s,%s asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,%s,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour =  \'%s\' " \
                              "and %s = %s and %s = %s  and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], fact_table,
                                 bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]), rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s,%s order by bid,hour,%s,%s,%s,%s asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,%s,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' and %s " \
                              "= %s and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], fact_table,
                                 bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]), rtb_list[1], any_val,rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s,%s,%s order by bid,hour,%s,%s,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])
        else:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,%s,country,province,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s " \
                              "and %s = %s and %s = %s"\
                              % (rtb_list[2], rtb_list[3], rtb_list[4], fact_table, bid_val, str(d_date[1:5]),
                                 str(d_date[5:7]), str(d_date[7:-1]), rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val)
                    f_sql_e = "group by bid,%s,%s,%s,country,province order by bid,%s,%s,%s,country,province asc;"\
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[2], rtb_list[3], rtb_list[4])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,%s,country,province,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s" \
                              " and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]),
                                 rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3],
                                 cny_val, rtb_list[4], dny_val)
                    f_sql_e = "group by bid,%s,%s,%s,%s,country,province order by bid,%s,%s,%s,%s,country,province asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4],
                                 rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,%s,country,province,hour,count(1) as vv from %s where " \
                              "bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' " \
                              "and hour = \'%s\' and %s = %s and %s = %s and %s = %s"\
                              % (rtb_list[2], rtb_list[3], rtb_list[4], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]), rtb_list[2], bny_val, rtb_list[3],
                                 cny_val, rtb_list[4], dny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s,country,province order by bid,hour,%s,%s,%s," \
                              "country,province asc;"\
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[2], rtb_list[3], rtb_list[4])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,%s,country,province,hour,count(1) as vv from %s " \
                              "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                              "hour = \'%s\' and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]), rtb_list[1], any_val, rtb_list[2],
                                 bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s,country,province,%s order by bid,hour,%s,%s,%s,country," \
                              "province,%s asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4])
    elif len(rtb_list) == 8:
        if 'ap' not in rtb_list:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,%s,%s,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s " \
                              "and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]), rtb_list[1],
                                 any_val, rtb_list[2], bny_val, rtb_list[3], cny_val, rtb_list[4],
                                 dny_val, rtb_list[5], eny_val)
                    f_sql_e = "group by bid,%s,%s,%s,%s,%s order by bid,%s,%s,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,%s,%s,%s,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s" \
                              " and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 rtb_list[6], fact_table, bid_val, str(d_date[1:5]), str(d_date[5:7]),
                                 str(d_date[7:-1]), rtb_list[1], any_val, rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val, rtb_list[5],
                                 eny_val, rtb_list[6], fny_val)
                    f_sql_e = "group by bid,%s,%s,%s,%s,%s,%s order by bid,%s,%s,%s,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4],
                                 rtb_list[5], rtb_list[6], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[6])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,%s,%s,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour =  \'%s\' " \
                              "and %s = %s and %s = %s  and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[6], fact_table,
                                 bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]), rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val, rtb_list[5],
                                 eny_val, rtb_list[6], fny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s,%s,%s order by bid,hour,%s,%s,%s,%s,%s asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[6],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[6])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,%s,%s,%s,hour,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and hour = \'%s\' and %s " \
                              "= %s and %s = %s and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4],
                                 rtb_list[5], rtb_list[6], fact_table,
                                 bid_val, str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]), rtb_list[1], any_val, rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val,
                                 rtb_list[5], eny_val, rtb_list[6], fny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s,%s,%s,%s order by bid,hour,%s,%s,%s,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4],
                                 rtb_list[5], rtb_list[6], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[6])
        else:
            if 'day' in rtb_list:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,%s,%s,country,province,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s " \
                              "and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], fact_table, bid_val, str(d_date[1:5]),
                                 str(d_date[5:7]), str(d_date[7:-1]), rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    f_sql_e = "group by bid,%s,%s,%s,%s,country,province order by " \
                              "bid,%s,%s,%s,%s,country,province asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[2],
                                 rtb_list[3], rtb_list[4], rtb_list[5])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,%s,%s,country,province,count(1) as vv from %s where bid = %s and " \
                              "year = \'%s\' and month = \'%s\' and day = \'%s\' and %s = %s and %s = %s" \
                              " and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4],
                                 rtb_list[5], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:-1]),
                                 rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3],
                                 cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    f_sql_e = "group by bid,%s,%s,%s,%s,%s,country,province order by bid," \
                              "%s,%s,%s,%s,%s,country,province asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])
            else:
                if rtb_list[1] == 'bid':
                    f_sql_f = "select bid,%s,%s,%s,%s,country,province,hour,count(1) as vv from %s where " \
                              "bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' " \
                              "and hour = \'%s\' and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]), rtb_list[2], bny_val, rtb_list[3],
                                 cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s,%s,country,province order by bid,hour,%s,%s,%s,%s," \
                              "country,province asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])
                else:
                    if 'sid' in tb:
                        rtb_list[1] = "seriesid"
                    elif 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    f_sql_f = "select bid,%s,%s,%s,%s,%s,country,province,hour,count(1) as vv from %s " \
                              "where bid = %s and year = \'%s\' and month = \'%s\' and day = \'%s\' and " \
                              "hour = \'%s\' and %s = %s and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], fact_table, bid_val,
                                 str(d_date[1:5]), str(d_date[5:7]), str(d_date[7:9]),
                                 str(d_date[9:-1]), rtb_list[1], any_val, rtb_list[2],
                                 bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    f_sql_e = "group by bid,hour,%s,%s,%s,%s,country,province,%s order by bid," \
                              "hour,%s,%s,%s,%s,country,province,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])
    f_sql = f_sql_f + ' ' + f_sql_e
    return f_sql
