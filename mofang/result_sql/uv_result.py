#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: QiBin
# Date : 20170411
"""
Usage:

"""
from base.base import *
from random import choice
bid_list = [13]
bid_val = str(choice(bid_list))


def uv_sql(tb, rtb_list, date_data, t_type, pa_list):
    """
    Get the result sql for  result table.
    :param tb:
    :param rtb_list:
    :param date_data:
    :param t_type:
    :return:
    """
    r_sql_f = r_sql_e = ""
    if t_type in ['hour', 'day']:
        rtb_list.remove(t_type)

    any_val = pa_list[0]
    bny_val = pa_list[1]
    cny_val = pa_list[2]
    dny_val = pa_list[3]
    eny_val = pa_list[4]
    fny_val = pa_list[5]
    print "the tb in uv is : %s " % tb
    print "the value of rtb_list is %s" % rtb_list

    if len(rtb_list) == 2:
        if 'ap' not in rtb_list:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,uv from %s where date = %s and bid = \'%s\'" \
                              % (tb, date_data, bid_val)
                    r_sql_e = "group by bid,uv order by bid,uv asc;"
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s" \
                              % (rtb_list[1], tb, date_data, bid_val, rtb_list[1], any_val)
                    r_sql_e = "group by bid,uv,%s order by bid,uv,%s asc;" \
                              % (rtb_list[1], rtb_list[1])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,substring(date, 9,10) as hour,uv from %s where " \
                              "date = %s and bid = \'%s\' " \
                              % (tb, date_data, bid_val)
                    r_sql_e = "group by bid,uv,hour order by bid,uv,hour  asc;"
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,substring(date, 9,10) as hour,%s,uv from %s where " \
                              "date = %s and bid = \'%s\' and %s = %s "\
                              % (rtb_list[1], tb, date_data, bid_val, rtb_list[1], any_val)
                    r_sql_e = "group by bid,uv,hour,%s order by bid,uv,hour,%s asc;" \
                              % (rtb_list[1], rtb_list[1])
    elif len(rtb_list) == 3:
        if 'ap' not in rtb_list:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s" \
                              % (rtb_list[2], tb, date_data, bid_val, rtb_list[2], bny_val)
                    r_sql_e = "group by bid,uv,%s  order by bid,uv,%s  asc;" \
                              % ( rtb_list[2], rtb_list[2])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s"\
                              % (rtb_list[1], rtb_list[2], tb, date_data, bid_val, rtb_list[1],
                                 any_val, rtb_list[2], bny_val)
                    r_sql_e = "group by bid,uv,%s,%s order by bid,uv,%s,%s asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[1], rtb_list[2])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,substring(date, 9,10) as hour,uv from %s where " \
                              "date = %s and bid = \'%s\' and   %s = %s"\
                              % (rtb_list[2], tb, date_data, bid_val, rtb_list[2], bny_val)
                    r_sql_e = "group by bid,uv,%s,hour order by bid,uv,%s,hour asc;" \
                              % (rtb_list[2], rtb_list[2])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,substring(date, 9,10) as hour,uv from %s where " \
                              "date = %s and bid = \'%s\' and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], tb, date_data, bid_val, rtb_list[1], any_val,
                                 rtb_list[2], bny_val)
                    r_sql_e = "group by bid,uv,%s,%s,hour order by bid,uv,%s,%s,hour asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[1], rtb_list[2])
        else:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,country,province,uv from %s where date = %s " \
                              "and bid = \'%s\'"\
                              % (tb, date_data, bid_val)
                    r_sql_e = "group by bid,uv,country,province order by bid,uv,country,province asc;"
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,country,province,uv from %s where date = %s " \
                              "and bid = \'%s\' and  %s = %s"\
                              % (rtb_list[1],tb, date_data, bid_val, rtb_list[1], any_val)
                    r_sql_e = "group by bid,uv,%s,country,province order by bid,uv,%s,country,province asc;"\
                              % (rtb_list[1], rtb_list[1])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,country,province,substring(date, 9,10) as hour,uv " \
                              "from %s where date = %s and bid = \'%s\'"\
                              % (tb, date_data, bid_val)
                    r_sql_e = "group by bid,uv,country,province,hour order by bid,uv,country,province,hour asc;"
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,country,province,substring(date, 9,10) as hour,uv " \
                              "from %s where date = %s and bid = \'%s\' and  %s = %s" \
                              % (rtb_list[1], tb, date_data, bid_val, rtb_list[1], any_val)
                    r_sql_e = "group by bid,uv,%s,country,province,hour order by bid,uv,%s,country,province,hour asc;" \
                              % (rtb_list[1], rtb_list[1])
    elif len(rtb_list) == 4:
        if 'ap' not in rtb_list:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], tb, date_data, bid_val,
                                 rtb_list[2], bny_val, rtb_list[3], cny_val)
                    r_sql_e = "group by bid,uv,%s,%s order by bid,uv,%s,%s asc;" \
                              % ( rtb_list[2], rtb_list[3], rtb_list[2], rtb_list[3])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s and %s = %s"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], tb, date_data, bid_val,
                                 rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3], cny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s order by bid,uv,%s,%s,%s asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[1], rtb_list[2], rtb_list[3])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,substring(date, 9,10) as hour,uv from %s where " \
                              "date = %s and bid = \'%s\' and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], tb, date_data, bid_val,
                                 rtb_list[2], bny_val, rtb_list[3], cny_val)
                    r_sql_e = "group by bid,uv,%s,%s,hour order by bid,uv,%s,%s,hour asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[2], rtb_list[3])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,substring(date, 9,10) as hour,uv from %s where " \
                              "date = %s and bid = \'%s\' and %s = %s and %s = %s and %s = %s"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], tb, date_data, bid_val,
                                 rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3], cny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,hour order by bid,uv,%s,%s,%s,hour asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[1], rtb_list[2], rtb_list[3])
        else:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,country,province,uv from %s where date = %s " \
                              "and bid = \'%s\' and %s = %s" \
                              % (rtb_list[2], tb, date_data, bid_val, rtb_list[2], bny_val)
                    r_sql_e = "group by bid,uv,%s,country,province order by bid,uv,%s,country,province asc;" \
                              % ( rtb_list[2], rtb_list[2])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,country,province,uv from %s where date = %s " \
                              "and bid = \'%s\' and %s = %s and %s = %s"\
                              % (rtb_list[1], rtb_list[2], tb, date_data, bid_val,
                                 rtb_list[1], any_val, rtb_list[2], bny_val)
                    r_sql_e = "group by bid,uv,%s,%s,country,province order by bid,uv,%s,%s,country,province asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[1], rtb_list[2])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,country,province,substring(date, 9,10) as hour,sum(pt) " \
                              "as pt from %s where date = %s and bid = \'%s\' and %s = %s" \
                              % (rtb_list[2], tb, date_data, bid_val, rtb_list[2], bny_val)
                    r_sql_e = "group by bid,uv,%s,country,province,hour order by bid,uv,%s,country,province," \
                              "hour asc;" \
                              % (rtb_list[2], rtb_list[2])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,country,province,substring(date, 9,10) as hour," \
                              "uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s"\
                              % (rtb_list[1], rtb_list[2], tb, date_data, bid_val, rtb_list[1],
                                 any_val, rtb_list[2], bny_val)
                    r_sql_e = "group by bid,uv,%s,%s,country,province,hour order by bid,uv,%s,%s," \
                              "country,province,hour asc;"\
                              % (rtb_list[1], rtb_list[2], rtb_list[1], rtb_list[2])
    elif len(rtb_list) == 5:
        if 'ap' not in rtb_list:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], tb, date_data, bid_val,
                                 rtb_list[2], bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s order by bid,uv,%s,%s,%s asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[2], rtb_list[3], rtb_list[4])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], tb, date_data,
                                 bid_val, rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3],
                                 cny_val, rtb_list[4], dny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s order by bid,uv,%s,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,%s,substring(date, 9,10) as hour,uv from %s where " \
                              "date = %s and bid = \'%s\' and %s = %s and %s = %s and %s = %s " \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], tb, date_data, bid_val,
                                 rtb_list[2], bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,hour order by bid,uv,%s,%s,%s,hour asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[2], rtb_list[3], rtb_list[4])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,%s,substring(date, 9,10) as hour,uv " \
                              "from %s where date = %s and bid = \'%s\' and %s = %s and %s = %s " \
                              "and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], tb, date_data,
                                 bid_val, rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3],
                                 cny_val, rtb_list[4], dny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,hour order by bid,uv,%s,%s,%s,%s,hour asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4])
        else:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,country,province,uv from %s where date = %s " \
                              "and bid = \'%s\' and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], tb, date_data, bid_val, rtb_list[2], bny_val,
                                 rtb_list[3], cny_val)
                    r_sql_e = "group by bid,uv,%s,%s,country,province order by bid,uv,%s,%s,country,province asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[2], rtb_list[3])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,country,province,uv from %s where date = %s " \
                              "and bid = \'%s\' and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], tb, date_data, bid_val,
                                 rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3], cny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,country,province order by bid,uv,%s,%s,%s," \
                              "country,province asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[1], rtb_list[2], rtb_list[3])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,country,province,substring(date, 9,10) as hour,sum(pt) " \
                              "as pt from %s where date = %s and bid = \'%s\' and %s = %s and %s = %s " \
                              % (rtb_list[2], rtb_list[3], tb, date_data, bid_val, rtb_list[2],
                                 bny_val, rtb_list[3], cny_val)
                    r_sql_e = "group by bid,uv,%s,%s,country,province,hour order by bid,uv,%s,%s,country,province," \
                              "hour asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[2], rtb_list[3])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,country,province,substring(date, 9,10) as hour," \
                              "uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], tb, date_data, bid_val,
                                 rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3], cny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,country,province,hour order by bid,uv,%s,%s,%s," \
                              "country,province,hour asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[1], rtb_list[2], rtb_list[3])
    elif len(rtb_list) == 6:
        if 'ap' not in rtb_list:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,%s,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], tb, date_data, bid_val,
                                 rtb_list[2], bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val,
                                 rtb_list[5], eny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s order by bid,uv,%s,%s,%s,%s asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[2],
                                 rtb_list[3], rtb_list[4], rtb_list[5])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,%s,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], tb, date_data,
                                 bid_val, rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3],
                                 cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,%s order by bid,uv,%s,%s,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,%s,%s,substring(date, 9,10) as hour,uv from %s where " \
                              "date = %s and bid = \'%s\' and %s = %s and %s = %s and %s = %s  and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], tb, date_data, bid_val,
                                 rtb_list[2], bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val,
                                 rtb_list[5], eny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,hour order by bid,uv,%s,%s,%s,%s,hour asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[2],
                                 rtb_list[3], rtb_list[4], rtb_list[5])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,%s,%s,substring(date, 9,10) as hour,uv " \
                              "from %s where date = %s and bid = \'%s\' and %s = %s and %s = %s " \
                              "and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], tb, date_data,
                                 bid_val, rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3],
                                 cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,%s,hour order by bid,uv,%s,%s,%s,%s,%s,hour asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])
        else:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,%s,country,province,uv from %s where date = %s " \
                              "and bid = \'%s\' and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], tb, date_data, bid_val, rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,country,province order by bid,uv,%s,%s,%s," \
                              "country,province asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[2], rtb_list[3], rtb_list[4])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,%s,country,province,uv from %s where date = %s " \
                              "and bid = \'%s\' and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], tb, date_data, bid_val,
                                 rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3], cny_val,
                                 rtb_list[4], dny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,country,province order by bid,uv,%s,%s,%s,%s," \
                              "country,province asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,%s,country,province,substring(date, 9,10) as hour,sum(pt) " \
                              "as pt from %s where date = %s and bid = \'%s\' and %s = %s and %s = %s " \
                              "and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], tb, date_data, bid_val, rtb_list[2],
                                 bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,country,province,hour order by bid,uv,%s,%s,%s," \
                              "country,province,hour asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[2], rtb_list[3], rtb_list[4])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,%s,country,province,substring(date, 9,10) as hour," \
                              "uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], tb, date_data, bid_val,
                                 rtb_list[1], any_val, rtb_list[2], bny_val, rtb_list[3], cny_val,
                                 rtb_list[4], dny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,country,province,hour order by bid,uv,%s,%s,%s,%s," \
                              "country,province,hour asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4])
    elif len(rtb_list) == 7:
        if 'ap' not in rtb_list:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,%s,%s,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[5], tb,
                                 date_data, bid_val, rtb_list[2], bny_val, rtb_list[3], cny_val,
                                 rtb_list[4], dny_val, rtb_list[5], eny_val, rtb_list[6], fny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,%s order by bid,uv,%s,%s,%s,%s,%s asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[6], rtb_list[2],
                                 rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[6])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,%s,%s,%s,uv from %s where date = %s and bid = \'%s\' " \
                              "and %s = %s and %s = %s and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 rtb_list[6], tb, date_data, bid_val, rtb_list[1], any_val, rtb_list[2],
                                 bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val,
                                 rtb_list[6], fny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,%s,%s order by bid,uv,%s,%s,%s,%s,%s,%s asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 rtb_list[6], rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4],
                                 rtb_list[5], rtb_list[6])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,%s,%s,%s,substring(date, 9,10) as hour,uv from %s where " \
                              "date = %s and bid = \'%s\' and %s = %s and %s = %s and %s = %s and " \
                              "%s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[6], tb,
                                 date_data, bid_val, rtb_list[2], bny_val, rtb_list[3], cny_val,
                                 rtb_list[4], dny_val, rtb_list[5], eny_val, rtb_list[6], fny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,%s,hour order by bid,uv,%s,%s,%s,%s,%s,hour asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[6], rtb_list[2],
                                 rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[6])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,%s,%s,%s,substring(date, 9,10) as hour,uv " \
                              "from %s where date = %s and bid = \'%s\' and %s = %s and %s = %s " \
                              "and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 rtb_list[6], tb, date_data, bid_val, rtb_list[1], any_val,
                                 rtb_list[2], bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val,
                                 rtb_list[5], eny_val, rtb_list[6], fny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,%s,%s,hour order by bid,uv,%s,%s,%s,%s,%s,%s,hour asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 rtb_list[6], rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4],
                                 rtb_list[5], rtb_list[6])
        else:
            if t_type == 'day':
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,%s,%s,country,province,uv from %s where date = %s " \
                              "and bid = \'%s\' and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], tb,
                                 date_data, bid_val, rtb_list[2], bny_val, rtb_list[3],
                                 cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,country,province order by bid,uv,%s,%s,%s,%s," \
                              "country,province asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[2],
                                 rtb_list[3], rtb_list[4], rtb_list[5])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,%s,%s,country,province,uv from %s where date = %s " \
                              "and bid = \'%s\' and %s = %s and %s = %s and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 tb, date_data, bid_val, rtb_list[1], any_val, rtb_list[2],
                                 bny_val, rtb_list[3], cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,%s,country,province order by bid,uv,%s,%s,%s,%s,%s," \
                              "country,province asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])
            else:
                if rtb_list[1] == 'bid':
                    r_sql_f = "select bid,%s,%s,%s,%s,country,province,substring(date, 9,10) as hour,sum(pt) " \
                              "as pt from %s where date = %s and bid = \'%s\' and %s = %s and %s = %s " \
                              "and %s = %s and %s = %s" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], tb, date_data,
                                 bid_val, rtb_list[2], bny_val, rtb_list[3], cny_val, rtb_list[4],
                                 dny_val, rtb_list[5], eny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,country,province,hour order by bid,uv,%s,%s,%s,%s," \
                              "country,province,hour asc;" \
                              % (rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[2],
                                 rtb_list[3], rtb_list[4], rtb_list[5])
                else:
                    if 'fbdid' in tb:
                        rtb_list[1] = "bdid"
                    r_sql_f = "select bid,%s,%s,%s,%s,%s,country,province,substring(date, 9,10) as hour," \
                              "uv from %s where date = %s and bid = \'%s\' and %s = %s and %s = %s " \
                              "and %s = %s and %s = %s and %s = %s" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5],
                                 tb, date_data, bid_val, rtb_list[1], any_val, rtb_list[2], bny_val,
                                 rtb_list[3], cny_val, rtb_list[4], dny_val, rtb_list[5], eny_val)
                    r_sql_e = "group by bid,uv,%s,%s,%s,%s,%s,country,province,hour order by bid,uv," \
                              "%s,%s,%s,%s,%s,country,province,hour asc;" \
                              % (rtb_list[1], rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5], rtb_list[1],
                                 rtb_list[2], rtb_list[3], rtb_list[4], rtb_list[5])

    r_sql = r_sql_f + ' ' + r_sql_e
    return r_sql
