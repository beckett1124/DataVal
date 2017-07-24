#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: QiBin
# Date : 20170411
"""
Usage:

"""

import logging
import os
import pandas as pd
from base import base, logger
from result_sql import pt_result
from result_sql import pv_result
from result_sql import uv_result
from result_sql import vv_result
from result_sql import device_result
from fact_sql import pt_fact
from fact_sql import pv_fact
from fact_sql import uv_fact
from fact_sql import vv_fact
from fact_sql import device_fact
from fact_sql import live_lb_vv_fact
from result_sql import live_lb_vv_result

bid_list = [13]
mei_list = ['bid', 'fbdid', 'cid', 'pid', 'vid', 'bdid']
other_pa = ['device', 'pt', 'dau', 'pv' ]


def result_data(ip1, port1, user1, password1, dbname1, tb, rtb_list, date_data, pa_list):
    """
    Get result database select data.
    :param ip1:
    :param port1:
    :param user1:
    :param password1:
    :param dbname1:
    :param tb:
    :return:
    """
    r_sql = ""
    if 'hour' in rtb_list:
        if 'carousel' not in rtb_list and 'pt' in rtb_list:
            r_sql = live_lb_vv_result.vv_sql(tb, rtb_list, date_data, 'hour', pa_list)
        elif 'carousel' in rtb_list and 'pt' in rtb_list:
            r_sql = pt_result.pt_sql(tb, rtb_list, date_data, 'hour', pa_list)
        elif 'pv' in rtb_list:
            r_sql = pv_result.pv_sql(tb, rtb_list, date_data, 'hour', pa_list)
        elif 'uv' in rtb_list:
            r_sql = uv_result.uv_sql(tb, rtb_list, date_data, 'hour', pa_list)
        elif 'vv' in rtb_list:
            r_sql = vv_result.vv_sql(tb, rtb_list, date_data, 'hour', pa_list)
        elif 'device' in rtb_list:
            r_sql = device_result.device_sql(tb, rtb_list, date_data, 'hour', pa_list)
    else:
        if 'carousel' not in rtb_list and 'pt' in rtb_list:
            r_sql = live_lb_vv_result.vv_sql(tb, rtb_list, date_data, 'day', pa_list)
        elif 'carousel' in rtb_list and 'pt' in rtb_list:
            r_sql = pt_result.pt_sql(tb, rtb_list, date_data, 'day', pa_list)
        elif 'pv' in rtb_list:
            r_sql = pv_result.pv_sql(tb, rtb_list, date_data, 'day', pa_list)
        elif 'uv' in rtb_list:
            r_sql = uv_result.uv_sql(tb, rtb_list, date_data, 'day', pa_list)
        elif 'vv' in rtb_list:
            r_sql = vv_result.vv_sql(tb, rtb_list, date_data, 'day', pa_list)
        elif 'device' in rtb_list:
            r_sql = device_result.device_sql(tb, rtb_list, date_data, 'day', pa_list)
    print r_sql
    logging.info("Result SQL info: %s" % r_sql)
    r_data = base.select_mode(ip1, port1, user1, password1, dbname1, r_sql, tb)
    return r_data, r_sql


def fact_data(ip2, port2, user2, password2, dbname2, tb, rtb_list, sql, d_date, pa_list):
    """
    :param ip2:
    :param port2:
    :param user2:
    :param password2:
    :param dbname2:
    :param tb:
    :return:
    """
    f_sql = ""

    if 'carousel' not in rtb_list and 'pt' in rtb_list:
        f_sql = pt_fact.pt_sql(tb, sql, rtb_list, d_date, pa_list)
    elif 'carousel' in rtb_list and 'pt' in rtb_list:
        rtb_list.remove('carousel')
        f_sql = live_lb_vv_fact.vv_sql(tb, sql, rtb_list, d_date, pa_list)
    elif 'pv' in rtb_list:
        f_sql = pv_fact.pv_sql(tb, sql, rtb_list, d_date, pa_list)
    elif 'uv' in rtb_list:
        f_sql = uv_fact.uv_sql(tb, sql, rtb_list, d_date, pa_list)
    elif 'vv' in rtb_list:
        f_sql = vv_fact.vv_sql(tb, sql, rtb_list, d_date, pa_list)
    elif 'device' in rtb_list:
        f_sql = device_fact.device_sql(tb, sql, rtb_list, d_date, pa_list)

    logging.info("Fact SQL info: %s" % f_sql)
    print f_sql
    f_data = base.select_mode(ip2, port2, user2, password2, dbname2, f_sql, tb)
    return f_data


def main(file, re_db, online_db, fact_db):
    """
    Main function for version
    :param file:
    :param re_db:
    :param online_db:
    :return:
    """
    logger.init_log('./log/test', level=logging.DEBUG)
    ip1, port1, user1, password1, dbname1 = base.parse_conf(file, re_db)
    ip2, port2, user2, password2, dbname2 = base.parse_conf(file, online_db)
    ip3, port3, user3, password3, dbname3 = base.parse_conf(file, fact_db)
    
    #re_tb = base.get_tname(file, re_db)
    re_tb = base.read_table('./config/carousel.txt')
    print "vaule of re_tb is %s:" % re_tb
    for i in range(len(re_tb)):
        d_date = ""
        tb = re_tb[i]
        logging.info("Result Table info: %s" % tb)
        print tb
        rtb_list, rtb_type = base.result_dtable(tb)
        if rtb_type == 'hour':
            d_date = base.date_d(ip1, port1, user1, password1, dbname1, 'hour')
        if rtb_type == 'day':
            d_date = base.date_d(ip1, port1, user1, password1, dbname1, 'day')
        print d_date
        logging.info("Time info: %s" % d_date)
        rtb_list, rtb_type = base.result_dtable(tb)
        if 'chtype' in rtb_list:
            rtb_list.remove('chtype')
        if 'ap' in rtb_list:
            rtb_list.remove('ap')
            rtb_list.append('ap')
        if 'cpn' in rtb_list:
            rtb_list.remove('cpn')
        if 'uvip' in rtb_list:
            rtb_list.remove('uvip')
        if 'duration' in rtb_list:
            rtb_list.remove('duration')
        if 'carousel' in rtb_list:
            rtb_list.remove('carousel')
        logging.info("The vaule of rtb_list is %s" % rtb_list)
        print "The vaule of rtb_list is %s" % rtb_list

        any_val, bny_val, cny_val, dny_val, eny_val, fny_val = base.get_param_val(tb, rtb_list)
        pa_list = [any_val, bny_val, cny_val, dny_val, eny_val, fny_val]
        logging.info("value of pa_list is %s!" % pa_list)
        print "value of pa_list is %s!" % pa_list

        r_data, r_sql = result_data(ip1, port1, user1, password1, dbname1, tb, rtb_list, d_date, pa_list)
        rtb_list, rtb_type = base.result_dtable(tb)
        if 'chtype' in rtb_list:
             rtb_list.remove('chtype')
        if 'ap' in rtb_list:
            rtb_list.remove('ap')
            rtb_list.append('ap')
        if 'cpn' in rtb_list:
            rtb_list.remove('cpn')
        if 'uvip' in rtb_list:
            rtb_list.remove('uvip')
        if 'duration' in rtb_list:
            rtb_list.remove('duration')

        if 'carousel' in rtb_list and 'pt' in rtb_list:
                #rtb_list.remove('carousel')
                #rtb_list.remove('pt')
                print "value of rtb_list in carousel_pt table is %s!" % rtb_list
                print "value of tb is %s" % tb
                print "value of r_sql is %s" % r_sql
                f_data = fact_data(ip3, port3, user3, password3, dbname3, tb, rtb_list, r_sql, d_date, pa_list)
        elif 'carousel' not in rtb_list and 'pt' in rtb_list:
            f_data = fact_data(ip2, port2, user2, password2, dbname2, tb, rtb_list, r_sql, d_date, pa_list)
        else:
            f_data = fact_data(ip3, port3, user3, password3, dbname3, tb, rtb_list, r_sql, d_date, pa_list)

        ff1 = os.path.join('/data1/qibin/workspace/mofang/data', r_data)
        ff2 = os.path.join('/data1/qibin/workspace/mofang/data', f_data)
        rf1 = os.path.join('/data1/qibin/workspace/mofang/data', 'bak' + '_' + r_data.split('/')[-1])
        rf2 = os.path.join('/data1/qibin/workspace/mofang/data', 'bak' + '_' + f_data.split('/')[-1])
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
                fa = base.read_in_chunks(rf1)
                fb = base.read_in_chunks(rf2)
                df1 = base.difference(fa, fb, 0)
                df2 = base.difference(fb, fa, 0)

                if df1.equals(df2) and df1.empty:
                    print "%s Success!" % tb
                    logging.info(" %s Success!" % tb)
                else:
                    print "%s False!" % tb
                    logging.info(" %s False!" % tb)


if __name__ == '__main__':
    file = "./config/database.json"
    re_db = "pre_release_result"
    online_db = "v2_dm_pv_fact"
    fact_db = "dm_pv_fact"
    main(file, re_db, online_db, fact_db)
