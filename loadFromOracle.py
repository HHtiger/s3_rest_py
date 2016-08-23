#!/bin/bash
# -*- coding: utf-8 -*-  
'''
 * ClassName: loadFromOracle
 * Author: he_hu@founder.com.cn
 * Description: 
 * CreateDate: 2016/8/11
 * Version: 1.0
'''

import cx_Oracle
import os
import datetime
import time
import s3


def upload_ZPFJ_PTRYZPXXB(cur, begin_time, end_time):
    sql = "select ID,ZP from ZPFJ_PTRYZPXXB where XT_ZHXGSJ > '%s' and XT_ZHXGSJ <= '%s'" % (begin_time, end_time)

    # print sql

    cur.execute(sql.decode('utf8'))

    for rows in cur:
        pic_id = rows[0]
        pic_content = rows[1]

        local_filename = 'img/%s' % pic_id
        obj_filename = '%s' % pic_id
        print "uploading %s " % local_filename
        file = open(local_filename, "wb")
        file.write(pic_content.read())
        file.close()
        client.upload_file(local_filename, 'ptryzp', obj_filename, s3.X_AMZ_ACL.public_read)
        # os.remove(local_filename)
        print "upload finished .. "


def upload_ZPFJ_FJXXB(cur, begin_time, end_time):
    sql = "select ID,WJ from ZPFJ_FJXXB where XT_ZHXGSJ > '%s' and XT_ZHXGSJ <= '%s'" % (begin_time, end_time)

    # print sql

    cur.execute(sql.decode('utf8'))

    for rows in cur:

        file_id = rows[0]
        file_content = rows[1]

        if file_content is None:
            print "ZPFJ_FJXXB file content is error by id : %s" % file_id
            continue

        local_filename = 'img/%s' % file_id
        obj_filename = '%s' % file_id
        print "uploading %s " % local_filename

        file = open(local_filename, "wb")
        file.write(file_content.read())
        file.close()
        client.upload_file(local_filename, 'fjxxb', obj_filename, s3.X_AMZ_ACL.public_read)
        # os.remove(local_filename)
        print "upload finished .. "


if __name__ == '__main__':

    client = s3.S3Client('access_key', 'secret_access_key')
    s3.end_point = "s3.end_point"

    for bucket in client.list_buckets()[1]:
        print bucket.name

    if False:  # 当Oracle服务是手动运行时，将False改为True
        os.system('net start OracleVssWriterORCL')
        os.system('net start OracleDBConsoleorcl')
        os.system('net start OracleOraDb11g_home1TNSListener')
        os.system('net start OracleServiceORCL')

    orcl = cx_Oracle.connect('username', 'pass', 'ip:port/sid')
    cur = orcl.cursor()

    now_time = datetime.datetime.now()
    while 1:
        begin_time = now_time + datetime.timedelta(minutes=-5)
        end_time = datetime.datetime.now() + datetime.timedelta(minutes=-2)
        now_time = end_time

        # print datetime.datetime.now().strftime('%Y_%m_%d %H:%M:%S')
        # print "1 ",begin_time.strftime('%Y_%m_%d %H:%M:%S')
        # print "2 ",end_time.strftime('%Y_%m_%d %H:%M:%S')

        upload_ZPFJ_PTRYZPXXB(cur, begin_time.strftime('%Y_%m_%d %H:%M:%S'), end_time.strftime('%Y_%m_%d %H:%M:%S'))

        upload_ZPFJ_FJXXB(cur, begin_time.strftime('%Y_%m_%d %H:%M:%S'), end_time.strftime('%Y_%m_%d %H:%M:%S'))

        time.sleep(3)

    cur.close()
