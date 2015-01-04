#!/opt/vertica/oss/python/bin/python
__author__ = 'xopc'

import pyodbc
import os
import logging
import sys
import random
import config
import argparse


MYSQL_DSN="MSSQL"
#MYSQL_DSN=sys.argv[1]
logfile = "/home/dbadmin/leo/mon/mon.log"
logging.basicConfig(format='[%(asctime)s] %(levelname)s : %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filename=logfile,
                    level=logging.INFO)
logger = logging.getLogger('MON')

#proc_id = "%s_%s" % (os.getpid(), random.randint(1, 10000))
#pidfile = "/data/etl/etl_%s.pid" % MYSQL_DSN
#input_path = "/data/input/"
#work_dir = "/data/etl/work/"
#error_dir = "/data/etl/error/"
#done_dir = "/data/etl/done/"
#rej_table = "rejected"
#NOTOK=108
#OK=109
alarms=config.alarms
msg=config.msg
drill=config.drill





class Vertica(object):
    """docstring for conn"""

    def __init__(self):
        super(Vertica, self).__init__()
        os.environ['VERTICAINI'] = '/etc/vertica.ini'
        try:
            self.cn = pyodbc.connect('DSN=vertica;PWD=OPVERTICA', ansi=True)
            self.cn.autocommit = False
            self.cursor = self.cn.cursor()
        except Exception, e:
            logging.fatal(e)
            logging.info("-------------------------------------")
            logging.shutdown()
#            os.remove(pidfile)
            sys.exit(-1)

    def execute(self, query):
        try:
            #logging.debug(query)
            rows = self.cursor.execute(query).rowcount
            desc = self.cursor.description
            output = []
            if desc is None:
                return 0, output, rows

            try:
                #output = self.cursor.fetchall()

                while True:
                    row = self.cursor.fetchone()
                    if row is None:
                        dummy = self.cursor.fetchall()
                        break
                    output.append(row)


            except Exception, e:
                logging.debug("Exception: %s" % e)
                print ("Exception: %s" % e)
                output = [(None, )]
            return 0, output, len(output)
        except Exception, e:
            logging.error("Exception: %s" %e)
            print ("Exception: %s" %e)
            return -1, str(e).replace("'", '')

    def insert_many(self, query,rows):
        try:
            #logging.debug(query)
            rows = self.cursor.executemany(query,rows)
            self.commit()
        except Exception, e:
            logging.error(e)
            return -1, str(e).replace("'", '') 


    def commit(self):
        self.cn.commit()
        logging.info("VERTICA: COMMIT")

    def close(self):
        self.cn.close()

    def rollback(self):
        self.cn.rollback()


#TODO: rewrite to normal code
class Mssql(object):
    """docstring for conn"""

    def __init__(self):
        try:
            self.cn = pyodbc.connect('DSN={0};UID=OptimalAdmin;PWD=xdr5%RDX'.format(MYSQL_DSN), autocommit=True, ansi=True)
            # self.cn.autocommit = True
            self.cursor = self.cn.cursor()
        except Exception, e:
            logging.fatal(e)
            logging.info("-------------------------------------")
            logging.shutdown()
#            os.remove(pidfile)
            sys.exit(-1)

    def execute(self, query):
        try:
            self.cn = pyodbc.connect('DSN={0};UID=OptimalAdmin;PWD=xdr5%RDX'.format(MYSQL_DSN), autocommit=True, ansi=True)
            # self.cn.autocommit = True
            self.cursor = self.cn.cursor()
        except Exception, e:
            logging.fatal(e)
            logging.info("-------------------------------------")
            logging.shutdown()
            #os.remove(pidfile)
            sys.exit(-1)
        try:
            # print query
            rows = self.cursor.execute(query).rowcount
            try:
                output = self.cursor.fetchall()
            except Exception, e:
                # print e
                output = [(None, )]
            RC = 0, output, rows
        except Exception, e:
            RC = -1, e, ""
        self.close()
        return RC

    def insert_many(self, query,rows):
        try:
            logging.debug(query)
            rows = self.cursor.executemany(query,rows)
            self.commit()
        except Exception, e:
            logging.error(e)
            return -1, str(e).replace("'", '') 

    def commit(self):
        self.cn.commit()


    def close(self):
        # self.cn.commit()
        self.cn.close()

def mon_alarms():
# sample: alarms ,[REJECTED_RESOURCES_COUNT]
    if(args.monType == 'alarms'):
        for k in config.mon:
            if(not args.monElement.__contains__(k) ):
                continue
            logging.info(k)
            sql = config.mon[k]['query'].replace("\n", " ") % {'MAX': config.mon[k]['threshold'],
                                                             'INTERVAL': config.mon[k]['interval']}
            #logging.info(sql)
            o = vert.execute(sql)
            if o[0] == 0:
                logging.info(o[1][0][0])
                #print('')
# sample: perf ,[QUERY_PERFORMANCE,QUERY_EVENTS]
# sample: perf ,[RESOURCE_QUEUES]
    if(args.monType == 'perf'):
        for k in config.perf:
            if(not args.monElement.__contains__(k) ):
            #if(k<>args.monElement):
                continue

            logging.info(k)
            cperf = config.perf[k]
#            sql = cperf['query'].replace("\n", " ") #% {'MAX': cperf['threshold'],
                                                    #         'INTERVAL': cperf['interval']}
            if("select" in cperf['interval']):
                sql = cperf['interval'] % cperf['sqltable']
                o = vert.execute(sql)
                if (o[0] == 0 and o[2] > 0 and len(o[1]) > 0):
                    cperf['interval'] = o[1][0][0]
                else:
                    cperf['interval'] = " sysdate() - interval '30 minute' ";

                sql = cperf['query'] % cperf['interval']
            else:
                sql = cperf['query']
            #logging.info(sql)
            o = vert.execute(sql)
            if (o[0] == 0 and o[2] == 0):
                #print("No Rows Returned")
                logging.info("No Rows Returned")
            if (o[0] == 0 and o[2] > 0 and len(o[1]) == 0):
                #print("%d Rows Returned" % o[2])
                logging.info("%d Rows Returned" % o[2])

            if (o[0] == 0 and o[2] > 0 and len(o[1]) > 0):
                rows = o[1]
                logging.info("Rows returned %d" % o[2])
                #print("Rows returned %d" % o[2])
                num_col = len(rows[0])
                #print(len(rows[0]))
                for col in range(num_col):
                    if col==0:
                        question = "?"
                    else:
                        question = question + ",?"
                sql_insert = """insert into %s values(%s) """ %(cperf['sqltable'], question)
                logging.info(sql_insert)
                #sql_insert = cperf['sqlinsert'] ;
                #print sql_insert
                if(cperf['connection'] == 'mssql'):
                    o = mssql.insert_many(sql_insert,rows)
                if(cperf['connection'] == 'vertica'):
                    o = vert.insert_many(sql_insert,rows)


                # for row in rows:
                    # for column in row:
                       # logging.info( str(column[0]) )

def arg_validation():
# example: ./mon.py perf QUERY_PERFORMANCE
# example: ./mon.py alarms MAX_SESSIONS_PERCENT
    parser = argparse.ArgumentParser('mon')
    parser.add_argument('monType', help="Monitor Type")
    parser.add_argument('monElement', help="Monitor Element")
    args = parser.parse_args()
    #print(args)
    monTypes = ['alarms','perf']
    if not monTypes.__contains__(args.monType):
        print("Not a valid Monitor Element - it should be one of the following: %s" %monTypes)
        sys.exit(-1)

    monElements = config.mon.keys()

    if not monTypes.__contains__(args.monType):
        print("Not a valid Monitor Element - it should be one of the following: %s" %monElements)
        sys.exit(-1)
    return args



args=arg_validation()

vert = Vertica()
mssql = Mssql()
#b = Mssql()
# OK = False

o = vert.execute("select local_node_name();")
if o[0] == 0:
    node_name = o[1][0][0]
else:
    logging.error("Can't get local node name: %s" % o[2])
    print("Can't get local node name: %s" % o[2])
    logging.info("-------------------------------------")
    logging.shutdown()
#    os.remove(pidfile)
    sys.exit(-1)

logging.info("Monitor process runs on %s" % node_name)
#print("Monitor process runs on %s" % node_name)
logging.info("----------------------------------------")
mon_alarms()


logging.shutdown()


# print a.execute("select * from %s" % rej_table)



