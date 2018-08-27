# encoding=utf8
'''
Created on 2017年11月27日

@author: jianlongli
'''
from datetime import datetime
import time
import MySQLdb
import httplib
import urllib
import json
import logging
from ticket import TicketMgr
from logging import handlers
import sys


class StockCrawler:

    def __init__(self, username, passwd, database, symbol, log_path):
        self.last_timestamp = 0
        self.db = MySQLdb.connect("localhost", username, passwd, database)
        self.db.set_character_set('utf8')
        self.cur = self.db.cursor()
        # 保持连接，避免服务器断开
        self.cur.execute("set wait_timeout=655350")
        # 股票代码
        self.symbol = symbol
        self.host = 'api.k780.com'

        # 初始化日志设置
        self.log_file = log_path
        self.log_handler = handlers.TimedRotatingFileHandler(
            self.log_file, 'D', 1, 7)
        self.log_format = "%(asctime)s %(filename)s %(lineno)d %(message)s"
        self.log_formatter = logging.Formatter(self.log_format)
        self.log_handler.setFormatter(self.log_formatter)
        self.logger = logging.getLogger(self.symbol)
        self.logger.addHandler(self.log_handler)

    def __get__(self):
        while True:
            try:
                conn = httplib.HTTPConnection(self.host)
                # print(id(TicketMgr()))
                # (app_key,sign) = TicketMgr().get_ticket()
                ticket = TicketMgr().get_ticket()
                app_key = ticket[0]
                sign = ticket[1]
                if(app_key == '' or sign == ''):
                    self.logger.error(
                        '%s:invalid_ticket=[],app_key=[%s],sign=[%s]' % (self.symbol, app_key, sign))
                    continue
                params = urllib.urlencode({'appkey': app_key, 'sign': sign, 'app': 'finance.stock_realtime',
                                           'symbol': self.symbol, 'format': 'json'})
                headers = {"Content-type": "application/x-www-form-urlencoded"}
                conn.request('POST', '', params, headers)
                resp = conn.getresponse().read()
                # print(resp)
                jsondata = json.loads(resp)
                sdata = jsondata["result"]["lists"][self.symbol]
                stoid = sdata["stoid"]
                symbol = sdata["symbol"]
                scode = sdata["scode"]
                sname = sdata["sname"]
                sname_eng = sdata["sname_eng"]
                open_price = float(sdata["open_price"])
                yesy_price = float(sdata["yesy_price"])
                last_price = float(sdata["last_price"])
                high_price = float(sdata["high_price"])
                low_price = float(sdata["low_price"])
                rise_fall = float(sdata["rise_fall"])
                rise_fall_per = float(sdata["rise_fall_per"])
                volume = float(sdata["volume"])
                turn_volume = float(sdata["turn_volume"])
                peratio = float(sdata["peratio"])
                week52_high = float(sdata["week52_high"])
                week52_low = float(sdata["week52_low"])
                day10_volume = float(sdata["day10_volume"])
                mvalue = float(sdata["mvalue"])
                ep_share = float(sdata["ep_share"])
                beta_coefficient = float(sdata["beta_coefficient"])
                dividend = float(sdata["dividend"])
                yieldratio = float(sdata["yield"])
                equity = float(sdata["equity"])
                db_price = float(sdata["db_price"])
                db_volume = float(sdata["db_volume"])
                uptime = sdata["uptime"]
                date_time = datetime.strptime(uptime, "%Y-%m-%d %H:%M:%S")
                #print("datetime=%s" % uptime)
                timestamp = int(time.mktime(date_time.timetuple()))
                now = int(time.time())
                if(timestamp <= self.last_timestamp or timestamp >= now):
                    self.logger.warning("%s:invalid time=[],last time=[%s],uptime=[%s],now=[%s]" % (self.symbol, time.asctime(datetime.fromtimestamp(
                        self.last_timestamp).timetuple()), uptime, time.asctime(datetime.fromtimestamp(now).timetuple())))
                    time.sleep(60)
                    continue
                sql_str = """INSERT INTO AmericanStock (stoid,symbol,scode,sname,sname_eng,open_price,
					yesy_price,last_price,high_price,low_price,rise_fall,rise_fall_per,volume,turn_volume,
					peratio,week52_high,week52_low,day10_volume,mvalue,ep_share,beta_coefficient,dividend,
					yield,equity,db_price,db_volume,uptime,timestamp) values ('%s','%s','%s','%s','%s',%f,
					%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,'%s',%d)""" % (stoid, symbol,
                                                                                scode, sname, sname_eng, open_price, yesy_price, last_price, high_price, low_price, rise_fall,
                                                                                rise_fall_per, volume, turn_volume, peratio, week52_high, week52_low, day10_volume, mvalue,
                                                                                ep_share, beta_coefficient, dividend, yieldratio, equity, db_price, db_volume, uptime, timestamp)
                # print(sql_str)
                num = self.cur.execute(sql_str)
                if(num > 0):
                    self.db.commit()
                    self.last_timestamp = timestamp
                    self.logger.warning(sql_str)
                else:
                    self.logger.warning(
                        "insert into table failed,mysql = %s" % sql_str)
            except Exception as e:
                self.logger.error("%s:some error occur,%s" %
                                  (self.symbol, str(e)))
            time.sleep(60)

'''
if __name__ == '__main__':
    momo = StockCrawler('root', '', 'stock', 'gb_momo')
    momo.__get__()
'''
