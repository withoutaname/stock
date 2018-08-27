# encoding=utf8
'''
Created on 2018年2月25日

@author: jianlongli
'''

import threading
from base import StockCrawler
import sys
from ticket import TicketMgr
import argparse


class StockServer(object):

    # 参数为tickets和symbols列表的文件路径
    # 需要自己去控制symbol的数量以免超过nowapi的频率限制
    def __init__(self, ticket_path, symbol_path, log_path):
        self.log_path = log_path
        self.symbols = []
        fin = open(symbol_path)
        if (not fin):
            sys.stderr.write('open_fail=[],symbol_path=[%s]\n' % symbol_path)
            sys.exit(-1)
        for line in fin:
            self.symbols.append(line.strip())

        # 单纯只是为了初始化
        TicketMgr(ticket_path)
        # print(id(TicketMgr(ticket_path)))

    def run(self):
        self.crawlers = []
        self.threads = []
        for symbol in self.symbols:
            crawler = StockCrawler('root', '', 'stock', symbol, self.log_path)
            self.crawlers.append(crawler)
            thread_t = threading.Thread(target=crawler.__get__)
            thread_t.start()
            self.threads.append(thread_t)


if __name__ == '__main__':
    arg_parse = argparse.ArgumentParser()
    arg_parse.add_argument('--log', '-l', type=str, help='log path')
    arg_parse.add_argument('--tickets', '-t', type=str, help='tickets path')
    arg_parse.add_argument('--symbols', '-s', type=str, help='symbols path')
    args = arg_parse.parse_args()
    server = StockServer(args.tickets, args.symbols, args.log)

    server.run()
