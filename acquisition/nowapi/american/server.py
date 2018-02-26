# encoding=utf8
'''
Created on 2018年2月25日

@author: jianlongli
'''

import threading
from base import StockCrawler
import sys
from ticket import TicketMgr


class StockServer(object):

    # 参数为tickets和symbols列表的文件路径
    # 需要自己去控制symbol的数量以免超过nowapi的频率限制
    def __init__(self, ticket_path, symbol_path):
        self.symbols = []
        fin = open(symbol_path)
        if(not fin):
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
            crawler = StockCrawler('root', '', 'stock', symbol)
            self.crawlers.append(crawler)
            thread_t = threading.Thread(target=crawler.__get__)
            thread_t.start()
            self.threads.append(thread_t)

if __name__ == '__main__':
    module_name = 'american_stock_acquisition_nowapi'
    server = StockServer("/root/data/%s/tickets" %
                         module_name, "/root/data/%s/symbols" % module_name)

    server.run()
