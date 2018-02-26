# encoding=utf8
'''
Created on 2018年2月24日

@author: jianlongli
'''
import threading
import time
import sys


class Counter():

    def __init__(self, appkey, sign, counter=35):
        self.appkey = appkey
        self.sign = sign
        self.remain = counter
        self.amount = counter
        self.lock = threading.Lock()

    def get(self):
        self.lock.acquire()
        remain = self.remain
        if(remain > 0):
            self.remain = remain - 1
        self.lock.release()
        if(remain > 0):
            return (self.appkey, self.sign)
        else:
            return ('', '')

    def reset(self):
        self.remain = self.amount

# 单例类


class TicketMgr(object):
    _instance = None

    def load_ticket(self, data_path):
        self.counters = []
        fin = open(data_path, 'r')
        if(not fin):
            sys.stderr.write('open_fail=[],data_path=[%s]\n' % data_path)
            sys.exit(-1)
        for line in fin:
            params = line.split(':')
            if(len(params) != 2):
                sys.stderr.write('invalid_line=[%s]\n', line)
                continue
            self.counters.append(Counter(params[0].strip(), params[1].strip()))

    def reset_ticket(self):
        last_reset_timestamp = 0
        while(True):
            # print("in reset")
            sys.stdout.flush()
            now = int(time.time())
            if(last_reset_timestamp + 60 * 60 > now):
                time.sleep(1)
                continue
            last_reset_timestamp = now
            for counter in self.counters:
                counter.reset()

    def get_ticket(self):
        self.idx = (self.idx + 1) % len(self.counters)
        return self.counters[self.idx].get()

    def __new__(cls, *args, **kw):
        if(not cls._instance):
            # print("in new")
            cls._instance = super(TicketMgr, cls).__new__(cls)
            cls._instance.load_ticket(args[0])
            cls._instance.idx = 0
            timer = threading.Thread(target=cls._instance.reset_ticket)
            # timer.setDaemon(True)
            timer.start()
        return cls._instance

'''
if __name__ == '__main__':
    # 单纯只是为了初始化
    mgr = TicketMgr('/root/data/data_acquisition/ticket')
    for i in range(900):
        # print(id(TicketMgr()))
        print(TicketMgr().get_ticket())
    time.sleep(30)
    for i in range(600):
        # print(id(TicketMgr()))
        print(TicketMgr().get_ticket())
'''
