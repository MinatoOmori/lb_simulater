from IPython.core.debugger import Pdb
import time
import random

class Request:
    def __init__(self, cload, ctime, gload, arrival, i):
        self.cload = cload
        self.ctime = ctime
        self.gload = gload
        self.lb_arrival = arrival
        self.id = i
        self.server_id = 0
        print("cload: %s ctime %s gload %s arrival %s"\
              %(cload, ctime, gload, arrival))
    def set_finish_time(self, current_time):
        self.server_arrival = current_time
        self.finish = current_time+self.ctime