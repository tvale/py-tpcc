# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

import logging
import time
import constants
from hdrh.histogram import HdrHistogram

class Results:
    
    def __init__(self):
        self.start = None
        self.stop = None
        self.txn_id = 0
        self.txn_counters = {
          constants.TransactionTypes.DELIVERY:     0,
          constants.TransactionTypes.NEW_ORDER:    0,
          constants.TransactionTypes.ORDER_STATUS: 0,
          constants.TransactionTypes.PAYMENT:      0,
          constants.TransactionTypes.STOCK_LEVEL:  0,
        }
        self.txn_times = {
          constants.TransactionTypes.DELIVERY:     HdrHistogram(1, 1000000, 3),
          constants.TransactionTypes.NEW_ORDER:    HdrHistogram(1, 1000000, 3),
          constants.TransactionTypes.ORDER_STATUS: HdrHistogram(1, 1000000, 3),
          constants.TransactionTypes.PAYMENT:      HdrHistogram(1, 1000000, 3),
          constants.TransactionTypes.STOCK_LEVEL:  HdrHistogram(1, 1000000, 3),
        }
        self.txn_aborts = {
          constants.TransactionTypes.DELIVERY:     0,
          constants.TransactionTypes.NEW_ORDER:    0,
          constants.TransactionTypes.ORDER_STATUS: 0,
          constants.TransactionTypes.PAYMENT:      0,
          constants.TransactionTypes.STOCK_LEVEL:  0,
        }
        self.running = { }
        
    def startBenchmark(self):
        """Mark the benchmark as having been started"""
        assert self.start == None
        logging.debug("Starting benchmark statistics collection")
        self.start = time.time()
        return self.start
        
    def stopBenchmark(self):
        """Mark the benchmark as having been stopped"""
        assert self.start != None
        assert self.stop == None
        logging.debug("Stopping benchmark statistics collection")
        self.stop = time.time()
        
    def startTransaction(self, txn):
        self.txn_id += 1
        id = self.txn_id
        self.running[id] = (txn, time.time())
        return id
        
    def abortTransaction(self, id):
        """Abort a transaction and discard its times"""
        assert id in self.running
        txn_name, txn_start = self.running[id]
        # del self.running[id]
        aborts = self.txn_aborts.get(txn_name, 0)
        self.txn_aborts[txn_name] = aborts + 1

    def stopTransaction(self, id, measure):
        """Record that the benchmark completed an invocation of the given transaction"""
        assert id in self.running
        txn_name, txn_start = self.running[id]
        del self.running[id]
        
        if measure:
            duration = time.time() - txn_start
            hdr = self.txn_times[txn_name]
            hdr.record_value(duration * 1000000) # microsecs
            
            total_cnt = self.txn_counters.get(txn_name, 0)
            self.txn_counters[txn_name] = total_cnt + 1
        
    def append(self, r):
        for txn_name in r.txn_counters.keys():
            orig_cnt = self.txn_counters.get(txn_name, 0)
            orig_hdr = self.txn_times[txn_name]
            orig_aborts = self.txn_aborts.get(txn_name, 0)
            self.txn_counters[txn_name] = orig_cnt + r.txn_counters.get(txn_name, 0)
            orig_hdr.decode_and_add(r.txn_times[txn_name])
            self.txn_aborts[txn_name] = orig_aborts + r.txn_aborts.get(txn_name, 0)
            #logging.debug("%s [cnt=%d, time=%d]" % (txn_name, self.txn_counters[txn_name], self.txn_times[txn_name]))
        ## HACK
        self.start = r.start
        self.stop = r.stop
            
    def __str__(self):
        return self.show()

    def show(self, load_time = None, total_clients = 1):
        if self.start == None:
            return "Benchmark not started"
        if self.stop == None:
            duration = time.time() - self.start
        else:
            duration = self.stop - self.start
        
        col_width = 16
        total_width = (col_width*5)+2
        f = "\n  " + (("%-" + str(col_width) + "s")*5)
        line = "-"*total_width

        ret = u"" + "="*total_width + "\n"
        if load_time != None:
            ret += "Data Loading Time: %d seconds\n\n" % (load_time)
        ret += "Execution Results after %d seconds\n%s" % (duration, line)
        ret += f % ("", "Executed", "Aborts", u"Time (µs)", "Rate")
        total_time = 0
        total_cnt = 0
        total_abort = 0
        for txn in sorted(self.txn_counters.keys()):
            txn_time = self.txn_times[txn] / total_clients
            txn_cnt = self.txn_counters[txn]
            txn_abort = self.txn_aborts[txn]
            rate = u"%.02f txn/s" % ((txn_cnt / txn_time))
            ret += f % (txn, str(txn_cnt), str(txn_abort), str(txn_time * 1000000), rate)
            total_time += txn_time
            total_cnt += txn_cnt
            total_abort += txn_abort
        ret += "\n" + ("-"*total_width)
        total_rate = "%.02f txn/s" % ((total_cnt / total_time))
        ret += f % ("TOTAL", str(total_cnt), str(total_abort), str(total_time * 1000000), total_rate)
        return (ret.encode('utf-8'))
## CLASS
