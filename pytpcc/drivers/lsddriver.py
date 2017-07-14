# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2016--2017
# Tiago Vale
# http://tvale.github.io
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
from datetime import datetime
import tpcc_pb2
import google.protobuf.text_format as pb
from pymemcache.client.base import Client as memcached
import uuid
import constants
from abstractdriver import *

## ==============================================
## LSDDriver
## ==============================================
class LsdDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        'host': ('server host', 'localhost'),
        'port': ('server port', '11211'),
        'clients': ('client hosts', 'node1 node2'),
        'path': ('path to pytpcc code on client nodes', '/home/ubuntu/lsd/bench/tpcc/pytpcc'),
    }

    def __init__(self, ddl):
        super(LsdDriver, self).__init__('lsd', ddl)
        self.name = 'lsd'

    def makeDefaultConfig(self):
        """This function needs to be implemented by all sub-classes.
            It should return the items that need to be in your implementation's configuration file.
            Each item in the list is a triplet containing: ( <PARAMETER NAME>, <DESCRIPTION>, <DEFAULT VALUE> )
        """
        return LsdDriver.DEFAULT_CONFIG

    def loadConfig(self, config):
        """Initialize the driver using the given configuration dict"""
        self.client = memcached((config['host'], int(config['port'])), no_delay=False)

    def loadStart(self):
        """Optional callback to indicate to the driver that the data loading phase is about to begin."""
        return None

    def loadFinish(self):
        """Optional callback to indicate to the driver that the data loading phase is finished."""
        return None

    def loadFinishItem(self):
        """Optional callback to indicate to the driver that the ITEM data has been passed to the driver."""
        return None

    def loadFinishWarehouse(self, w_id):
        """Optional callback to indicate to the driver that the data for the given warehouse is finished."""
        return None

    def loadFinishDistrict(self, w_id, d_id):
        """Optional callback to indicate to the driver that the data for the given district is finished."""
        return None

    def loadTuples(self, tableName, tuples):
        """Load a list of tuples into the target table"""
        if len(tuples) == 0:
            return
        if tableName == constants.TABLENAME_WAREHOUSE:
            if len(tuples[0]) != 9:
                return
            for row in tuples:
                w_id = int(row[0])
                w = tpcc_pb2.warehouse()
                w.name = str(row[1])
                w.street_1 = str(row[2])
                w.street_2 = str(row[3])
                w.city = str(row[4])
                w.state = str(row[5])
                w.zip = str(row[6])
                w.tax = float(row[7])
                key = self.__w_key(w_id, '')
                w = pb.MessageToString(w)
                self.client.set(key, w, noreply=False)
                w_ytd = row[8]
                key = self.__w_key(w_id, 'ytd')
                self.client.set(key, str(w_ytd), noreply=False)
        elif tableName == constants.TABLENAME_DISTRICT:
            for row in tuples:
                d_id = int(row[0])
                d = tpcc_pb2.district()
                d_w_id = row[1]
                d.w_id = int(d_w_id)
                d.name = str(row[2])
                d.street_1 = str(row[3])
                d.street_2 = str(row[4])
                d.city = str(row[5])
                d.state = str(row[6])
                d.zip = str(row[7])
                d.tax = float(row[8])
                key = self.__d_key(d_id, d_w_id, '')
                d = pb.MessageToString(d)
                self.client.set(key, d, noreply=False)
                d_ytd = row[9]
                key = self.__d_key(d_id, d_w_id, 'ytd')
                self.client.set(key, str(d_ytd), noreply=False)
                d_next_o_id = int(row[10])
                key = self.__d_key(d_id, d_w_id, 'next_o_id')
                self.client.set(key, str(d_next_o_id), noreply=False)
        elif tableName == constants.TABLENAME_CUSTOMER:
            index = {}
            for row in tuples:
                # customer
                c_id = row[0]
                c = tpcc_pb2.customer()
                c_d_id = row[1]
                c.d_id = int(c_d_id)
                c_w_id = row[2]
                c.w_id = int(c_w_id)
                c.first = str(row[3])
                c.middle = str(row[4])
                c_last = row[5]
                c.last = str(c_last)
                c.street_1 = str(row[6])
                c.street_2 = str(row[7])
                c.city = str(row[8])
                c.state = str(row[9])
                c.zip = str(row[10])
                c.phone = str(row[11])
                c.since = str(row[12])
                c.credit = str(row[13])
                c.credit_lim = str(row[14])
                c.discount = float(row[15])
                key = self.__c_key(c_id, c_d_id, c_w_id, '')
                c = pb.MessageToString(c)
                self.client.set(key, c, noreply=False)
                c_balance = row[16]
                key = self.__c_key(c_id, c_d_id, c_w_id, 'balance')
                self.client.set(key, str(c_balance), noreply=False)
                c_ytd_payment = row[17]
                key = self.__c_key(c_id, c_d_id, c_w_id, 'ytd_payment')
                self.client.set(key, str(c_ytd_payment), noreply=False)
                c_payment_cnt = row[18]
                key = self.__c_key(c_id, c_d_id, c_w_id, 'payment_cnt')
                self.client.set(key, str(c_payment_cnt), noreply=False)
                c_delivery_cnt = row[19]
                key = self.__c_key(c_id, c_d_id, c_w_id, 'delivery_cnt')
                self.client.set(key, str(c_delivery_cnt), noreply=False)
                c_data = row[20]
                key = self.__c_key(c_id, c_d_id, c_w_id, 'data')
                self.client.set(key, str(c_data), noreply=False)
                # index on (c_d_id, c_w_id, c_last) for payment
                key = self.__c_index_key(c_d_id, c_w_id, c_last)
                exists = (key in index)
                if exists:
                    index[key].append(c_id)
                else:
                    index[key] = [c_id]
            # update index
            for k, v in index.iteritems():
                value = self.client.get(k)
                exists = value is not None
                l = tpcc_pb2.customer_index()
                if exists:
                    pb.Merge(value, l) # l.ParseFromString(value)
                l.c_id.extend(v)
                l = pb.MessageToString(l)
                self.client.set(k, l, noreply=False)
        elif tableName == constants.TABLENAME_HISTORY:
            for row in tuples:
                h_uuid = uuid.uuid1()
                h = tpcc_pb2.history()
                h_c_id = row[0]
                h.c_id = int(h_c_id)
                h.c_d_id = int(row[1])
                h_c_w_id = row[2]
                h.c_w_id = int(h_c_w_id)
                h.d_id = int(row[3])
                h_w_id = row[4]
                h.w_id = int(h_w_id)
                h.date = str(row[5])
                h.amount = float(row[6])
                h.data = str(row[7])
                key = self.__h_key(h_uuid, h_c_id, h_c_w_id, h_w_id, '')
                h = pb.MessageToString(h)
                self.client.set(key, h, noreply=False)
        elif tableName == constants.TABLENAME_NEW_ORDER:
            index = {}
            for row in tuples:
                no_o_id = row[0]
                no_d_id = row[1]
                no_w_id = row[2]
                key = self.__no_key(no_o_id, no_d_id, no_w_id, '')
                self.client.set(key, str(no_o_id), noreply=False)
                # index on (no_d_id, no_w_id) for delivery
                key = self.__no_index_key(no_d_id, no_w_id)
                exists = key in index
                if (not exists) or (no_o_id < index[key]):
                    index[key] = no_o_id
            # update index
            for k, v in index.iteritems():
                value = self.client.get(k)
                exists = value is not None
                if (not exists) or (v < int(value)):
                    self.client.set(k, str(v), noreply=False)
        elif tableName == constants.TABLENAME_ORDERS:
            index = {}
            for row in tuples:
                o_id = row[0]
                o = tpcc_pb2.order()
                o_d_id = row[2]
                o.d_id = int(o_d_id)
                o_w_id = row[3]
                o.w_id = int(o_w_id)
                o_c_id = row[1]
                o.c_id = int(o_c_id)
                o.entry_d = str(row[4])
                o.ol_cnt = int(row[6])
                o.all_local = bool(row[7])
                key = self.__o_key(o_id, o_d_id, o_w_id, '')
                o = pb.MessageToString(o)
                self.client.set(key, o, noreply=False)
                o_carrier_id = row[5]
                key = self.__o_key(o_id, o_d_id, o_w_id, 'carrier_id')
                self.client.set(key, str(o_carrier_id), noreply=False)
                # index on (o_d_id, o_w_id, o_c_id) for order-status
                key = self.__o_index_key(o_d_id, o_w_id, o_c_id)
                index[key] = o_id
            # update index
            for k, v in index.iteritems():
                self.client.set(k, str(v), noreply=False)
        elif tableName == constants.TABLENAME_ORDER_LINE:
            for row in tuples:
                ol_o_id = row[0]
                ol_d_id = row[1]
                ol_w_id = row[2]
                ol_number = row[3]
                ol = tpcc_pb2.order_line()
                ol_i_id = row[4]
                ol.i_id = int(ol_i_id)
                ol.supply_w_id = int(row[5])
                ol.quantity = int(row[7])
                ol.amount = float(row[8])
                ol.dist_info = str(row[9])
                key = self.__ol_key(ol_number, ol_o_id, ol_d_id, ol_w_id, '')
                ol = pb.MessageToString(ol)
                self.client.set(key, ol, noreply=False)
                ol_delivery_d = row[6]
                key = self.__ol_key(ol_number, ol_o_id, ol_d_id, ol_w_id, 'delivery_d')
                self.client.set(key, str(ol_delivery_d), noreply=False)
        elif tableName == constants.TABLENAME_ITEM:
            for row in tuples:
                i_id = row[0]
                i = tpcc_pb2.item()
                i.im_id = int(row[1])
                i.name = str(row[2])
                i.price = float(row[3])
                i.data = str(row[4])
                key = self.__i_key(i_id, '')
                i = pb.MessageToString(i)
                self.client.set(key, i, noreply=False)
        elif tableName == constants.TABLENAME_STOCK:
            for row in tuples:
                s_i_id = row[0]
                s_w_id = row[1]
                s = tpcc_pb2.stock()
                s.dist_01 = str(row[3])
                s.dist_02 = str(row[4])
                s.dist_03 = str(row[5])
                s.dist_04 = str(row[6])
                s.dist_05 = str(row[7])
                s.dist_06 = str(row[8])
                s.dist_07 = str(row[9])
                s.dist_08 = str(row[10])
                s.dist_09 = str(row[11])
                s.dist_10 = str(row[12])
                s.data = str(row[16])
                key = self.__s_key(s_i_id, s_w_id, '')
                s = pb.MessageToString(s)
                self.client.set(key, s, noreply=False)
                s_quantity = row[2]
                key = self.__s_key(s_i_id, s_w_id, 'quantity')
                self.client.set(key, str(s_quantity), noreply=False)
                s_ytd = row[13]
                key = self.__s_key(s_i_id, s_w_id, 'ytd')
                self.client.set(key, str(s_ytd), noreply=False)
                s_order_cnt = row[14]
                key = self.__s_key(s_i_id, s_w_id, 'order_cnt')
                self.client.set(key, str(s_order_cnt), noreply=False)
                s_remote_cnt = row[15]
                key = self.__s_key(s_i_id, s_w_id, 'remote_cnt')
                self.client.set(key, str(s_remote_cnt), noreply=False)
        else:
            raise Exception('Unknown table: {}'.format(tableName))

    def executeStart(self):
        """Optional callback before the execution phase starts"""
        return None

    def executeFinish(self):
        """Callback after the execution phase finishes"""
        return None

    def doDelivery(self, params):
        """
        Execute DELIVERY Transaction
        Parameters Dict:
            w_id
            o_carrier_id
            ol_delivery_d
        """
        w_id = params['w_id']
        o_carrier_id = params['o_carrier_id']
        ol_delivery_d = params['ol_delivery_d']
        args = tpcc_pb2.delivery_args()
        args.w_id = w_id
        args.o_carrier_id = o_carrier_id
        args.ol_delivery_d = str(ol_delivery_d)
        args = pb.MessageToString(args)
        self.client.add('tpcc.delivery', args, noreply=False)
        return 1

    def doNewOrder(self, params):
        """
        Execute NEW_ORDER Transaction
        Parameters Dict:
            w_id
            d_id
            c_id
            o_entry_d
            i_ids
            i_w_ids
            i_qtys
        """
        w_id = params['w_id']
        d_id = params['d_id']
        c_id = params['c_id']
        o_entry_d = params['o_entry_d']
        i_ids = params['i_ids']
        i_w_ids = params['i_w_ids']
        i_qtys = params['i_qtys']
        assert constants.MIN_OL_CNT <= len(i_ids)
        assert len(i_ids) <= constants.MAX_OL_CNT
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)
        assert len(set(i_ids)) == len(i_ids)
        args = tpcc_pb2.new_order_args()
        args.w_id = w_id
        args.d_id = d_id
        args.c_id = c_id
        args.o_entry_d = str(o_entry_d)
        args.i_ids.extend(i_ids)
        args.i_w_ids.extend(i_w_ids)
        args.i_qtys.extend(i_qtys)
        args = pb.MessageToString(args)
        self.client.add('tpcc.new_order', args, noreply=False)
        return 1

    def doOrderStatus(self, params):
        """
        Execute ORDER_STATUS Transaction
        Parameters Dict:
            w_id
            d_id
            c_id
            c_last
        """
        w_id = params['w_id']
        d_id = params['d_id']
        c_id = params['c_id']
        c_last = params['c_last']
        assert (c_id != None and c_last == None) or (c_id == None and c_last != None)
        args = tpcc_pb2.order_status_args()
        args.w_id = w_id;
        args.d_id = d_id;
        if c_id is not None:
            args.c_id = c_id
        else:
            args.c_last = c_last
        args = pb.MessageToString(args)
        self.client.add('tpcc.order_status', args, noreply=False)
        return 1

    def doPayment(self, params):
        """
        Execute PAYMENT Transaction
        Parameters Dict:
            w_id
            d_id
            h_amount
            c_w_id
            c_d_id
            c_id
            c_last
            h_date
        """
        w_id = params['w_id']
        d_id = params['d_id']
        h_amount = params['h_amount']
        c_w_id = params['c_w_id']
        c_d_id = params['c_d_id']
        c_id = params['c_id']
        c_last = params['c_last']
        h_date = params['h_date']
        assert (c_id != None and c_last == None) or (c_id == None and c_last != None)
        args = tpcc_pb2.payment_args()
        args.w_id = w_id
        args.d_id = d_id
        args.h_amount = h_amount
        args.h_date = str(h_date)
        args.c_w_id = c_w_id
        args.c_d_id = c_d_id
        if c_id is not None:
            args.c_id = c_id
        else:
            args.c_last = c_last
        args = pb.MessageToString(args)
        self.client.add('tpcc.payment', args, noreply=False)
        return 1

    def doStockLevel(self, params):
        """
        Execute STOCK_LEVEL Transaction
        Parameters Dict:
            w_id
            d_id
            threshold
        """
        w_id = params['w_id']
        d_id = params['d_id']
        threshold = params['threshold']
        # TODO

    def __w_key(self, w_id, field):
        k = 'w-{}-{}'
        return k.format(w_id, field)
    def __d_key(self, d_id, d_w_id, field):
        k = 'd-{}_{}-{}'
        return k.format(d_id, d_w_id, field)
    def __c_key(self, c_id, c_d_id, c_w_id, field):
        k = 'c-{}_{}_{}-{}'
        return k.format(c_id, c_d_id, c_w_id, field)
    def __c_index_key(self, c_d_id, c_w_id, c_last):
        k = 'ci-{}_{}_{}'
        return k.format(c_d_id, c_w_id, c_last)
    def __h_key(self, h_uuid, h_c_id, h_c_w_id, h_w_id, field):
        k = 'h-{}_{}_{}_{}-{}'
        return k.format(h_uuid, h_c_id, h_c_w_id, h_w_id, field)
    def __no_key(self, no_o_id, no_d_id, no_w_id, field):
        k = 'no-{}_{}_{}-{}'
        return k.format(no_o_id, no_d_id, no_w_id, field)
    def __no_index_key(self, no_d_id, no_w_id):
        k = 'noi-{}_{}'
        return k.format(no_d_id, no_w_id)
    def __o_key(self, o_id, o_d_id, o_w_id, field):
        k = 'o-{}_{}_{}-{}'
        return k.format(o_id, o_d_id, o_w_id, field)
    def __o_index_key(self, o_d_id, o_w_id, o_c_id):
        k = 'oi-{}_{}_{}'
        return k.format(o_d_id, o_w_id, o_c_id)
    def __ol_key(self, ol_number, ol_o_id, ol_d_id, ol_w_id, field):
        k = 'ol-{}_{}_{}_{}-{}'
        return k.format(ol_number, ol_o_id, ol_d_id, ol_w_id, field)
    def __i_key(self, i_id, field):
        k = 'i-{}-{}'
        return k.format(i_id, field)
    def __s_key(self, s_i_id, s_w_id, field):
        k = 's-{}_{}-{}'
        return k.format(s_i_id, s_w_id, field)
## CLASS
