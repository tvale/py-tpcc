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
import api_pb2
import tpcc_pb2
from pymemcache.client.base import Client as memcached
import uuid
import constants
from abstractdriver import *

## ==============================================
## "Client library"
## ==============================================
class transaction_aborted(Exception):
    pass

class client:
    def __init__(self, server_host, server_port):
        self._id = uuid.uuid4()
        self._transaction_counter = 1
        self._database = memcached((server_host, server_port), no_delay=True)
        self._rwset_index = {}
        self._transaction = api_pb2.transaction()
        self._is_retrying = False
    
    def begin(self):
        self._rwset_index = {}
        # - as separator because it's ASCII value is < than alphanum
        self._transaction.id = '{}-{}'.format(self._transaction_counter, self._id)
        del self._transaction.wset[:]
        del self._transaction.fwset[:]
        del self._transaction.pset[:]
        if not self._is_retrying:
            result = self._database.incr(self._transaction.id, 1, noreply=False)
            assert result is not None

    def get(self, key):
        # simplify code by assuming:
        # a. no more than one get for a specific key per transaction
        # b. no get for a specific key if put before
        assert key not in self._rwset_index
        self._rwset_index[key] = None
        reply = self._database.gets_many([self._transaction.id, 'tx', key])
        exists = key in reply
        value, _ = reply[key] if exists else (None, None)
        return (exists, value)

    def get_notxn(self, key):
        reply = self._database.gets_many([self._transaction.id, 'notx', key])
        exists = key in reply
        assert exists
        value, _ = reply[key]
        return (exists, value)

    def multiget(self, klist):
        # simplify code by assuming:
        # a. no more than one get for a specific key per transaction
        # b. no get for a specific key if put before
        assert not filter(lambda k: k in self._rwset_index, klist)
        result = {}
        replies = self._database.gets_many([self._transaction.id, 'tx'] + klist)
        for key in klist:
            self._rwset_index[key] = None
            exists = key in replies
            value, _ = replies[key] if exists else (None, None)
            result[key] = (exists, value)
        return result

    def multiget_notxn(self, klist):
        replies = self._database.gets_many([self._transaction.id, 'notx'] + klist)
        result = {}
        for key in klist:
            assert key in replies
            value, _ = replies[key]
            result[key] = (True, value)
        return result

    def put(self, key, value):
        self._write(api_pb2.write.SET, key, value)

    def remove(self, key):
        self._write(api_pb2.write.DELETE, key, value='')

    def _write(self, write_type, key, value):
        # simplify code by assuming no more than one put for a specific key
        # per transaction
        if key in self._rwset_index:
            entry = self._rwset_index[key]
            if entry is None:
                entry = self._transaction.wset.add(key=key)
                self._rwset_index[key] = entry
        else:
            entry = self._transaction.wset.add(key=key)
            self._rwset_index[key] = entry
        entry.w.type = write_type
        entry.w.is_future = False
        entry.w.value = str(value)

    def commit(self):
        pb = self._transaction.SerializeToString()
        results = self._database.add(self._transaction.id, pb, expire=0, noreply=False)
        committed, _ = results['__lsd__committed']
        committed = committed == "1"
        if committed:
            self._done()
        else:
            self._retry('commit')

    def abort(self):
        result = self._database.decr(self._transaction.id, 1, noreply=False)
        assert result is not None
        self._done()

    def _done(self):
        self._is_retrying = False
        self._transaction_counter += 1

    def _retry(self, why):
        self._is_retrying = True
        raise transaction_aborted(why)

## ==============================================
## LSDDriver
## ==============================================
class LsdDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        'host': ('server host', 'localhost'),
        'port': ('server port', '11211'),
        'clients': ('client hosts:#processes', 'node1:1 node2:4'),
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
        self.client = client(config['host'], int(config['port']))

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
                w = w.SerializeToString()
                self.client._database.set(key, w, noreply=False)
                w_ytd = row[8]
                key = self.__w_key(w_id, 'ytd')
                self.client._database.set(key, str(w_ytd), noreply=False)
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
                d = d.SerializeToString()
                self.client._database.set(key, d, noreply=False)
                d_ytd = row[9]
                key = self.__d_key(d_id, d_w_id, 'ytd')
                self.client._database.set(key, str(d_ytd), noreply=False)
                d_next_o_id = int(row[10])
                key = self.__d_key(d_id, d_w_id, 'next_o_id')
                self.client._database.set(key, str(d_next_o_id), noreply=False)
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
                c = c.SerializeToString()
                self.client._database.set(key, c, noreply=False)
                c_balance = row[16]
                key = self.__c_key(c_id, c_d_id, c_w_id, 'balance')
                self.client._database.set(key, str(c_balance), noreply=False)
                c_ytd_payment = row[17]
                key = self.__c_key(c_id, c_d_id, c_w_id, 'ytd_payment')
                self.client._database.set(key, str(c_ytd_payment), noreply=False)
                c_payment_cnt = row[18]
                key = self.__c_key(c_id, c_d_id, c_w_id, 'payment_cnt')
                self.client._database.set(key, str(c_payment_cnt), noreply=False)
                c_delivery_cnt = row[19]
                key = self.__c_key(c_id, c_d_id, c_w_id, 'delivery_cnt')
                self.client._database.set(key, str(c_delivery_cnt), noreply=False)
                c_data = row[20]
                key = self.__c_key(c_id, c_d_id, c_w_id, 'data')
                self.client._database.set(key, str(c_data), noreply=False)
                # index on (c_d_id, c_w_id, c_last) for payment
                key = self.__c_index_key(c_d_id, c_w_id, c_last)
                exists = (key in index)
                if exists:
                    index[key].append(c_id)
                else:
                    index[key] = [c_id]
            # update index
            for k, v in index.iteritems():
                reply = self.client._database.gets_many(['unused', 'notx', k])
                exists = k in reply
                value , _ = reply[k] if exists else (None, None)
                l = tpcc_pb2.customer_index()
                if exists:
                    l.ParseFromString(value)
                l.c_id.extend(v)
                l = l.SerializeToString()
                self.client._database.set(k, l, noreply=False)
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
                h = h.SerializeToString()
                self.client._database.set(key, h, noreply=False)
        elif tableName == constants.TABLENAME_NEW_ORDER:
            index = {}
            for row in tuples:
                no_o_id = row[0]
                no_d_id = row[1]
                no_w_id = row[2]
                key = self.__no_key(no_o_id, no_d_id, no_w_id, '')
                self.client._database.set(key, str(no_o_id), noreply=False)
                # index on (no_d_id, no_w_id) for delivery
                key = self.__no_index_key(no_d_id, no_w_id)
                exists = key in index
                if (not exists) or (no_o_id < index[key]):
                    index[key] = no_o_id
            # update index
            for k, v in index.iteritems():
                reply = self.client._database.gets_many(['unused', 'notx', k])
                exists = k in reply
                value , _ = reply[k] if exists else (None, None)
                if (not exists) or (v < int(value)):
                    self.client._database.set(k, str(v), noreply=False)
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
                o = o.SerializeToString()
                self.client._database.set(key, o, noreply=False)
                o_carrier_id = row[5]
                key = self.__o_key(o_id, o_d_id, o_w_id, 'carrier_id')
                self.client._database.set(key, str(o_carrier_id), noreply=False)
                # index on (o_d_id, o_w_id, o_c_id) for order-status
                key = self.__o_index_key(o_d_id, o_w_id, o_c_id)
                index[key] = o_id
            # update index
            for k, v in index.iteritems():
                self.client._database.set(k, str(v), noreply=False)
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
                ol = ol.SerializeToString()
                self.client._database.set(key, ol, noreply=False)
                ol_delivery_d = row[6]
                key = self.__ol_key(ol_number, ol_o_id, ol_d_id, ol_w_id, 'delivery_d')
                self.client._database.set(key, str(ol_delivery_d), noreply=False)
        elif tableName == constants.TABLENAME_ITEM:
            for row in tuples:
                i_id = row[0]
                i = tpcc_pb2.item()
                i.im_id = int(row[1])
                i.name = str(row[2])
                i.price = float(row[3])
                i.data = str(row[4])
                key = self.__i_key(i_id, '')
                i = i.SerializeToString()
                self.client._database.set(key, i, noreply=False)
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
                s = s.SerializeToString()
                self.client._database.set(key, s, noreply=False)
                s_quantity = row[2]
                key = self.__s_key(s_i_id, s_w_id, 'quantity')
                self.client._database.set(key, str(s_quantity), noreply=False)
                s_ytd = row[13]
                key = self.__s_key(s_i_id, s_w_id, 'ytd')
                self.client._database.set(key, str(s_ytd), noreply=False)
                s_order_cnt = row[14]
                key = self.__s_key(s_i_id, s_w_id, 'order_cnt')
                self.client._database.set(key, str(s_order_cnt), noreply=False)
                s_remote_cnt = row[15]
                key = self.__s_key(s_i_id, s_w_id, 'remote_cnt')
                self.client._database.set(key, str(s_remote_cnt), noreply=False)
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
            d_ids
            o_carrier_id
            ol_delivery_d
        """
        w_id = params['w_id']
        d_ids = params['d_ids']
        o_carrier_id = params['o_carrier_id']
        ol_delivery_d = params['ol_delivery_d']
        d_id = params['tid']
        try:
            self.client.begin()
            '''
            retrieve oldest new-order (no_o_id) for the given warehouse (no_w_id) and district (no_d_id)

            DECLARE c_no CURSOR FOR
            SELECT no_o_id
            FROM new_order
            WHERE no_d_id = :d_id AND no_w_id = :w_id
            ORDER BY no_o_id ASC;

            OPEN c_no;

            FETCH c_no INTO :no_o_id;
            '''
            no_index_key = self.__no_index_key(d_id, w_id)
            _, value = self.client.get(no_index_key)
            o_id = int(value)
            # check if new-order exists
            no_key = self.__no_key(o_id, d_id, w_id, '')
            exists, _ = self.client.get(no_key)
            if not exists:
                self.client.abort()
                return 1
            '''
            delete oldest new-order

            DELETE FROM new_order WHERE CURRENT OF c_no;
            '''
            self.client.remove(no_key)
            # update oldest new-order index
            self.client.put(no_index_key, str(o_id + 1))
            '''
            retrieve order client id (o_c_id)

            SELECT o_c_id
            INTO    :c_id
            FROM orders
            WHERE o_id = :no_o_id AND o_d_id = :d_id AND o_w_id = :w_id;
            '''
            o_key = self.__o_key(o_id, d_id, w_id, '')
            exists, value = self.client.get(o_key)
            assert exists
            o = tpcc_pb2.order()
            o.ParseFromString(value)
            c_id = o.c_id
            o_ol_cnt = o.ol_cnt
            '''
            update order carrier (o_carrier_id)

            UPDATE orders
            SET o_carrier_id = :o_carrier_id
            WHERE o_id = :no_o_id AND o_d_id = :d_id AND o_w_id = :w_id;
            '''
            o_carrier_id_key = self.__o_key(o_id, d_id, w_id, 'carrier_id')
            self.client.put(o_carrier_id_key, str(o_carrier_id))
            '''
            retrieve sum all order-line amount (ol_amount) and
            update delivery dates (ol_delivery_d)

            SELECT SUM(ol_amount)
            INTO    :ol_total
            FROM order_line
            WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND ol_w_id = :w_id;

            UPDATE order_line
            SET ol_delivery_d = :datetime
            WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND ol_w_id = :w_id;
            '''
            keys = []
            for ol_number in range(o_ol_cnt):
                ol_key = self.__ol_key(ol_number, o_id, d_id, w_id, '')
                keys.append(ol_key)
            mg_res = self.client.multiget(keys)
            ol_total = 0.0
            for ol_number in range(o_ol_cnt):
                ol_key = self.__ol_key(ol_number, o_id, d_id, w_id, '')
                ol_delivery_d_key = self.__ol_key(ol_number, o_id, d_id, w_id, 'delivery_d')
                exists, value = mg_res[ol_key]
                assert exists
                ol = tpcc_pb2.order_line()
                ol.ParseFromString(value)
                ol_total += ol.amount
                self.client.put(ol_delivery_d_key, str(ol_delivery_d))
            '''
            increase customer balance (c_balance) by the sum of all order-line amounts (ol_amount), and increment delivery count (c_delivery_cnt)

            UPDATE customer
            SET c_balance = c_balance + :ol_total
            WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;

            UPDATE customer
            SET c_delivery_cnt = c_delivery_cnt + 1
            WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;
            '''
            c_balance_key = self.__c_key(c_id, d_id, w_id, 'balance')
            c_delivery_cnt_key = self.__c_key(c_id, d_id, w_id, 'delivery_cnt')
            mg_res = self.client.multiget([c_balance_key, c_delivery_cnt_key])
            exists, value = mg_res[c_balance_key]
            assert exists
            c_balance = float(value)
            c_balance += ol_total
            self.client.put(c_balance_key, str(c_balance))
            exists, value = mg_res[c_delivery_cnt_key]
            assert exists
            c_delivery_cnt = int(value)
            c_delivery_cnt += 1
            self.client.put(c_delivery_cnt_key, str(c_delivery_cnt))
            self.client.commit()
        except transaction_aborted as ex:
            info = 'txn: {} | tpcc: w_id={} d_id={}'
            info = info.format(str(ex), w_id, d_id)
            raise Exception(info)
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
        try:
            self.client.begin()
            '''
            retrieve customer discout rate (c_discount), last name (c_last), credit (c_credit), and warehouse tax rate (w_tax)

            SELECT c_discount,  c_last,  c_credit
            INTO  :c_discount, :c_last, :c_credit
            FROM customer
            WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_id = :c_id;

            SELECT w_tax
            INTO  :w_tax
            FROM warehouse
            WHERE w_id = :w_id;
            '''
            c_key = self.__c_key(c_id, d_id, w_id, '')
            w_key = self.__w_key(w_id, '')
            exists, value = self.client.get_notxn(c_key)
            assert exists
            c = tpcc_pb2.customer()
            c.ParseFromString(value)
            c_discount = c.discount
            c_last = c.last
            c_credit = c.credit
            exists, value = self.client.get_notxn(w_key)
            assert exists
            w = tpcc_pb2.warehouse()
            w.ParseFromString(value)
            w_tax = w.tax
            '''
            retrieve district tax rate (d_tax), and next available order number (d_next_o_id)

            SELECT d_next_o_id,  d_tax
            INTO  :d_next_o_id, :d_tax
            FROM district
            WHERE d_id = :d_id AND d_w_id = :w_id;
            '''
            d_key = self.__d_key(d_id, w_id, '')
            d_next_o_id_key = self.__d_key(d_id, w_id, 'next_o_id')
            exists, value = self.client.get(d_next_o_id_key)
            d_next_o_id = int(value)
            exists, value = self.client.get_notxn(d_key)
            assert exists
            d = tpcc_pb2.district()
            d.ParseFromString(value)
            d_tax = d.tax
            '''
            increment district next available order number (d_next_o_id)

            UPDATE district
            SET d_next_o_id = :d_next_o_id + 1
            WHERE d_id = :d_id AND d_w_id = :w_id;
            '''
            self.client.put(d_next_o_id_key, str(d_next_o_id + 1))
            '''
            insert order

            INSERT INTO ORDERS ( o_id,  o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt,  o_all_local)
            VALUES             (:o_id, :d_id,  :w_id,  :c_id,  :datetime, :o_ol_cnt, :o_all_local);
            '''
            o_ol_cnt = len(i_ids)
            o_all_local = len(set(i_w_ids)) == 1 and i_w_ids[0] == w_id
            o_key = self.__o_key(d_next_o_id, d_id, w_id, '')
            o_carrier_id_key = self.__o_key(d_next_o_id, d_id, w_id, 'carrier_id')
            o = tpcc_pb2.order()
            o.d_id = int(d_id)
            o.w_id = int(w_id)
            o.c_id = int(c_id)
            o.entry_d = str(o_entry_d)
            o.ol_cnt = int(o_ol_cnt)
            o.all_local = bool(o_all_local)
            o = o.SerializeToString()
            self.client.put(o_key, o)
            self.client.put(o_carrier_id_key, str(constants.NULL_CARRIER_ID))
            '''
            update customer last order index

            '''
            o_index_key = self.__o_index_key(d_id, w_id, c_id)
            self.client.put(o_index_key, str(d_next_o_id))
            '''
            insert new-order

            INSERT INTO NEW_ORDER ( no_o_id, no_d_id, no_w_id)
            VALUES                (:o_id,   :d_id,   :w_id);
            '''
            no_key = self.__no_key(d_next_o_id, d_id, w_id, '')
            self.client.put(no_key, str(d_next_o_id))
            '''
            insert order-lines

            '''
            sum_ol_amount = 0
            for j in range(o_ol_cnt):
                ol_i_id = i_ids[j]
                ol_supply_w_id = i_w_ids[j]
                ol_quantity = i_qtys[j]
                ol_is_remote = (w_id == ol_supply_w_id)
                '''
                retrieve item price (i_price), name (i_name), and data (i_data)

                SELECT i_price, i_name , i_data
                INTO :i_price, :i_name, :i_data
                FROM item
                WHERE i_id = :ol_i_id;
                if item does not exist
                    ROLLBACK WORK;
                '''
                i_key = self.__i_key(ol_i_id, '')
                exists, value = self.client.get_notxn(i_key)
                # TODO needs EXISTS unaryop
                if not exists:
                    assert False
                    self.client.abort()
                    return 1
                assert exists
                i = tpcc_pb2.item()
                i.ParseFromString(value)
                # TODO i_price and i_data need their own key
                i_price = i.price
                i_name = i.name
                i_data = i.data
                '''
                retrieve stock quantity (s_quantity), year-to-date balance (s_ytd), number of local and remote orders (s_order_cnt, s_remote_cnt), district info (s_dist_xx), and data (s_data)

                SELECT s_quantity,  s_data,     s_dist_01,  s_dist_02,
                       s_dist_03,   s_dist_04,  s_dist_05,  s_dist_06,
                       s_dist_07,   s_dist_08,  s_dist_09,  s_dist_10
                INTO  :s_quantity, :s_data,    :s_dist_01, :s_dist_02,
                      :s_dist_03,  :s_dist_04, :s_dist_05, :s_dist_06,
                      :s_dist_07,  :s_dist_08, :s_dist_09, :s_dist_10
                FROM stock
                WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
                '''
                s_key = self.__s_key(ol_i_id, ol_supply_w_id, '')
                s_quantity_key = self.__s_key(ol_i_id, ol_supply_w_id, 'quantity')
                s_ytd_key = self.__s_key(ol_i_id, ol_supply_w_id, 'ytd')
                s_order_cnt_key = self.__s_key(ol_i_id, ol_supply_w_id, 'order_cnt')
                if ol_is_remote:
                    s_remote_cnt_key = self.__s_key(ol_i_id, ol_supply_w_id, 'remote_cnt')
                if len(str(d_id)) == 1:
                    dist_xx = 'dist_0{}'.format(str(d_id))
                else:
                    dist_xx = 'dist_{}'.format(str(d_id))
                keys = []
                keys.append(s_quantity_key)
                keys.append(s_ytd_key)
                keys.append(s_order_cnt_key)
                if ol_is_remote:
                    keys.append(s_remote_cnt_key)
                mg_res = self.client.multiget(keys)
                exists, value = mg_res[s_quantity_key]
                s_quantity = int(value)
                exists, value = mg_res[s_ytd_key]
                s_ytd = int(value)
                exists, value = mg_res[s_order_cnt_key]
                s_order_cnt = int(value)
                if ol_is_remote:
                    exists, value = mg_res[s_remote_cnt_key]
                    s_remote_cnt = int(value)
                exists, value = self.client.get_notxn(s_key)
                assert exists
                s = tpcc_pb2.stock()
                s.ParseFromString(value)
                s_dist_xx = getattr(s, dist_xx)
                s_data = s.data
                '''
                update stock quantity (s_quantity)

                if (s_quantity >= ol_quantity + 10)
                    s_quantity = s_quantity - ol_quantity;
                else
                    s_quantity = s_quantity - ol_quantity + 91;
                UPDATE stock
                SET s_quantity = :s_quantity
                WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
                '''
                restock_qty = constants.MAX_QUANTITY - 9
                restock_threshold = 10
                if s_quantity >= ol_quantity + restock_threshold:
                    s_quantity -= ol_quantity
                else:
                    s_quantity = (s_quantity - ol_quantity) + restock_qty
                self.client.put(s_quantity_key, str(s_quantity))
                '''
                update stock year-to-date balance (s_ytd)

                s_ytd += ol_quantity;
                UPDATE stock
                SET s_ytd = :s_ytd
                WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
                '''
                s_ytd += ol_quantity
                self.client.put(s_ytd_key, str(s_ytd))
                '''
                update stock number of local (s_order_cnt) and remote orders (s_remote_cnt)

                UPDATE stock
                SET s_order_cnt = :s_order_cnt + 1
                WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
                if order is remote
                    UPDATE stock
                    SET s_remote_cnt = :s_remote_cnt + 1
                    WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
                '''
                s_order_cnt += 1
                self.client.put(s_order_cnt_key, str(s_order_cnt))
                if ol_is_remote:
                    s_remote_cnt += 1
                    self.client.put(s_remote_cnt_key, str(s_remote_cnt))
                '''
                compute amount for the item in the order (ol_amount)

                '''
                # TODO needs MUL binaryop
                ol_amount = ol_quantity * i_price
                '''
                compute brand-generic

                if ((strstr(i_data,"original") != NULL) && (strstr(s_data,"original") != NULL))
                    bg = 'B';
                else
                    bg = 'G';
                '''
                # TODO needs SUBSTR binaryop
                if ((constants.ORIGINAL_STRING in i_data) and (constants.ORIGINAL_STRING in s_data)):
                    brand_generic = 'B'
                else:
                    brand_generic = 'G'
                '''
                insert order-line

                INSERT INTO order_line ( ol_o_id, ol_d_id, ol_w_id, ol_number,  ol_i_id,  ol_supply_w_id,  ol_quantity,  ol_amount,  ol_dist_info)
                VALUES                 (:o_id,   :d_id,   :w_id,   :ol_number, :ol_i_id, :ol_supply_w_id, :ol_quantity, :ol_amount, :ol_dist_info);
                '''
                ol_number = j
                ol_key = self.__ol_key(ol_number, d_next_o_id, d_id, w_id, '')
                ol_delivery_d_key = self.__ol_key(ol_number, d_next_o_id, d_id, w_id, 'delivery_d')
                ol = tpcc_pb2.order_line()
                ol.i_id = int(ol_i_id)
                ol.supply_w_id = int(ol_supply_w_id)
                ol.quantity = int(ol_quantity)
                ol.amount = float(ol_amount)
                ol.dist_info = str(s_dist_xx)
                ol = ol.SerializeToString()
                self.client.put(ol_key, ol)
                self.client.put(ol_delivery_d_key, str(o_entry_d))
                sum_ol_amount += ol_amount
            # for
            self.client.commit()
        except transaction_aborted as ex:
            info = 'txn: {} | tpcc: c_id={} w_id={} d_id={} i_ids={}'
            info = info.format(str(ex), c_id, w_id, d_id, str(sorted(i_ids)))
            raise Exception(info)
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
        try:
            self.client.begin()
            # retrieve customer id (c_id) if only given last name (c_last)
            if c_last != None:
                # get all customer ids (c_id) from index on last name (c_last)
                key = self.__c_index_key(d_id, w_id, c_last)
                exists, value = self.client.get_notxn(key)
                assert exists
                ci = tpcc_pb2.customer_index()
                ci.ParseFromString(value)
                c_ids = ci.c_id
                # get all customer names (c_first)
                keys = []
                for id in c_ids:
                    key = self.__c_key(id, d_id, w_id, '')
                    keys.append(key)
                mg_res = self.client.multiget_notxn(keys)
                c_ids_firsts = []
                for id in c_ids:
                    key = self.__c_key(id, d_id, w_id, '')
                    exists, value = mg_res[key]
                    assert exists
                    c = tpcc_pb2.customer()
                    c.ParseFromString(value)
                    c_ids_firsts.append((id, c.first))
                # sort by c_first
                c_ids_firsts.sort(key = lambda t: t[1])
                j = (len(c_ids_firsts) - 1) / 2
                c_id = c_ids_firsts[j][0]
                c_first = c_ids_firsts[j][1]
            '''
            retrieve customer name (c_first/middle/last), and balance (c_balance)

            SELECT c_balance,  c_first,  c_middle,  c_last
            INTO  :c_balance, :c_first, :c_middle, :c_last
            FROM customer
            WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;
            '''
            c_balance_key = self.__c_key(c_id, d_id, w_id, 'balance')
            exists, value = self.client.get(c_balance_key)
            assert exists
            c_balance = float(value)
            '''
            retrieve customer's last order id (o_id), entry date (o_entry_d), carrier (o_carrier_id), and item count (o_ol_cnt)

            SELECT o_id,  o_carrier_id,  o_entry_d
            INTO  :o_id, :o_carrier_id, :entdate
            FROM orders
            ORDER BY o_id DESC;
            '''
            key = self.__o_index_key(d_id, w_id, c_id)
            exists, value = self.client.get(key)
            assert exists
            o_id = int(value)
            o_key = self.__o_key(o_id, d_id, w_id, '')
            o_carrier_id_key = self.__o_key(o_id, d_id, w_id, 'carrier_id')
            mg_res = self.client.multiget([o_key, o_carrier_id_key])
            exists, value = mg_res[o_carrier_id_key]
            assert exists
            o_carrier_id = value
            exists, value = mg_res[o_key]
            assert exists
            o = tpcc_pb2.order()
            o.ParseFromString(value)
            o_ol_cnt = o.ol_cnt
            '''
            retrieve order-line item (ol_i_id), supply warehouse (ol_supply_w_id), amount (ol_amount), and delivery date (ol_delivery_d)

            SELECT ol_i_id,  ol_supply_w_id,  ol_quantity,  ol_amount,  ol_delivery_d
            INTO  :ol_i_id, :ol_supply_w_id, :ol_quantity, :ol_amount, :ol_delivery_d
            FROM order_line
            WHERE ol_o_id = :o_id AND ol_d_id = :d_id AND ol_w_id = :w_id;
            '''
            keys = []
            for ol_number in range(o_ol_cnt):
                ol_key = self.__ol_key(ol_number, o_id, d_id, w_id, '')
                ol_delivery_d_key = self.__ol_key(ol_number, o_id, d_id, w_id, 'delivery_d')
                keys.append(ol_key)
                keys.append(ol_delivery_d_key)
            mg_res = self.client.multiget(keys)
            self.client.commit()
        except transaction_aborted as ex:
            info = 'txn: {} | tpcc: w_id={} d_id={} c_id={} c_last={}'
            info = info.format(str(ex), w_id, d_id, c_id, c_last)
            raise Exception(info)
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
        try:
            self.client.begin()
            '''
            retrieve warehouse name (w_name), street (w_street_1/2), city (w_city), state (w_state), zip code (w_zip), and year-to-date balance (w_ytd)

            SELECT w_street_1,  w_street_2,  w_city,  w_state,  w_zip,  w_name
            INTO  :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
            FROM warehouse
            WHERE w_id = :w_id;
            '''
            w_key = self.__w_key(w_id, '')
            w_ytd_key = self.__w_key(w_id, 'ytd')
            exists, value = self.client.get_notxn(w_key)
            assert exists
            w = tpcc_pb2.warehouse()
            w.ParseFromString(value)
            w_name = w.name
            exists, value = self.client.get(w_ytd_key)
            assert exists
            w_ytd = float(value)
            '''
            increase warehouse year-to-date balance (w_ytd)

            UPDATE warehouse
            SET w_ytd = w_ytd + :h_amount
            WHERE w_id = :w_id;
            '''
            w_ytd += h_amount
            self.client.put(w_ytd_key, str(w_ytd))
            '''
            retrieve district name (d_name), street (d_street_1/2), city (d_city), state (d_state), zip code (d_zip), and year-to-date balance (d_ytd)

            SELECT d_street_1,  d_street_2,  d_city,  d_state,  d_zip,  d_name
            INTO  :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
            FROM district
            WHERE d_w_id = :w_id AND d_id = :d_id;
            '''
            d_key = self.__d_key(d_id, w_id, '')
            d_ytd_key = self.__d_key(d_id, w_id, 'ytd')
            exists, value = self.client.get_notxn(d_key)
            assert exists
            d = tpcc_pb2.district()
            d.ParseFromString(value)
            d_name = d.name
            exists, value = self.client.get(d_ytd_key)
            assert exists
            d_ytd = float(value)
            '''
            increase district year-to-date balance (d_ytd)

            UPDATE district
            SET d_ytd = d_ytd + :h_amount
            WHERE d_w_id = :w_id AND d_id = :d_id;
            '''
            d_ytd += h_amount
            self.client.put(d_ytd_key, str(d_ytd))
            '''
            retrieve customer id (c_id) if only given last name (c_last)

            '''
            if c_last != None:
                c_credits = {}
                # get all customer ids (c_id) from index on last name (c_last)
                key = self.__c_index_key(c_d_id, c_w_id, c_last)
                exists, value = self.client.get_notxn(key)
                assert exists
                ci = tpcc_pb2.customer_index()
                ci.ParseFromString(value)
                c_ids = ci.c_id
                # get all customer names (c_first)
                keys = []
                for id in c_ids:
                    key = self.__c_key(id, c_d_id, c_w_id, '')
                    keys.append(key)
                mg_res = self.client.multiget_notxn(keys)
                c_ids_firsts = []
                for id in c_ids:
                    key = self.__c_key(id, c_d_id, c_w_id, '')
                    exists, value = mg_res[key]
                    assert exists
                    c = tpcc_pb2.customer()
                    c.ParseFromString(value)
                    c_ids_firsts.append((id, c.first))
                    c_credits[id] = c.credit
                # sort by c_first
                c_ids_firsts.sort(key = lambda t: t[1])
                j = (len(c_ids_firsts) - 1) / 2
                c_id = c_ids_firsts[j][0]
                c_first = c_ids_firsts[j][1]
                c_credit = c_credits[c_id]
            else:
                c_key = self.__c_key(c_id, c_d_id, c_w_id, '')
                exists, value = self.client.get_notxn(c_key)
                assert exists
                c = tpcc_pb2.customer()
                c.ParseFromString(value)
                c_credit = c.credit
            '''
            retrieve customer name (c_first/middle/last), street (c_street1/2), city (c_city), state (c_state), zip code (c_zip), phone number (c_phone), creation date (c_since), credit (c_credit), credit limit (c_credit_lim), discount (c_discount), balance (c_balance), year-to-date payment (c_ytd_payment), and number of payments (c_payment_cnt)

            SELECT c_first,  c_middle,  c_last,  c_street_1,  c_street_2,  c_city,  c_state,  c_zip,  c_phone,  c_credit,  c_credit_lim,  c_discount,  c_balance,  c_since
            INTO  :c_first, :c_middle, :c_last, :c_street_1, :c_street_2, :c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since
            FROM customer
            WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
            '''
            c_balance_key = self.__c_key(c_id, c_d_id, c_w_id, 'balance')
            c_ytd_payment_key = self.__c_key(c_id, c_d_id, c_w_id, 'ytd_payment')
            c_payment_cnt_key = self.__c_key(c_id, c_d_id, c_w_id, 'payment_cnt')
            mg_res = self.client.multiget([c_balance_key, c_ytd_payment_key, c_payment_cnt_key])
            exists, value = mg_res[c_balance_key]
            c_balance = float(value)
            exists, value = mg_res[c_ytd_payment_key]
            c_ytd_payment = float(value)
            exists, value = mg_res[c_payment_cnt_key]
            c_payment_cnt = int(value)
            '''
            decrease customer balance (c_balance)

            UPDATE customer
            SET c_balance = :c_balance - :h_amount
            WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
            '''
            c_balance -= h_amount
            self.client.put(c_balance_key, str(c_balance))
            '''
            increase customer year-to-date payment (c_ytd_payment)

            '''
            c_ytd_payment += h_amount
            self.client.put(c_ytd_payment_key, str(c_ytd_payment))
            '''
            increment customer payment number (c_payment_cnt)

            '''
            c_payment_cnt += 1
            self.client.put(c_payment_cnt_key, str(c_payment_cnt))
            '''
            update customer data (c_data) if she has bad credit (c_credit = BC)

            '''
            # TODO needs CONCAT binaryop and TRUNC unaryop
            if c_credit == constants.BAD_CREDIT:
                key = self.__c_key(c_id, c_d_id, c_w_id, 'data')
                exists, value = self.client.get(key)
                data = ' '.join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
                c_data = (data + '|' + value)
                if len(c_data) > constants.MAX_C_DATA:
                    c_data = c_data[:constants.MAX_C_DATA]
                self.client.put(key, c_data)
            '''
            insert history

            INSERT INTO history ( h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date,    h_amount,  h_data)
            VALUES              (:c_d_id,  :c_w_id,  :c_id,  :d_id,  :w_id,  :datetime, :h_amount, :h_data);
            '''
            h_uuid = uuid.uuid1()
            # create history data (h_data) by concatenating warehouse and
            # district name (w_name, d_name) separated by 4 spaces
            # TODO needs CONCAT binaryop
            h_data = '{}    {}'.format(w_name, d_name)
            h = tpcc_pb2.history()
            h.c_id = int(c_id)
            h.c_d_id = int(c_d_id)
            h.c_w_id = int(c_w_id)
            h.d_id = int(d_id)
            h.w_id = int(w_id)
            h.date = str(h_date)
            h.amount = float(h_amount)
            h.data = str(h_data)
            h = h.SerializeToString()
            h_key = self.__h_key(h_uuid, c_id, c_w_id, w_id, '')
            self.client.put(h_key, h)
            self.client.commit()
        except transaction_aborted as ex:
            info = 'txn: {} | tpcc: c_id={} c_last={} w_id={} c_w_id={} d_id={} c_d_id={}'
            info = info.format(str(ex), c_id, c_last, w_id, c_w_id, d_id, c_d_id)
            raise Exception(info)
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
        tid = params['tid']
        if tid == 1:
            try:
                self.client.begin()
                '''
                retrieve district next order identifier (d_next_o_id)

                SELECT d_next_o_id
                INTO  :o_id
                FROM district
                WHERE d_w_id = :w_id AND d_id = :d_id;
                '''
                d_next_o_id_key = self.__d_key(d_id, w_id, 'next_o_id')
                exists, value = self.client.get(d_next_o_id_key)
                self.client.commit()
            except transaction_aborted as ex:
                info = 'txn: {} | tpcc: w_id={} d_id={} threshold={} (getting d_next_o_id)'
                info = info.format(str(ex), w_id, d_id, threshold)
                raise Exception(info)
            d_next_o_id = int(value)
            params['d_next_o_id'] = d_next_o_id
        elif tid == 2:
            d_next_o_id = params['d_next_o_id']
            try:
                self.client.begin()
                '''

                SELECT COUNT(DISTINCT(s_i_id))
                INTO  :stock_count
                FROM order_line, stock
                WHERE ol_w_id = :w_id AND ol_d_id = :d_id AND ol_o_id < :o_id AND ol_o_id >= :o_id - 20 AND s_w_id = :w_id AND s_i_id = ol_i_id AND s_quantity < :threshold;
                '''
                # retrieve number of order-lines (o_ol_cnt) for last 20 orders
                keys = []
                for j in range(1, 21):
                    o_id = d_next_o_id - j
                    o_key = self.__o_key(o_id, d_id, w_id, '')
                    keys.append(o_key)
                mg_res = self.client.multiget(keys)
                ol_numbers = {}
                for j in range(1, 21):
                    o_id = d_next_o_id - j
                    o_key = self.__o_key(o_id, d_id, w_id, '')
                    exists, value = mg_res[o_key]
                    if exists:
                        o = tpcc_pb2.order()
                        o.ParseFromString(value)
                        o_ol_cnt = o.ol_cnt
                        ol_numbers[o_id] = o_ol_cnt
                    else:
                        break
                # retrieve all order-line items (ol_i_id)
                keys = []
                for o_id, o_ol_cnt in ol_numbers.iteritems():
                    for ol_number in range(o_ol_cnt):
                        ol_key = self.__ol_key(ol_number, o_id, d_id, w_id, '')
                        keys.append(ol_key)
                mg_res = self.client.multiget(keys)
                self.client.commit()
            except transaction_aborted as ex:
                info = 'txn: {} | tpcc: w_id={} d_id={} threshold={} (getting s_i_id)'
                info = info.format(str(ex), w_id, d_id, threshold)
                raise Exception(info)
            i_ids = []
            for o_id, o_ol_cnt in ol_numbers.iteritems():
                for ol_number in range(o_ol_cnt):
                    ol_key = self.__ol_key(ol_number, o_id, d_id, w_id, '')
                    exists, value = mg_res[ol_key]
                    assert exists
                    ol = tpcc_pb2.order_line()
                    ol.ParseFromString(value)
                    i_id = ol.i_id
                    i_ids.append(i_id)
            i_ids = set(i_ids)
            params['i_ids'] = i_ids
        elif tid == 3:
            i_ids = params['i_ids']
            try:
                # retrieve the stock quantity (s_quantity) of each item
                keys = []
                for i_id in i_ids:
                    s_quantity_key = self.__s_key(i_id, w_id, 'quantity')
                    keys.append(s_quantity_key)
                mg_res = self.client.multiget_notxn(keys)
            except transaction_aborted as ex:
                info = 'txn: {} | tpcc: w_id={} d_id={} threshold={} (getting s_quantity)'
                info = info.format(str(ex), w_id, d_id, threshold)
                raise Exception(info)
            # count the number of items with stock below threshold
            low_stock = 0
            for i_id in i_ids:
                s_quantity_key = self.__s_key(i_id, w_id, 'quantity')
                exists, value = mg_res[s_quantity_key]
                s_quantity = int(value)
                if s_quantity < threshold:
                    low_stock += 1
        return 1

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
