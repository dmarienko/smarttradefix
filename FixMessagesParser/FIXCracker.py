import json
import logging
import os, sys
import time
import xml.etree.cElementTree as etree
from collections import OrderedDict, namedtuple
from operator import neg

from sortedcontainers import SortedDict

logger = logging.getLogger('FIX-PARSER')


class help_struct:
    def __init__(self, **kwargs):
        _odw = OrderedDict(**kwargs)
        self.__initialize(_odw.keys(), _odw.values())

    def __initialize(self, fields, values):
        self._fields = list(fields)
        self._meta = namedtuple('__help_mstruct', ' '.join(fields))
        self._inst = self._meta(*values)

    def __getattr__(self, k):
        return getattr(self._inst, k)

    def __dir__(self):
        return self._fields

    def __repr__(self):
        return self._inst.__repr__()

    def __setattr__(self, k, v):
        if k not in ['_inst', '_meta', '_fields']:
            new_vals = {**self._inst._asdict(), **{k: v}}
            self.__initialize(new_vals.keys(), new_vals.values())
        else:
            super().__setattr__(k, v)


class FixParser:

    def __init__(self, dictionary):
        self._dictionary = dictionary
        self._tags = self._parse_dictionary()

    @property
    def tags(self):
        return self._tags

    @property
    def dictionary(self):
        return self._dictionary

    def _parse_dictionary(self):
        doc = etree.parse(self.dictionary)
        root = doc.getroot()
        fi = root.find('fields')

        tags = OrderedDict()
        for node in fi.getchildren():
            r = node.get("number")
            tags[r] = {}
            tags[r]["name"] = node.get("name")
            for value in node:
                tags[r][value.get("enum")] = value.get("description")

        return tags

    def convertToDict(self, message):
        messageDict = OrderedDict()
        for k, v in message:
            # convert message-type
            name, value = k, v
            try:
                definition = self.tags[k]
                name = definition['name']
                value = definition.get(value, value)
            except KeyError:
                logger.warning("unknown-tag: {0}={1}".format(k, v))
            finally:
                messageDict[name] = value

        return messageDict

    def print_decoded(self, message):
        for k, v in message:
            name, value = k, v
            try:
                definition = self.tags[k]
                name = definition['name']
                value = definition.get(value, value)

                if name.startswith('MDEntry') or name.startswith('MDUpdate'):
                    print('\t', end='')

                print('%s : %s' % (('%s (%s)' % (name, k)).ljust(30), value))
            except KeyError:
                logger.warning("unknown-tag: {0}={1}".format(k, v))

    def html_decoded(self, message):
        r = '<table>'
        for k, v in message:
            # convert message-type
            name, value = k, v
            try:
                definition = self.tags[k]
                name = definition['name']
                is_marked = name.startswith('MDEntry') or name.startswith('MDUpdate')
                value = definition.get(value, value)
                c_name = '%s (%s)' % (name, k)
                c1 = ('<font color="red">%s</font>' % c_name) if is_marked else c_name
                s = '<tr><td>%s</td><td>%s</td></tr>' % (c1, value)
                r += s
            except KeyError:
                logger.warning("unknown-tag: {0}={1}".format(k, v))
        return r + '</table>'

    def convert(self, message):
        converted = self.convertToDict(message)
        encoded = json.dumps(converted, sort_keys=True, indent=4, separators=(',', ': '))
        return encoded + '\n'

    @staticmethod
    def parse_raw_message(rawMessage, delimiter):
        pairs = rawMessage.split(delimiter)

        # remove trailing pair
        if len(pairs[-1]) == 0:
            del pairs[-1]

        return tuple(map(lambda p: tuple(p.split('=')), pairs))

    @staticmethod
    def parse_message_log(logFile, delimiter):
        with open(logFile, 'rb') as logFileFd:
            messages = []
            for line in logFileFd:
                line = line.decode('utf8').rstrip('\n\r')

                pairs = FixParser.parse_raw_message(line, delimiter)
                messages.append(pairs)

            return messages

    def to_table(self, message):
        r = '<table>'
        is_snapshot = None
        columns = []

        _as_html_str = lambda clmns: '<tr>%s</tr>' % ''.join(['<td><font size="1">%s</font></td>' % c for c in clmns])

        r += '<tr>%s</tr>' % ''.join(
            ['<th>%s</th>' % s for s in ['EntryID', 'RefID', 'ENtryType', 'Action', 'Px', 'Size', 'EntryNo']])

        for k, v in message:
            name, value = k, v
            try:
                definition = self.tags[k]
                name = definition['name']

                if name == 'MsgType':
                    is_snapshot = value == 'W'

                value = definition.get(value, value)
                c_name = '%s (%s)' % (name, k)
                if name == 'MDEntryID':
                    columns[0] = value
                elif name == 'MDEntryRefID':
                    columns[1] = '<strong>' + value + '</strong>'
                elif name == 'MDEntryType':
                    if is_snapshot:
                        if len(columns) > 0:
                            r += _as_html_str(columns)
                        columns = ['-'] * 7

                    columns[2] = value
                elif name == 'MDUpdateAction':
                    if not is_snapshot:
                        if len(columns) > 0:
                            r += _as_html_str(columns)
                        columns = ['-'] * 7

                    columns[3] = value
                elif name == 'MDEntryPx':
                    columns[4] = '%.5f' % float(value)
                elif name == 'MDEntrySize':
                    columns[5] = value
                elif name == 'MDEntryPositionNo':
                    columns[6] = value
            except KeyError:
                logger.warning("unknown-tag: {0}={1}".format(k, v))
        if len(columns) > 0:
            r += _as_html_str(columns)
        return r + '</table>'


def cls():
    os.system('cls' if os.name == 'nt' else 'clear')


def print_book(asks, bids):
    cls()
    print(' ========= Aggregated OrderBook ========= \n')
    for k, v in asks.items():
        print('\t%.5f: %d\t\t' % (k, v.size), ', '.join(v.ids.keys()))
    print('\t--------------------')
    for k, v in bids.items():
        print('\t%.5f: %d\t\t' % (k, v.size), ', '.join(v.ids.keys()))


def find_key_by_refid(side, refid):
    return list(filter(lambda x: refid in side[x].ids, side))[0]


def new_entry(side, refid, px, sz):
    if side is not None:
        # key - price, values - struct where ids - dict of id:size, size = sum all sizes
        levels = side.get(px, help_struct(ids=dict(), size=0))
        levels.size += sz
        rec = levels.ids.get(refid, help_struct())
        rec.size = sz
        levels.ids.update({refid: rec})
        side.update({px: levels})
    return side


def del_entry(side, refid):
    if side is not None:
        key = find_key_by_refid(side, refid)
        rec = side[key]
        drop_size = rec.ids[refid].size
        del rec.ids[refid]
        rec.size -= drop_size
        if len(rec.ids) == 0:
            del side[key]


def change_entry(side, refid, px, sz):
    if side is not None:
        key = find_key_by_refid(side, refid)
        rec = side[key]
        if sz is not None:
            rec.ids[refid].size = sz
            rec.size = sum([r.size for r in rec.ids.values()])
        if px is not None and px != key:
            update_size = sz if sz is not None else rec.ids[refid].size
            del_entry(side, refid)
            new_entry(side, refid, px, update_size)


def snapshot_update(parser, message, asks, bids):
    if 'MARKET_DATA_SNAPSHOT_FULL_REFRESH' != parser.convertToDict(message)['MsgType']:
        raise ValueError("Wrong message type")

    ref_id, entry, px, sz = [None] * 4

    for k, v in message:
        definition = parser.tags[k]
        name, value = definition['name'], definition.get(v, v)

        if name == 'MDEntryType':
            # record new entry
            subj = bids if entry == 'BID' else (asks if entry == 'OFFER' else None)
            new_entry(subj, ref_id, px, sz)

            entry = value
            continue

        if name == 'MDEntryRefID': ref_id = value
        if name == 'MDEntryPx': px = float(value)
        if name == 'MDEntrySize': sz = int(value)

    # we need to add last entry
    subj = bids if entry == 'BID' else (asks if entry == 'OFFER' else None)
    new_entry(subj, ref_id, px, sz)


def do_action(side, action, refid, px, sz):
    if action == 'NEW':
        new_entry(side, refid, px, sz)
    elif action == 'DELETE':
        del_entry(side, refid)
    elif action == 'CHANGE':
        change_entry(side, refid, px, sz)
    elif action is not None:
        raise ValueError('Unknown action: %s' % action)


def incremental_update(parser, message, asks, bids):
    if 'MARKET_DATA_INCREMENTAL_REFRESH' != parser.convertToDict(message)['MsgType']:
        raise ValueError("Wrong message type")

    action, ref_id, entry, px, sz = [None] * 5

    for k, v in message:
        definition = parser.tags[k]
        name, value = definition['name'], definition.get(v, v)

        if name == 'MDUpdateAction':
            # record new entry
            subj = bids if entry == 'BID' else (asks if entry == 'OFFER' else None)
            do_action(subj, action, ref_id, px, sz)

            action = value
            px, sz = None, None
            continue

        if name == 'MDEntryType': entry = value
        if name == 'MDEntryRefID': ref_id = value
        if name == 'MDEntryPx': px = float(value)
        if name == 'MDEntrySize': sz = int(value)

    # we need to add last entry
    subj = bids if entry == 'BID' else (asks if entry == 'OFFER' else None)
    do_action(subj, action, ref_id, px, sz)


if __name__ == '__main__':
    parser = FixParser('./FIX44.smart.trade.xml')
    messages = parser.parse_message_log(sys.argv[1], '|')

    asks, bids = SortedDict(neg), SortedDict()

    # load snapshot
    snapshot_update(parser, messages[0], asks, bids)
    print_book(asks, bids)

    for i, m in enumerate(messages[1:]):
        incremental_update(parser, m, asks, bids)
        print_book(asks, bids)
        print('\n UPDATE: %d \n' % i)
        time.sleep(1)
