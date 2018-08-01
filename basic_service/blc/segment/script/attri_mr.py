#coding=utf8
#author=zhangyunfei
import sys
import os
import json

def Map():
  item_sep = '&'
  key_val_sep = ':'
  for line in sys.stdin:
    try:
      items = line.strip().split('\t')
      spec_attr = items[19].split(item_sep)
      for key_val in spec_attr:
        key, val = key_val.split(key_val_sep)
        print '%s\t%s' % (key, val)
      ext_attr = items[20].split(item_sep)
      for key_val in ext_attr:
        key, val = key_val.split(key_val_sep)
        print '%s\t%s' % (key, val)
    except Exception, e:
      pass

def handle_batch(key, value_dict):
  for value in value_dict:
    print '%s\t%s\t%s' % (key, value, value_dict[value])

def Reduce():
  cur_key = None
  value_dict = dict()
  for line in sys.stdin:
    try:
      key, value = line.strip().split('\t')
      if cur_key is None:
        cur_key = key
      if cur_key != key:
        handle_batch(cur_key, value_dict)
        cur_key = key
        value_dict = dict()
      value_dict[value] = value_dict.get(value, 0) + 1
    except Exception, e:
      pass
  handle_batch(cur_key, value_dict)

if __name__ == '__main__':
  cmd = sys.argv[1]
  globals()[cmd]()
