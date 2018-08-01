#!/bin/sh
#coding=utf8
import sys
import random

def write_skuids(word, skuid_set, multiple):
  skuids = list(skuid_set)
  skuids = random.sample(skuids, min(len(skuids), multiple))
  for skuid in skuids:
    print '%s\t%s' % (skuid, word)

def reducer(multiple):
  word, skuid_set = '', set()
  for line in sys.stdin:
    try:
      items = line.strip().split('\t')
      w = items[0]
      skuids = items[1].split()
      if word == '':
        word = w
      if word != w:
        write_skuids(word, skuid_set, multiple)
        word = w
        skuid_set = set()
      skuid_set |= set(skuids)
    except Exception, e:
      pass
  write_skuids(word, skuid_set, multiple)

if __name__ == '__main__':
  multiple = int(sys.argv[1])
  reducer(multiple)
