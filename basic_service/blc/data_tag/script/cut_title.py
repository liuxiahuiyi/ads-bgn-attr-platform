#!/bin/sh
#coding=utf8

import sys
import jieba

def mapper():
  for line in sys.stdin:
    try:
      items = line.strip().split('\t')
      skuid = items[0]
      seg = ''
      for s in items[1:]:
        if seg == '':
          seg = ' '.join(jieba.cut(s))
        else:
          seg += ' ' + ' '.join(jieba.cut(s))
      if seg == '':
        continue
      seg = seg.encode('utf8')
      print '%s\t%s' % (skuid, seg)
    except Exception, e:
      pass

if __name__ == '__main__':
  mapper()
