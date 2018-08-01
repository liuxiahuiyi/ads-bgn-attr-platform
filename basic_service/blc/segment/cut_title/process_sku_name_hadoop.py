# coding: utf-8
import codecs
import sys
import json
#import os
sys.path.append("./")

from bgnseg import BgnSeg

def map(user_dict_file, product_words_file):

#  s = []
#  for root, dirs, files in os.walk('./'):
#    s.append(root+': ' + ', '.join(dirs) + ' + ' + ', '.join(files))
#  result = ' <> '.join(s)
#  print result

  bgnseg = BgnSeg(user_dict_file, product_words_file, 'word_prob/word_prob.pkl')

  for line in sys.stdin:
    ss = line.decode('utf-8').strip().split('\t')
    if len(ss) >= 2:
      tmp = list()
      s_trans = ss[1].strip()
      s_trans_seg = bgnseg.cut(s_trans)
      for s in s_trans_seg:
        if len(s.strip()) > 0:
          tmp.append(s.strip())
      sku_segment = ' '.join(tmp)
      sku_segment_json = json.dumps(s_trans_seg)
      sku_id = ss[0].strip()
      sku_name = s_trans
      print ('\t'.join([sku_id, sku_name, sku_segment_json, sku_segment])).encode('utf-8')

def reduce():
  for line in sys.stdin:
    print line.strip()

if __name__ == '__main__':
#  if len(sys.argv) != 2:
#    print 'usage: python process_sku_name_hadoop cmd'
#    sys.exit(1)
  cmd = sys.argv[1]
  if 'map' == cmd:
    user_dict_file = sys.argv[2]
    product_words_file = sys.argv[3]
    map(user_dict_file, product_words_file)
  elif 'reduce' == cmd:
    reduce()
  else:
    print 'wrong cmd, map/reduce'
    sys.exit(1)
  sys.exit(0)
