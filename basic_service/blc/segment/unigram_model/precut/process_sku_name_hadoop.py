# coding: utf-8
import codecs
import jieba
import sys

def map(user_dict_file):

  jieba.load_userdict(user_dict_file)

  for line in sys.stdin:
    ss = line.decode('utf-8').strip().split('\t')
    if len(ss) >= 2:
      cut_result = jieba.lcut(ss[1].strip())
      print u' '.join(cut_result).encode('utf-8')

def reduce():
  for line in sys.stdin:
    print line.strip()

if __name__ == '__main__':
  cmd = sys.argv[1]
  user_dict_file = sys.argv[2]
  if 'map' == cmd:
    map(user_dict_file)
  elif 'reduce' == cmd:
    reduce()
  else:
    print 'wrong cmd, map/reduce'
    sys.exit(1)
  sys.exit(0)
