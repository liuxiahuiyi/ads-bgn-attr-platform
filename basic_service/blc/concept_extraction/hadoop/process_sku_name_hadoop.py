# coding: utf-8
import codecs
import jieba
import sys
from string import maketrans

def map(user_dict_file):

  jieba.load_userdict(user_dict_file)

  punct = set(u''':!),:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
  ﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
  々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
  ︽︿﹁﹃﹙﹛﹝（｛“‘-—_…*/+.''')

  signrepl = u' ' * len(punct)
  table = {ord(s): unicode(d) for s, d in zip(punct, signrepl)}
#  table = maketrans(punct, signrepl)

  for line in sys.stdin:
    ss = line.decode('utf-8').strip().split('\t')
    if len(ss) >= 2:
      s = ss[1].strip().translate(table) 
      output = u' '.join(jieba.lcut(s))
      result = (ss[0].strip() + '\01' + output).encode('utf-8')
      print result

def reduce():
  for line in sys.stdin:
    print line.strip()

if __name__ == '__main__':
  if len(sys.argv) != 3:
    print 'usage: python process_sku_name_hadoop cmd user_dict_file_name'
    sys.exit(1)
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
