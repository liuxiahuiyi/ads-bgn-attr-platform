#coding=utf8
#author=zhangyunfei
import sys
import codecs
from collections import defaultdict
from trietree import TrieTree
from full_cutter import match_vocab

class VocabMatcher(object):

  def __init__(self, vocab_file, vocab_type, sep=None, user_key=None, value_min_count=2):
    keys = set()
    invert_index = defaultdict(set)
    f = codecs.open(vocab_file, 'r', 'utf8')
    for line in f:
      key, value = None, None
      if vocab_type == 'value':
        value = line.strip()
        key = user_key
      elif vocab_type == 'key_value':
        items = line.strip().split(sep)
        key, value = items[0], items[1]
      elif vocab_type == 'value_key':
        items = line.strip().split(sep)
        key, value = items[1], items[0]
      else:
        raise Exception('VocabMather unknow vocab_type=%s' % vocab_type)

      if key and value and len(value) >= value_min_count:
        invert_index[value].add(key)
        keys.add(key)

    # build match tree
    vocab_dict = {word : 'ph' for word in invert_index if word}
    self.value_tree = TrieTree(vocab_dict)
    self.keys = keys
    self.invert_index = invert_index
  
  def get_keys(self):
    return self.keys

  def match_all(self, sentence, start):
    '''
    match all vocabs and find keys
    return : list of [(end, key), (end, key), ...]
    '''
    match_keys = list()
    end_list = match_vocab(sentence, self.value_tree.root, start)
    for end in end_list:
      for key in self.invert_index[sentence[start:end]]:
        match_keys.append((end, key))
    return match_keys

  def match_max(self, sentence, start):
    '''
    match all vocabs and find keys
    return : end, keys
    if end == -1 : not found
    '''
    match_keys = list()
    end_list = match_vocab(sentence, self.value_tree.root, start)
    if len(end_list) == 0:
      return -1, None
    max_end = max(end_list)
    return max_end, self.invert_index[sentence[start:max_end]]

if __name__ == '__main__':
  vocab_file = sys.argv[1]
  vocab_type = sys.argv[2]
  sep = sys.argv[3]
  user_key = sys.argv[4]
  matcher = VocabMatcher(vocab_file, vocab_type, sep, user_key)
  print 'keys=%s' % (len(matcher.get_keys()))
  sentence = u'飞利浦（PHILIPS）咖啡机 家用全自动现磨一体带咖啡豆研磨功能 HD7751/00'
  for start in range(len(sentence)):
    end_list = matcher.match_all(sentence, start)
    for end, key in end_list:
      print 'start=%s end=%s word=%s key=%s' % (start, end, sentence[start:end], key)
  
  print '-------max end------'
  for start in range(len(sentence)):
    end, keys = matcher.match_max(sentence, start)
    if end  == -1:
      continue
    for key in keys:
      print 'start=%s end=%s word=%s key=%s' % (start, end, sentence[start:end], key)
