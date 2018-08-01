#coding=utf8
import sys

def is_chinese(uchar):         
    if u'\u4e00' <= uchar<=u'\u9fff':
        return True
    else:
        return False

def is_filter_word(word):
  cn_count = 0
  for char in word:
    if is_chinese(char):
      cn_count += 1
  return cn_count < 2

def mapper():
  vocab_dict = dict()
  for line in sys.stdin:
    try:
      words = line.strip().split('\t')[1].split()
      for word in words:
        if is_filter_word(word.decode('utf8')):
          continue
        vocab_dict[word] = vocab_dict.get(word, 0) + 1
    except Exception, e:
      pass
  
  for word in vocab_dict:
    print '%s\t%s' %(word, vocab_dict[word])

if __name__ == '__main__':
  mapper()

