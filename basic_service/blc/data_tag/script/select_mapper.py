#coding=utf8
import sys
import random

def load_vocab(vocab_file, topk):
  vocab_dict = dict()
  f = open(vocab_file, 'r')
  for line in f:
    vocab, count = line.strip().split()
    count = int(count)
    vocab_dict[vocab] = count
  vocab_dict = vocab_dict.items()
  vocab_dict.sort(key=lambda x:x[1], reverse=True)
  vocab_dict = vocab_dict[:topk]
  return {k:v for k,v in vocab_dict}

# 蓄水池算法抽样
def add_skuid(vocab_dict, multiple, word, skuid):
  if word not in vocab_dict:
    return False

  if vocab_dict[word][1] < multiple:
    vocab_dict[word][0].append(skuid)
    vocab_dict[word][1] += 1
    return True
   
  vocab_dict[word][1] += 1
  m = random.randint(0, 100000000) % vocab_dict[word][1]
  if m < multiple:
    vocab_dict[word][0][m] = skuid
    return True
  return False

def mapper(vocab_file, topk, multiple):
  vocab_dict = load_vocab(vocab_file, topk)
  vocab_dict = {k:[list(), 0] for k in vocab_dict}
  for line in sys.stdin:
    try:
      items = line.strip().split('\t')
      skuid, words = items[:2]
      words = words.split()
      random.shuffle(words)
      for word in words:
        if add_skuid(vocab_dict, multiple, word, skuid):
          break
    except Exception, e:
      pass
  
  for word in vocab_dict:
    if len(vocab_dict[word][0]) == 0:
      continue
    print '%s\t%s' % (word, ' '.join(vocab_dict[word][0]))

if __name__ == '__main__':
  vocab_file = sys.argv[1]
  topk = int(sys.argv[2])
  multiple = max(1, int(sys.argv[3]))
  mapper(vocab_file, topk, multiple)
