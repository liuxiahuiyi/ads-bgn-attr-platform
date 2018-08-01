#coding=utf8
import sys
from collections import Counter

def load_vocab(vocab_file):
  vocab_counter = Counter()
  f = open(vocab_file, 'r')
  for line in f:
    #try:
      items = line.strip().split('\t')
      vocab, count = items[0], int(items[1])
      vocab_counter[vocab] += count
    #except Exception, e:
      pass
  return vocab_counter

def get_tag_vocab(total_vocab_file, note_vocab_file, brand_vocab_file, product_vocab_file, tag_vocab_file):
  total_vocab_counter = load_vocab(total_vocab_file)
  print "%s load vocab size=%s" % (total_vocab_file, len(total_vocab_counter))

  note_vocab_counter = load_vocab(note_vocab_file)
  print "%s load vocab size=%s" % (note_vocab_file, len(note_vocab_counter))
  
  brand_vocab_counter = load_vocab(brand_vocab_file)
  print "%s load vocab size=%s" % (brand_vocab_file, len(brand_vocab_counter))
  
  product_vocab_counter = load_vocab(product_vocab_file)
  print "%s load vocab size=%s" % (product_vocab_file, len(product_vocab_counter))

  tag_vocab_counter = Counter()
  for word in total_vocab_counter:
    if word in note_vocab_counter or \
       word in brand_vocab_counter or \
       word in product_vocab_counter:
      continue
    tag_vocab_counter[word] = total_vocab_counter[word]
  print 'tag_vocab_counter_size=%s' % len(tag_vocab_counter)

  tag_vocab_counter = tag_vocab_counter.items()
  tag_vocab_counter.sort(key=lambda x:x[1], reverse=True)
  f = open(tag_vocab_file, 'w')
  for word, count in tag_vocab_counter:
    s = '%s\t%s\n' % (word, count)
    f.write(s)

if __name__ == '__main__':
  total_vocab_file = sys.argv[1]
  note_vocab_file = sys.argv[2]
  brand_vocab_file = sys.argv[3]
  product_vocab_file = sys.argv[4]
  tag_vocab_file = sys.argv[5]

  get_tag_vocab(total_vocab_file, note_vocab_file, brand_vocab_file, product_vocab_file, tag_vocab_file)

