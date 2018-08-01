#coding=utf8
import sys

def load_vocab(vocab_file):
  vocab_dict = dict()
  with open(vocab_file, 'r') as f:
    for line in f:
      try:
        items = line.strip().split('\t')
        word = items[0]
        count = int(items[1])
        vocab_dict[word] = count
      except Exception, e:
        print 'load_vocab failed e=%s' % e
  return vocab_dict

def gen_tag_tuple(outfile, data_file, vocab_dict, count):
  full_size = 0
  tag_tuple_dict = dict()
  f = open(data_file, 'r')
  for idx, line in enumerate(f):
    if idx % 80000 == 0:
      print 'gen_tag_tuple process line=%s full_size=%s' % (idx, full_size)
    line = line.strip()
    items = line.split('\t')
    words = items[1].split(' ')
    for word in words:
      if word not in vocab_dict:
        continue
      if word not in tag_tuple_dict:
        tag_tuple_dict[word] = list()
      if len(tag_tuple_dict[word]) >= count:
        continue
      tag_tuple_dict[word].append(line)
      if len(tag_tuple_dict[word]) >= count:
        full_size += 1
      break
    if full_size >= len(vocab_dict):
      break

  # 按顺序写入
  vocab_dict = vocab_dict.items()
  vocab_dict.sort(key=lambda x:x[1], reverse=True)
  f = open(outfile, 'w')
  no = 1
  for word, count in vocab_dict:
    if word not in tag_tuple_dict:
      continue
    s = '%s\t%s\n%s\n\n' % (no, word, '\n'.join(tag_tuple_dict[word]))
    f.write(s)
    no += 1

if __name__ == '__main__':
  outfile = sys.argv[1]
  title_file = sys.argv[2]
  vocab_file = sys.argv[3]
  count = int(sys.argv[4])
  
  vocab_dict = load_vocab(vocab_file)
  print 'vocab_size=%s' % (len(vocab_dict))

  gen_tag_tuple(outfile, title_file, vocab_dict, count)
