#coding=utf8
import sys
import json
import codecs

def write_seq_note_vocab(data_file, outfile, threshold):
  vocab_dict = dict()
  f = open(data_file, 'r')
  for line in f:
    try:
      items = line.strip().split('\t')
      seg_list = json.loads(items[3])
      for seg in seg_list:
        word = seg['quote']
        tag = seg['text']
        if word not in vocab_dict:
          vocab_dict[word] = dict()
        vocab_dict[word][tag] = vocab_dict[word].get(tag, 0) + 1
    except Exception, e:
      print 'write_seq_vocab failed e=%s' % e

  f = codecs.open(outfile, 'w', 'utf8')
  for word in vocab_dict:
    count = sum(vocab_dict[word].values())
    if count < threshold:
      continue
    tags = vocab_dict[word].items()
    tags.sort(key=lambda x:x[1], reverse=True)
    tags = ['%s:%s' % (tag[0], tag[1]) for tag in tags]
    s = '%s\t%s\t%s\n' % (word, count, '\t'.join(tags))
    f.write(s)

if __name__ == '__main__':
  note_file = sys.argv[1]
  outfile = sys.argv[2]
  threshold = 2
  write_seq_note_vocab(note_file, outfile, threshold)
