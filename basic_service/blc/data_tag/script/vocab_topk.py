#coding=utf8
import sys

def write_topk_vocab(vocab_file, topk_file, ratio):
  vocab_dict = dict()
  with open(vocab_file, 'r') as f:
    for line in f:
      items = line.strip().split()
      if len(items) < 2:
        continue
      word = items[0]
      cnt = int(items[1])
      vocab_dict[word] = vocab_dict.get(word, 0) + cnt
  total_count = sum(vocab_dict.values())
  total_vocab = vocab_dict.items()
  total_vocab.sort(key=lambda x:x[1], reverse=True)
  ftopk = open(topk_file, 'w')
  max_count = int(total_count * ratio)
  part_count = 0
  for vocab, count in total_vocab:
    part_count += count
    s = '%s\t%s\n' % (vocab, count)
    ftopk.write(s)
    if part_count >= max_count :
      break

if __name__ == '__main__':
  vocab_file = sys.argv[1]
  topk_file = sys.argv[2]
  ratio = float(sys.argv[3])
  write_topk_vocab(vocab_file, topk_file, ratio)

