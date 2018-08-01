import codecs
from six.moves import cPickle

src_file = 'word_count.txt'
dest_file = 'word_prob.pkl'

def check_all_chinese(word):
  for ch in word:
    if ch < u'\u4e00' or ch > u'\u9fff':
      return False
  return True

def check_contains_chinese(word):
  for ch in word:
    if ch >= u'\u4e00' and ch <= u'\u9fff':
      return True
  return False

prob_dict = dict()
all_count = 0
min_count = 100000000

with codecs.open(src_file, 'r', 'utf-8') as f:
  for line in f:
    try:
      word, count_str = line.strip().split('\t',1)
    except:
#      print line
      continue 
    if check_contains_chinese(word):
      count = int(count_str)
      prob_dict[word] = count
      all_count += count
      if count < min_count:
        min_count = count

for word in prob_dict:
  prob_dict[word] = float(prob_dict[word]) / float(all_count)
  if len(word) == 1:
    prob_dict[word] /= 100.0

prob_dict[u'_BGN_UNK'] = float(min_count) / float(all_count*2) 

with open(dest_file, 'w') as f:
  cPickle.dump(prob_dict, f)

