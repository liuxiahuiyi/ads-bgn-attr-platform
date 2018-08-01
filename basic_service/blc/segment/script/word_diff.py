import sys
import json

true_count = 0
both_count = 0
alg_count = 0

for line in sys.stdin:
  try:
    items = line.strip().split('\t')
    label = json.loads(items[0])
    true_word_set = set()
    for data in label:
      word = data['quote']
      if len(word) > 4:
        continue
      true_word_set.add(data['quote'].encode('utf8'))
    alg_word_set = set(items[1].split())
    #print ' '.join(true_word_set)
    #print ' '.join(alg_word_set)
    #print ' '.join(true_word_set & alg_word_set)
    #print
    true_count += len(true_word_set)
    alg_count += len(alg_word_set)
    both_count += len(true_word_set & alg_word_set)
  except Exception, e:
    print e
print 'ture_count=%s both_count=%s recall=%s' % (true_count, both_count, float(both_count) / true_count)
print 'alg_count=%s both_count=%s acc=%s' % (alg_count, both_count, float(both_count) / alg_count)
