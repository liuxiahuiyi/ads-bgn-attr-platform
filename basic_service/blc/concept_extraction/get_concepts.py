# coding: utf-8
import codecs
from six.moves import cPickle
from get_pos_probabilities import check_concept

with open('intermediate/pos_probability.pkl', 'r') as f:
  p_concept, p_non_concept, p_nothing = cPickle.load(f)

pos_decay = list()
for i in range(len(p_concept)):
  pos_decay.append(1.0-float(i)/float(len(p_concept)))

sorted_concepts = set()

def get_concept_probabilities(use_pos_decay=True):
  concept_probabilities = dict()
  last_pos = len(p_concept)
  with codecs.open('intermediate/cellphone_sku_name_processed.txt', 'r', 'utf-8') as f:
    for line in f.readlines():
      phrases = line.strip().split()
      for i in range(min(len(phrases), last_pos)):
        phrase = phrases[i]
        if check_concept(phrase) == 1:
          pos_prob = p_concept[i]
          if use_pos_decay:
            pos_prob *= pos_decay[i]
          if concept_probabilities.has_key(phrase):
            concept_probabilities[phrase][0] += pos_prob
            concept_probabilities[phrase][1] += 1
          else:
            concept_probabilities[phrase] = [pos_prob, 1]
  
  concepts = list()
  for (k, v) in concept_probabilities.items():
    concepts.append((k, float(v[0])/float(v[1]), v[1]))

  return sorted(concepts, key = lambda x: x[1], reverse=True)

def cross_validate(word):
  if len(sorted_concepts) == 0:
    with codecs.open('intermediate/sorted_concepts.txt', 'r', 'utf-8') as f:
      for line in f.readlines():
        sorted_concepts.add(line.strip())
  if word in sorted_concepts:
    return True
  else:
    return False
         
def main():
  cp = get_concept_probabilities()
  with codecs.open('result/concepts_with_probability_pos_decay_2.txt', 'w', 'utf-8') as f:
    for concept, prob, freq in cp:
      f.write(concept + ': ' + str(prob) + ': ' + str(freq) + '\n')
  with codecs.open('result/concepts_with_probability_pos_decay_filtered_2.txt', 'w', 'utf-8') as f:
    for concept, prob, freq in cp:
      if cross_validate(concept):
        f.write(concept + ': ' + str(prob) + ': ' + str(freq) + '\n')

if __name__ == '__main__':
  main()
