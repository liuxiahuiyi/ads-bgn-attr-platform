import codecs
import numpy as np
from six.moves import cPickle

brands_and_colors = set()
stopwords = set()

def check_brands_and_colors(word):
  if len(brands_and_colors) == 0:
    with codecs.open('intermediate/brands.txt', 'r', 'utf-8') as f1, \
         codecs.open('intermediate/colors_ext.txt', 'r', 'utf-8') as f2:
      for line in f1.readlines():
        brands_and_colors.add(line.strip().lower())
      for line in f2.readlines():
        brands_and_colors.add(line.strip().lower())
  if word.lower() in brands_and_colors:
    return True
  else:
    return False

def check_eng_num_word(word):
  for ch in word:
    if ch < u'\u4e00' or ch > u'\u9fff':
      return True
  return False

def check_stopword(word):
  if len(stopwords) == 0:
    with codecs.open('stopwords.txt', 'r', 'utf-8') as f:
      for line in f.readlines():
        word = line.strip()
        if len(word) > 0:
          stopwords.add(word)
  for stopword in stopwords:
    if stopword in word:
      return True
  return False

# concept: 1, non concept: -1
def check_concept(word):
  if check_brands_and_colors(word) or check_eng_num_word(word) or check_stopword(word) or len(word) < 2:
    return -1
  else:
    return 1

def get_statistics():
  max_length = 0
  raw_matrix = list()
  with codecs.open('intermediate/cellphone_sku_name_processed.txt', 'r', 'utf-8') as f:
    for line in f.readlines():
      tmplist = list()
      for word in line.split():
        tmplist.append(check_concept(word))
      if len(tmplist) > max_length:
        max_length = len(tmplist)
      raw_matrix.append(tmplist)
  sample_num = len(raw_matrix)
  aligned_matrix = np.zeros([sample_num, max_length], dtype=np.int32)
  for i in range(sample_num):
    aligned_matrix[i,:] = np.pad(raw_matrix[i], (0, max_length-len(raw_matrix[i])), 'constant', constant_values=(0,0))
  return aligned_matrix

def calculate_pos_probability(aligned_matrix):
  p_concept = list()
  p_non_concept = list()
  p_nothing = list()
  all_count = aligned_matrix.shape[0]
  for i in range(aligned_matrix.shape[1]):
    cur_list = aligned_matrix[:,i].tolist()
    concept_count = cur_list.count(1)
    non_concept_count = cur_list.count(-1)
    nothing_count = cur_list.count(0)
    p_concept.append(float(concept_count)/float(all_count))
    p_non_concept.append(float(non_concept_count)/float(all_count))
    p_nothing.append(float(nothing_count)/float(all_count))
  return p_concept, p_non_concept, p_nothing

def main():
  aligned_matrix = get_statistics()
  p_concept, p_non_concept, p_nothing = calculate_pos_probability(aligned_matrix)
  with open('intermediate/pos_probability.pkl', 'w') as f:
    cPickle.dump((p_concept, p_non_concept, p_nothing), f)

#  print aligned_matrix[0:8,:]
#  print p_concept
#  print p_non_concept
#  print p_nothing


if __name__ == '__main__':
  main()

   
