import codecs
import sys

raw_concept_list = list()
raw_concept_index = dict()

concept_list = list()

def init_data(raw_concept_file):
  global raw_concept_list
  global raw_concept_index
  with codecs.open(raw_concept_file, 'r', 'utf-8') as f:
    for line in f.readlines():
      s = line.strip()
      raw_concept_list.append(s)
      if s[0] not in raw_concept_index:
        raw_concept_index[s[0]] = list()
      raw_concept_index[s[0]].append(s)

#def check_concept_duplicate(concept):
#  flags = [0] * len(concept)
#  for c_key in raw_concept_index:
#    if c_key not in concept:
#      continue
#    for c in raw_concept_index[c_key]:
#      if len(c) >= len(concept):
#        continue
#      pos = concept.find(c)
#      if pos < 0:
#        continue
#      for i in range(len(c)):
#        flags[pos+i] += 1
#  for x in flags:
#    if x != 1:
#      return False
#  return True


def check_sub_string_coverage(start, sub_start_end_pos_dict, string_len, result_set):
  if start == string_len or True in result_set:
    return True
  if start not in sub_start_end_pos_dict:
    return False
  for end in sub_start_end_pos_dict[start]:
    result = check_sub_string_coverage(end+1, sub_start_end_pos_dict, string_len, result_set)
    result_set.add(result)
    if result:
      break
  if True in result_set:
    return True
  else:
    return False

def check_concept_duplicate(concept):
  # sub_start_end_pos is a dict
  # the key is start index
  # the value is a set of end indices from different sub string (concept)
  # for example, the whole string is "abcdefg", two sub strings: "abc", "abcde", "bcd"
  # the dict will be: {0:{2,4}, 1:{3}}
  sub_start_end_pos = dict()

  for c_key in raw_concept_index:
    if c_key not in concept:
      continue
    for c in raw_concept_index[c_key]:
      if len(c) >= len(concept):
        continue
      pos = concept.find(c)
      if pos < 0:
        continue
      if pos not in sub_start_end_pos:
        sub_start_end_pos[pos] = set()
      sub_start_end_pos[pos].add(pos+len(c)-1)
  
  result_set = set()
  check_sub_string_coverage(0, sub_start_end_pos, len(concept), result_set)
  if True in result_set:
    return True
  else:
    return False

def deduplication():
  global concept_list
  i = 0
  for concept in raw_concept_list:
    if i % 10000 == 0:
      print i
    i += 1
    if not check_concept_duplicate(concept):
      concept_list.append(concept)

def main(raw_concept_file, concept_file):
  init_data(raw_concept_file)
  deduplication()
  with codecs.open(concept_file, 'w', 'utf-8') as f:
    for s in concept_list:
      f.write('%s\n' % s)

if __name__ == '__main__':
  main(sys.argv[1], sys.argv[2])
