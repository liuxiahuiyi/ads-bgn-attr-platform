import codecs

words = list()
with codecs.open('intermediate/cellphone_name_from_natural_concepts.txt', 'r', 'utf-8') as f:
  for line in f.readlines():
    words.append(line.strip())

newwords = sorted(words)
with codecs.open('intermediate/sorted_concepts.txt', 'w', 'utf-8') as f:
  for word in newwords:
    f.write(word + '\n')
