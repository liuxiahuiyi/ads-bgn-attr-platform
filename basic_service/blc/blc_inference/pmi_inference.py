import codecs
import copy
from six.moves import cPickle
import os
import math


concepts = dict()
entities = dict()
father_dict = dict()
son_dict = dict()
cooccurrence_dict = dict()
n_instance = 0
n_entity = dict()

entities_have_no_concept = set()

basic_data_file = 'intermediate/basic_data.pkl'
all_blc_lists_file = 'intermediate/all_blc_lists.pkl'
entities_have_no_concept_file = 'intermediate/entities_have_no_concept.pkl'

concept_hash_file = 'graph_data/concept_hash.txt'  
entity_hash_file = 'graph_data/entity_hash_map'  
concept_concept_edge_file = 'graph_data/graph_edge.txt'  
entity_concept_edge_file = 'graph_data/entity_concept_edge'  
entity_concept_weight_file = 'graph_data/entity_concept_weight'  

sku_entity_map_file = 'graph_data/sku_entityhash_map'  
entity_sku_count_file = 'graph_data/entity_sku_count'  

def read_graph_data_file(filename, split_note='\01'):
  with codecs.open(filename, 'r', 'utf-8') as f:
    for line in f.readlines():
      r = line.strip().split(split_note)
      yield r

def cooccurrence(entity, concept):
  if cooccurrence_dict.has_key((entity, concept)):
    return cooccurrence_dict[(entity, concept)]
  else:
    return 0

def init_data():
  global concepts 
  global entities
  global father_dict 
  global son_dict 
  global cooccurrence_dict
  global n_instance
  global n_entity

  if os.path.exists(basic_data_file):
    with open(basic_data_file, 'r') as f:
      concepts, entities, father_dict, son_dict, cooccurrence_dict, n_instance = cPickle.load(f)
  else:
    # build concept dict and entity dict, which maps name to id (hash code)
    for r in read_graph_data_file(concept_hash_file):
      concepts[r[0]] = r[1]
    for r in read_graph_data_file(entity_hash_file):
      entities[r[0]] = r[1]
    # build father dict, key is son, value is father set
    # build son dict, key is father, value is son set
    for r in read_graph_data_file(concept_concept_edge_file):
      son = r[0]
      father = r[1]
      if not father_dict.has_key(son):
        father_dict[son] = set()
      father_dict[son].add(father)
      if not son_dict.has_key(father):
        son_dict[father] = set()
      son_dict[father].add(son)
    for r in read_graph_data_file(entity_concept_edge_file):
      son = r[0]
      father = r[1]
      if not father_dict.has_key(son):
        father_dict[son] = set()
      father_dict[son].add(father)
      if not son_dict.has_key(father):
        son_dict[father] = set()
      son_dict[father].add(son)

#      if son == u'entity_1404':
#        print 'get it'
#        print son
#        print father
#        print son_dict[u'concept_7473']

    # build cooccurrence dict, which indicates the cooccurrence of entity and concept
    for r in read_graph_data_file(entity_concept_weight_file):
      e_c_pair = r[0].split(',')
      cooccurrence_dict[(e_c_pair[0],e_c_pair[1])] = int(r[1])

    # get n_instance, we count all sku number as n_instance
    with codecs.open(sku_entity_map_file, 'r', 'utf-8') as f:
      n_instance = len(f.readlines())

    for r in read_graph_data_file(entity_sku_count_file):
      n_entity[r[0]] = int(r[1])
    
    with open(basic_data_file, 'w') as f:
      cPickle.dump((concepts, entities, father_dict, son_dict, cooccurrence_dict, n_instance), f)

  print 'built basic data'
#  print son_dict[u'concept_7473']



def find_all_father_nodes(node):
  if not father_dict.has_key(node):
    return set()
  else:
    result = copy.copy(father_dict[node])
    for father in father_dict[node]:
      result = result | find_all_father_nodes(father)
    return result
  

def find_all_son_nodes(node):
  if not son_dict.has_key(node):
    return set()
  else:
    result = copy.copy(son_dict[node])
    for son in son_dict[node]:
      result = result | find_all_son_nodes(son)
    return result

def find_blc(entity, concept_set):
#  epsilon = 0.001
  epsilon = 0.00001
  n_e = 0
  if entity in n_entity.keys():
    n_e = n_entity[entity] 
  p_e = float(n_e) / float(n_instance)

  blc_list = list()
  for concept in concept_set:
#    son_set = set()
#    son_nodes_result = find_all_son_nodes(concept, son_set)
    son_nodes_result = find_all_son_nodes(concept)
    if concept == u'concept_1850':
      print son_nodes_result
    all_ei_in_c = son_nodes_result - set(concepts.keys())
    
    n_c_e = cooccurrence(entity, concept)
    sigma_n_c_ei = sum(cooccurrence(ei, concept) for ei in all_ei_in_c)
#    p_e_c = (float(n_c_e) + epsilon) / (float(sigma_n_c_ei) + epsilon*float(n_instance))
    p_e_c = (float(n_c_e) + epsilon) / (float(sigma_n_c_ei) + epsilon)

#    sigma_n_ci_e = sum(cooccurrence(entity, ci) for ci in concept_set)
#    p_c_e = (float(n_c_e) + epsilon) / (float(sigma_n_ci_e) + epsilon*float(n_instance))

    rep_e_c = math.log(p_e_c) - math.log(p_e)

#    if rep_e_c > 0:
    blc_list.append((concept, rep_e_c, n_c_e, sigma_n_c_ei, n_e, p_e_c, p_e))

  return sorted(blc_list, key = lambda x: x[1], reverse=True)

def build_all_blc_lists():
  if os.path.exists(all_blc_lists_file):
    with open(all_blc_lists_file, 'r') as f:
      all_blc_lists = cPickle.load(f)
    return all_blc_lists
  else:
    i = 0
    all_blc_lists = list()
    for entity in entities.keys():
#      father_set = set()
#      concept_set = find_all_father_nodes(entity, father_set)
      concept_set = find_all_father_nodes(entity)
      if len(concept_set) == 0:
        entities_have_no_concept.add(entity)
        continue
      blc = find_blc(entity, concept_set)
      all_blc_lists.append((entity,blc))
      if i % 200 == 0:
        print i
      i += 1
    with open(all_blc_lists_file, 'w') as f:
      cPickle.dump(all_blc_lists, f)
    with open(entities_have_no_concept_file, 'w') as f:
      cPickle.dump(entities_have_no_concept, f)
    return all_blc_lists
  
def main():
  init_data()
  all_blc_lists = build_all_blc_lists()
  with codecs.open('result/pmi.txt', 'w', 'utf-8') as f, codecs.open('result/pmi_relation.txt', 'w', 'utf-8') as f1:
    for blc in all_blc_lists:
      entity = blc[0]
      concepts_with_rep_list = blc[1]
      f.write('entity: ' + entities[entity] + '\n')
      f.write('max pmi: ' + concepts[concepts_with_rep_list[0][0]] + '    ' + str(concepts_with_rep_list[0][1]) + '\n')
      f.write('concepts: \n')
      for x in concepts_with_rep_list:
        f.write('    %s (%.5f: %.3f=%s/%s, %.3f=%s/%s)\n' % (concepts[x[0]], x[1], x[5], x[2], x[3], x[6], x[4], n_instance))
      f.write('\n')
      f1.write(entity+'\01'+blc[1][0][0]+'\n')


if __name__ == '__main__':
  main()




