#coding=utf8
#author=zhangyunfei

import sys
import os
import copy
from trietree import TrieTree

def match_vocab(sentence, root, start):
    """
    Args:
        sentence: str
        root: Node, root node
        start: int, 句子的起始位置
    Return:
        list of [end_position]
    """
    match_list = list()
    current_node = root
    end = start
    while (end < len(sentence)) and \
          (sentence[end] in current_node.children):
      current_node = current_node.children[sentence[end]]
      if current_node.end_flag:
        match_list.append(end+1)
      end += 1
    return match_list

class Node(object):
  
  def __init__(self, end, match_vocab):
    self.end = end
    self.match_vocab = match_vocab

class PositionNode(object):
  
  def __init__(self, start, nodes):
    self.start = start
    self.nodes = nodes

class CutGraph(object):
  
  def __init__(self, sentence, seg_list):
    '''
    sentence : string
    seg_list : list of PositionNode
    '''
    self.sentence = sentence
    self.seg_dict = dict() # {pos : EndNodes}
    for node in seg_list:
      if node.start not in self.seg_dict:
        self.seg_dict[node.start] = node.nodes
      else:
        self.seg_dict[node.start] = list(set(self.seg_dict[node.start].nodes + node.nodes))

  def print_graph(self):
    for pos in self.seg_dict:
      print 'start=%s' % pos
      for node in self.seg_dict[pos]:
        print 'end=%s match_vocab=%s word=%s' % (node.end, node.match_vocab, sentence[pos:node.end])
      print

def do_max_coverage_path(graph, start, best_match_stack, best_path_metric, path_stack, path_metric):
  if start not in graph.seg_dict:
    if (path_metric[0] > best_path_metric[0]) or \
       (path_metric[0] == best_path_metric[0] and path_metric[1] < best_path_metric[1]):
      # convert metric
      best_path_metric[0] = path_metric[0]
      best_path_metric[1] = path_metric[1]
      # convert path
      best_match_stack[:] = []
      for path in path_stack:
        best_match_stack.append(copy.deepcopy(path))
    return
  # walk graph
  end_nodes = graph.seg_dict[start]
  for end_node in end_nodes:
    end_pos = end_node.end
    path_stack.append([start, end_pos, end_node.match_vocab])
    if end_node.match_vocab:
      path_metric[0] += end_pos - start
      path_metric[1] += 1
    do_max_coverage_path(graph, end_pos, best_match_stack, best_path_metric, path_stack, path_metric)
    if end_node.match_vocab:
      path_metric[1] -= 1
      path_metric[0] -= end_pos - start
    path_stack.pop()

def max_coverage_path(graph):
  '''
  return : list of trunck [(start, end, match_vocab), ....]
  '''
  best_path_stack = list() # list of (start, end, match_vocab)
  best_path_metric = [-1, -1] # 1.match char count; 2. cut count
  path_stack = list()
  path_metric = [0, 0]
  do_max_coverage_path(graph, 0, best_path_stack, best_path_metric, path_stack, path_metric)
  return best_path_stack

def max_coverage_path_dyn(graph):
  '''
  return : list of trunck [(start, end, match_vocab), ....]
  '''
  next_pos = [-1 for x in range(len(graph.sentence))]
  match_vocab = [0 for x in range(len(graph.sentence))]
  best_metrics = [None for x in range(len(graph.sentence)+1)] # 1. max match count; 2. max cut count
  best_metrics[-1] = [0, 0]
  for i in range(len(graph.sentence)-1, -1, -1):
    for node in graph.seg_dict[i]:
      char_inc = node.end - i if node.match_vocab else 0
      word_inc = 1 if node.match_vocab else 0
      if (best_metrics[i] is None) or \
         (char_inc + best_metrics[node.end][0] > best_metrics[i][0]) or \
         ((char_inc + best_metrics[node.end][0] == best_metrics[i][0]) and \
          (word_inc + best_metrics[node.end][1] < best_metrics[i][1])):
  #      print 'pos=%s char_inc=%s word_inc=%s end=%s match=%s' % (i, char_inc, word_inc, node.end, node.match_vocab)
        best_metrics[i] = copy.deepcopy(best_metrics[node.end])
        best_metrics[i][0] = char_inc
        best_metrics[i][1] = word_inc
        match_vocab[i] = node.match_vocab
        next_pos[i] = node.end
    assert(best_metrics[i] is not None)
  
  #for i, metric in enumerate(best_metrics):
  #  print 'pos=%s max_char_count=%s max_cut_count=%s' % (i, metric[0], metric[1])

  # find max path
  best_path = list()
  pos = 0
  while pos < len(graph.sentence):
    assert(pos < next_pos[pos])
    best_path.append([pos, next_pos[pos], match_vocab[pos]])
    pos = next_pos[pos]
  return best_path

class FullCutter(object):
  
  def __init__(self, vocabs):
    '''
    vocabs: {word1, word2, ...}
    '''
    # ph means placeholder
    vocab_dict = {word : 'ph' for word in vocabs if word}
    self.tree = TrieTree(vocab_dict)

  def full_cut(self, sentence):
    '''
    sentence : string
    ret : CutGraph
    '''
    seg_list = list()
    for i in range(len(sentence)):
      end_list = match_vocab(sentence, self.tree.root, i)
      end_nodes = [Node(end, True) for end in end_list]
      if (i+1) not in end_list:
        end_nodes.append(Node(i+1, False))
      seg_list.append(PositionNode(i, end_nodes))
    return CutGraph(sentence, seg_list)

if __name__ == '__main__':
  vocabs = set([u'家用', u'用意', u'家', u'意式',  u'式', u'咖啡机'])
  cutter = FullCutter(vocabs)
  sentence = u'德华家用意式咖啡机哈哈'
  #sentence = u'咖啡机'
  graph = cutter.full_cut(sentence)
  graph.print_graph()
  print
  print
  match_segs = max_coverage_path_dyn(graph)
  for start, end, match_vocab in match_segs:
    print 'start=%s end=%s match=%s word=%s' % (start, end, match_vocab, sentence[start:end])
