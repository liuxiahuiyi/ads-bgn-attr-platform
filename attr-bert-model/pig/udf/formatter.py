# -*- coding: UTF-8 –*-
from __future__ import division
from collections import defaultdict
import re
import math
import sys
import inspect
import os
sys.path.append(os.path.dirname(os.path.abspath(inspect.stack()[0][1])))
import util

@outputSchema("m:bag{t:(item_sku_id:chararray,sku_name:chararray,com_attr_value_cd:chararray,com_attr_value_name:chararray)}")
def format(t, bag):
  output = []
  if hasattr(Formatter, 'c1%s_c2%s_c3%s_a%s' % (t[0], t[1], t[2], t[4])):
    output = getattr(Formatter, 'c1%s_c2%s_c3%s_a%s' % (t[0], t[1], t[2], t[4]))(bag)
  elif hasattr(Formatter, 'c1%s_c2%s_a%s' % (t[0], t[1], t[4])):
    output = getattr(Formatter, 'c1%s_c2%s_a%s' % (t[0], t[1], t[4]))(bag)
  elif hasattr(Formatter, 'c1%s_a%s' % (t[0], t[4])):
    output = getattr(Formatter, 'c1%s_a%s' % (t[0], t[4]))(bag)
  elif hasattr(Formatter, 'a%s' % t[4]):
    output = getattr(Formatter, 'a%s' % t[4])(bag)
  else:
    output = getattr(Formatter, 'default')(bag)
  assert(len(set([(e[0], e[1]) for e in output])) == len(output))
  return output

class Formatter:
  def __init__(self):
    pass
  @classmethod
  def default(cls, bag):
    minimun = 1e-3
    stop_words = [u'其他', '', u'其它']
    d = defaultdict(list)
    for item_sku_id, sku_name, com_attr_value_cd, com_attr_value_name, attr_ratio in bag:
      if com_attr_value_name.strip() in stop_words or attr_ratio < minimun:
        continue
      d[(item_sku_id, sku_name)].append((com_attr_value_cd, com_attr_value_name, attr_ratio))
    output = []
    for k, v in d.items():
      v.sort(key = lambda x: x[2], reverse = True)
      output.append(k + v[0][0:-1])
    return output

  @classmethod
  def c11319_a2342(cls, bag):
    minimun = 1e-3
    d = defaultdict(list)
    units = [u'下', u'月', u'岁', u'上']
    result = []
    for item_sku_id, sku_name, com_attr_value_cd, com_attr_value_name, attr_ratio in bag:
      if attr_ratio < minimun or util.convertApplicableAge((com_attr_value_cd, com_attr_value_name)) == None:
        continue
      d[(item_sku_id, sku_name)].append(util.convertApplicableAge((com_attr_value_cd, com_attr_value_name)))
    for sku, ages in d.items():
      ages = list(set(ages))
      ages.sort(key = lambda age: [units.index(age[1][-1])] +
        ([units.index(age[1][-3])] if age[1][-3] in units else [0]) +
        list( map(lambda x: float(x), re.compile(r'\d+\.?\d*').findall(age[1])) ))  

      bound1 = re.compile(r'\d+\.?\d*').findall(ages[0][1])
      bound2 = re.compile(r'\d+\.?\d*').findall(ages[-1][1])
      if ages[0][1][-1] == u'下':
        min_age = 0.0
      elif ages[0][1][-1] == u'月':
        min_age = float(bound1[0]) / 12
      elif ages[0][1][-1] == u'岁':
        min_age = float(bound1[0])
      elif ages[0][1][-3] == u'月':
        min_age = float(bound1[0]) / 12
      else:
        min_age = float(bound1[0])
      if ages[-1][1][-1] == u'下':
        max_age = float(bound2[0])
      elif ages[-1][1][-1] == u'月':
        max_age = float(bound2[1]) / 12
      elif ages[-1][1][-1] == u'岁':
        max_age = float(bound2[1])
      else:
        max_age = 100.0
      result.append(sku + util.searchAge(min_age, max_age))
    return result

  @classmethod
  def c11319_c214941_a2342(cls, bag):
    minimun = 1e-3
    units = [u'下', u'月', u'岁', u'上']
    new_bag = [(item_sku_id, sku_name) + util.convertApplicableAge((com_attr_value_cd, com_attr_value_name))
                for item_sku_id, sku_name, com_attr_value_cd, com_attr_value_name, attr_ratio in bag
                if attr_ratio >= minimun and
                util.convertApplicableAge((com_attr_value_cd, com_attr_value_name)) != None]
    if len(new_bag) == 0:
      return []
    tmp = list(zip(*new_bag))
    skus = list(set(zip(*tmp[0:2])))
    ages = list(set(zip(*tmp[2:4])))
    ages.sort(key = lambda age: [units.index(age[1][-1])] +
      ([units.index(age[1][-3])] if age[1][-3] in units else [0]) +
      list( map(lambda x: float(x), re.compile(r'\d+\.?\d*').findall(age[1])) ))
    d = defaultdict(list)
    sizes = {'XS': 0, 'S': 1, 'M': 2, 'L': 3, 'XL': 4, 'XXL': 5, 'XXXL': 6, '2XL': 5, '3XL': 6, 'NB': 0}
    for sku in skus:
      key1 = re.compile('[^|/a-zA-Z](' + '|'.join(sizes.keys()) + ')[^|/a-zA-Z]').findall(sku[1])
      if len(key1) >= 1:
        key1 = (sizes[key1[-1]],)
      else:
        key1 = (0,)
      key2 = tuple( map(lambda x: float(x), re.compile(u'(\d+\.?\d*)[^片条件包]').findall(sku[1])[-1:-3:-1]) )
      d[key1 + key2].append(sku)
    sort_key = sorted(d.keys())
    ratio = max(1, round(len(d) / len(ages)))   
    result = []
    for i, key in enumerate(sort_key):
      pos = i // ratio
      for sku in d[key]:
        result.append(sku + ages[min(int(pos), len(ages) - 1)])
    return result

  @classmethod
  def c11319_c211842_a2342(cls, bag):
    return cls.c11319_c214941_a2342(bag)
  @classmethod
  def c11319_c26313_a2342(cls, bag):
    return cls.c11319_c214941_a2342(bag)
  @classmethod
  def c11319_c21525_a2342(cls, bag):
    return cls.c11319_c214941_a2342(bag)

  @classmethod
  def c11319_c21523_a2342(cls, bag):
    minimun = 1e-3
    d = defaultdict(list)
    units = [u'下', u'月', u'岁', u'上']
    result = []
    for item_sku_id, sku_name, com_attr_value_cd, com_attr_value_name, attr_ratio in bag:
      if attr_ratio < minimun or util.convertApplicableAge((com_attr_value_cd, com_attr_value_name)) == None:
        continue
      d[(item_sku_id, sku_name)].append(util.convertApplicableAge((com_attr_value_cd, com_attr_value_name)))
    for sku, ages in d.items():
      ages = list(set(ages))
      ages.sort(key = lambda age: [units.index(age[1][-1])] +
        ([units.index(age[1][-3])] if age[1][-3] in units else [0]) +
        list( map(lambda x: float(x), re.compile(r'\d+\.?\d*').findall(age[1])) ))  

      if re.compile(u'[^段]{3}[初1一]段[^段]{3}').search(sku[1]) != None:
        min_age = 0.0
        max_age = 0.5
      elif re.compile(u'[^段]{3}[2二]段[^段]{3}').search(sku[1]) != None:
        min_age = 0.5
        max_age = 1.0
      elif re.compile(u'[^段]{3}[3三]段[^段]{3}').search(sku[1]) != None:
        min_age = 1.0
        max_age = 3.0
      elif re.compile(u'[^段]{3}[4四]段[^段]{3}').search(sku[1]) != None:
        min_age = 3.0
        max_age = 6.0
      else:
        bound1 = re.compile(r'\d+\.?\d*').findall(ages[0][1])
        bound2 = re.compile(r'\d+\.?\d*').findall(ages[-1][1])
        if ages[0][1][-1] == u'下':
          min_age = 0.0
        elif ages[0][1][-1] == u'月':
          min_age = float(bound1[0]) / 12
        elif ages[0][1][-1] == u'岁':
          min_age = float(bound1[0])
        elif ages[0][1][-3] == u'月':
          min_age = float(bound1[0]) / 12
        else:
          min_age = float(bound1[0])
        if ages[-1][1][-1] == u'下':
          max_age = float(bound2[0])
        elif ages[-1][1][-1] == u'月':
          max_age = float(bound2[1]) / 12
        elif ages[-1][1][-1] == u'岁':
          max_age = float(bound2[1])
        else:
          max_age = 100.0
      result.append(sku + util.searchAge(min_age, max_age))
    return result

