# coding: utf-8
import codecs

colors = u'金银蓝黑黄橙粉红绿灰白紫青棕色'
color_list = set()

with codecs.open('intermediate/colors.txt', 'r', 'utf-8') as f:
  for color in f.readlines():
    color_list.add(color.strip())

with codecs.open('intermediate/cellphone_sku_name_processed.txt','r','utf-8') as f:
  for line in f.readlines():
    words = line.strip().split()
    for word in words:
      for color in colors:
        if word.endswith(color):
          color_list.add(word)
          break
with codecs.open('intermediate/colors_ext.txt', 'w', 'utf-8') as f:
  for color in color_list:
    f.write(color+'\n')
