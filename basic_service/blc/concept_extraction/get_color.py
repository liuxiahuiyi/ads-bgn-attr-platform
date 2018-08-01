# coding: utf-8
import codecs

colors = u'金银蓝黑黄橙粉红绿灰白紫青棕色棕'
color_list = list()

with codecs.open('intermediate/cellphone_name_from_natural.txt','r','utf-8') as f:
  for line in f.readlines():
    word = line.split('\t')[0].strip()
    for color in colors:
      if word.endswith(color):
        color_list.append(word)
        break
with codecs.open('intermediate/colors.txt', 'w', 'utf-8') as f:
  for color in color_list:
    f.write(color+'\n')
