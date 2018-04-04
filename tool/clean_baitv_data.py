import re


with open('baitv.com.zho-CHN.UTF8.tsv') as data_file:
    lines = data_file.readlines()

a1 = re.compile('\(.*\)')
a2 = re.compile('-*\d{4}-\d+')
text_list = list()
for line in lines:
    # test
    # line = line.strip()
    # match = a1.search(line)
    # if match:
    #     print("%s match the ()" % match.string)
    # match = a2.search(line)
    # if match:
    #     print("%s match the -" % match.string)

    text = line
    text = re.sub(a1, '', text)
    text = re.sub(a2, '', text)
    print(text.strip())
    text_list.append(text)

with open('baitv.com.zho-CHN.UTF8_new.tsv', 'w') as data_file:
    data_file.writelines(text_list)

