import gzip
import time

from autonavi.autonaviParser import ENCODING

b = {"1": 2, "3": 4}
for key, value in b.items():
    print(key)
    print(value)

import re

songCleanRegex = re.compile('\(.*\)|-.*|\[.*\]|【.*】|《.*》')
song = "你在身邊 - 我不離開demo"
song1 = "周杰伦 (Jay Chou), 杨瑞代 (Gary Yang)"
song = songCleanRegex.sub('', song)
print(song)

songs = song1.split(",")


with gzip.open("temp.UTF8.tsv.gz", 'a') as handle:
    handle.write(bytes("nihao", 'utf-8'))
    handle.write(bytes('\n', 'utf-8'))


for song_1 in songs:
    song1_s = songCleanRegex.sub('', song_1)
    alternate_song = ""
    if re.search('\((.*)\)', song_1):
        alternate_song = re.search('\((.*)\)', song_1).group(1)

    print(song1_s)
    print(alternate_song)


print((time.strftime("%Y/%m/%d", time.gmtime())))



def normalizeLine(itemList):
    itemList = [re.sub(';$','', x) for x in itemList]
    itemList = [x.replace(';',',') for x in itemList]
    return itemList

b = "1	101441471B0FFGLGIG2	偙朗木门	Di Lang Mu Men		530302	060601"
s = b.split('\t')
d = normalizeLine(s)
print(d)
x = "中国石化加油站（三江大道）"
f = re.sub(';$','', x)
print(f)

line= "法律链接▲▲▲ "
a = re.sub('[■▲▼○△→]', '', line)
print(a)