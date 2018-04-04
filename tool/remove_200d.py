
with open("movie.douban.com.list.zho-CHN.UTF8.tsv") as file_reader:
    lines = file_reader.readlines()

for line in lines:
    if '\u200d' in line:
        line.replace('\u200d', '')
        print(line)
