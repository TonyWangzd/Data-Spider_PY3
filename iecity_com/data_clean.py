
format_lines = list()
with open("iecity.com.0-3.zho-CHN.UTF8.tsv") as data_file:
    lines = data_file.readlines()
    for line in lines:
        items = line.split('\t')
        title = items[0]
        if "《" in line:
            title = title.replace("《", " ").replace("》", " ")
        if "【" in line:
            title = title.replace("【", " ").replace("】", " ")
        if "{" in title:
            title = title.replace("{", "").replace("}", "")
        items[0] = title
        format_lines.append('\t'.join(items))

with  open("iecity.com.0-3.zho-CHN.UTF8.new.tsv", 'w') as write_data_file:
    write_data_file.writelines(format_lines)


b = [1,2,3]
c = b[2:4]