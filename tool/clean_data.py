import re

a1 = re.compile('\(.*\)')
regex1 = r"(?<=《)[^》]+(?=》)"

str_1 = "爱车天天汇(长春站)"
str_2 = "不是AB《不是最高档》"

text = re.sub(a1, '', str_1)
print(text)

deal_title = re.search(regex1, str_2)
final_title = deal_title.group()
print(final_title)
