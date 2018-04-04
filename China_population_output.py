import xlrd
import re

xlsfile = r"/netshare/china_population.xls"

book = xlrd.open_workbook(xlsfile)

sheet_name = book.sheet_names()[0]
sheet1 = book.sheet_by_name(sheet_name)
nrows = sheet1.nrows

class node:
    def __init__(self,data):
        self.data = data
        self.children = []
        self.parent = 0
        self.number = 0
        self.population = 0

    def getdata(self):
        return self.data

    def getchildren(self):
        return self.children

    def add(self, node):
        self.children.append(node)
        node.parent = self

    def go(self, data):
        for child in self.children:
            if child.getdata() == data:
                return child

class tree:
    def __init__(self):
        self.head = node('china')

    def linktohead(self, node):
        self.head.add(node)

    def insert(self, path, data):
        cur = self.head
        for step in path:
            if cur.go(step) == None:
                return False
            else:
                cur = cur.go(step)
        cur.add(node(data))
        return True

    def search(self, path):
        cur = self.head
        for step in path:
            if cur.go(step) == None:
                return None
            else:
                cur = cur.go(step)
        return cur

    #def print_all(self):

def parse_provice():

    global cur_province
    global cur_city

    for i in range(3, nrows):
        read_once = 0
        print(sheet1.row_values(i))
        part = sheet1.cell_value(i,0)

        province_tag = ['自治区', '省']
        for value in province_tag:
            if sheet1.cell_value(i, 0).endswith(value):
                read_once = 1
                cur_province = node(sheet1.cell_value(i, 0))
                cur_province.population = node(sheet1.cell_value(i, 1))
                country.add(cur_province)
                country.number += 1
                break

        if read_once == 0:
            if sheet1.cell_value(i, 0).endswith('市'):
                cur_city = node(sheet1.cell_value(i, 0))
                cur_city.population = sheet1.cell_value(i, 1)
                for direct_city in ['北京市', '上海市', '天津市', '重庆市']:
                    if sheet1.cell_value(i,0) == direct_city:
                        country.add(cur_city)
                        country.number += 1
                        read_once = 1
                        break

                if read_once == 0:
                    cur_province.add(cur_city)
                    cur_province.number += 1

            else:
                cur_district = node(sheet1.cell_value(i,0))
                cur_district.population = sheet1.cell_value(i,1)
                cur_city.add(cur_district)
                cur_city.number += 1

###################

def find_children(node,position):
    if node.number > 0:
        position += node.data+'\t'
        for i in range(len(node.children)):
            node = node.children[i]
            find_children(node,position)
            node = node.parent

    else:
        record.append(position+node.data+'\t'+str(node.population)+'\n')


###################
def hasNumbers(inputString):
    return bool(re.search(r'\d', inputString))

def sort_city():
    for line in record:
        i = 0
        if hasNumbers(line):
            for city in ['\t北京市\t', '\t上海市\t', '\t天津市\t', '\t重庆市\t']:
                if city in line:
                    str_list = line.split(city, 1)
                    new_line = ''.join((str_list[0], '\t', city, str_list[1]))
                    data.append(new_line)
                    i = 1
                    break
            if i ==0:
                data.append(line)
        else:
            continue


if __name__ == "__main__":
    data = []
    record = []
    output_file = '/netshare/write_population2.tsv'
    tree = tree()
    country = node("中国")
    tree.linktohead(country)
    parse_provice()
    find_children(country,'')
    sort_city()
    with open(output_file, 'a') as f:
        for line in data:
            f.write(line)










