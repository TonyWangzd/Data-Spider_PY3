#!/usr/bin/python3.3
# -*- coding: utf-8 -*-

######################################################################
#                                                                    #
# Trade secret and confidential information of                       #
# Nuance Communications, Inc.                                        #
#                                                                    #
# Copyright (c) 2001-2018 Nuance Communications, Inc.                #
# All rights reserved.                                               #
#                                                                    #
# Copyright protection claimed includes all forms and matters of     #
# copyrightable material and information now allowed by statutory or #
# judicial law or hereinafter granted, including without limitation, #
# material generated from the software programs which are displayed  #
# on the screen such as icons, screen display looks, etc.            #
#                                                                    #
######################################################################

######################################################################
#
# news_qq_com.py
#
# Purpose : #83738-zho-CHN - Scrape top news from qq
#
# Ticket Link  : https://bn-fbdb01.nuance.com/f/cases/83738/Scrape-QQ
#
# Zhaodong Wang , for Nuance Corporation, Chengdu, China
#
# Date Started: 25-01-2018
#
# Modules:
#
# Revision History:
#
#####################################################################
try: import py3paths
except ImportError: pass

import time
import datetime
from optparse import OptionParser

import requests
from bs4 import BeautifulSoup
import os
import os.path
import json
import re

now = datetime.datetime.now()
file_name_list = []


def process_indexpage(url):
    html_data = requests.get(url).text
    # request url

    # html_data = urllib2.urlopen(url).read()

    soup = BeautifulSoup(html_data)
    # parse url with bs4
    item_list = [
        'item major',
        'item finance',
        'item ent'
    ]
    for item in item_list:
        news_titles = (soup.find(attrs={'class': item})).findAll(attrs={'class': 'Q-tpList'})
        initial_file_name = "news.qq.com." + item.split(" ")[1] + ".zho.CHN" + ".txt"
        # generate the path with date

        parse_blanklist(item,news_titles,initial_file_name)


def parse_blanklist(item, news_titles,initial_file_name):

    #generate file path
    path = generate_path(options.dest)
    if not os.path.exists(path):
        os.makedirs(path)
    file_name_list.append(path + initial_file_name)
    print("start scrape " + item)

    for titles in news_titles:
        try:
            blank = (titles.find(attrs={'class': 'f14 l24'})).find(attrs={'class': 'linkto'})
            title_name = blank.get_text()
            title_link = blank.get("href")
            title_content = parse_content(title_link)

            # write the content to the file

            if len(title_content):
                with open(path + initial_file_name, 'a', encoding="utf-8") as write_file:
                    write_file.write(title_name + "\n\n\n")
                    for line in title_content:
                        if len(line) > 4:
                            write_file.write(line)
                    write_file.write("\n\n")
        except Exception as ex:
            print(ex)
    print(item + "successfully scraped")

def process_tab(taburl_list):
    for taburl in taburl_list:
        taburl_data = requests.get(taburl).text
        soup = BeautifulSoup(taburl_data)
        news_titles = soup.findAll(attrs={'class': 'Q-tpList'})

        # generate the file name
        item_tab = taburl.split('_')[1]
        item = item_tab.split('tab')[0]
        initial_file_name = "news.qq.com." + item + ".zho.CHN" + ".txt"
        parse_blanklist(item,news_titles,initial_file_name)

def parse_content(link):
    try:
        data = requests.get(link)
        data.encoding = 'GBK'
        content_data = data.text
        content_data.encode('utf-8').decode('latin1')
        content = []
        soup = BeautifulSoup(content_data)
        values = link.split('/')[3]
        if values == "omn":
            paras = (soup.find(attrs={'class': 'content-article'})).findAll('p')
        if values == "a":
            paras = (soup.find(attrs={'id': 'Cnt-Main-Article-QQ'})).findAll('p')

        for para in paras:
            if not para.find('script'):
                para_content = para.get_text()
                if len(para_content) > 0:
                    content.append(para_content.strip().rstrip() + "\n")
        return filter_content(content)
    except Exception as ex:
        print('link has skip, regenerate url')
        return parse_json(link)


def parse_json(link):

    data = dict()
    data[u"id"] = (link.split('/')[4]).split('.')[0]+"00"
    data[u"child"] = "news_rss"
    data[u"refer"] = "mobilewwwqqcom"
    data[u"otype"] = "json"
    data[u"ext_data"] = "all"
    data[u"srcfrom"] = "newsapp"
    data[u"callback"] = "getNewsContentOnlyOutput"

    url = "http://openapi.inews.qq.com/getQQNewsNormalContent"
    page_response = requests.get(url, params= data)

    if page_response.status_code == 200:
        try:
            page_data = page_response.text.lstrip('getNewsContentOnlyOutput(').rstrip(')')
            page_json = json.loads(page_data)
        except Exception as e:
            print('error occured', e, 'when getting this page', url)
    else:
        print ("Can't get the page", url, "go to the next page")
        return

    content = []

    for line in page_json['content']:
        if line.get('type')==1:

            #remove tag
            dr = re.compile(r'</?\w+[^>]*>')
            content.append(dr.sub('',line.get('value')).strip()+'\n')

    return filter_content(content)

def filter_content(content):
    try:
        filter_pool = ['来源', '资料图', ' 摄', '编辑', '报记者', 'END', '图：','图片来源：']
        delete_line = []
        for line in content:
            content[content.index(line)] = re.sub('[■▲▼○△→●]', '', line)
            for word in filter_pool:
                if word in line:
                    delete_line.append(line)
                    break
            continue
        for item in delete_line:
            content.remove(item)

        # delete the repeated line
        news_content = list(set(content))
        news_content.sort(key=content.index)
        return news_content
    except Exception as e:
        print(e)

def generate_path(initial_path):
    return os.path.join(initial_path, year4FsMonth2FsDay2())

def year4FsMonth2FsDay2():
    return (time.strftime("%Y/%m/%d/", time.gmtime()))

#################################################
usage = '''
 python3.3 %prog [--debug]

 <<NOTE>>
'''
################################################
if __name__ == "__main__":

    parser = OptionParser(usage=usage)

    parser.add_option('--dest', dest='dest',
                      default='/lm/data2/scrapers/zho-CHN/news/news.qq.com/text-train.inc')
    parser.add_option('--debug', action='store_true', dest='debug', default=False,
                      help='print status messages to stdout')

    options, args = parser.parse_args()

    try:
        print("start scraping")
        news_url = 'http://news.qq.com'
        newstab_url_list = [
            'http://news.qq.com/ninja/timeline_autotab_newsindex.htm',
            'http://news.qq.com/ninja/timeline_techtab_newsindex.htm',
            'http://news.qq.com/ninja/timeline_housetab_newsindex.htm',
            'http://news.qq.com/ninja/timeline_edutab_newsindex.htm',
            'http://news.qq.com/ninja/timeline_digitab_newsindex.htm',
            'http://news.qq.com/ninja/timeline_fashiontab_newsindex.htm',
            'http://news.qq.com/ninja/timeline_astrotab_newsindex.htm',
            'http://news.qq.com/ninja/timeline_gamezonetab_newsindex.htm',
            'http://news.qq.com/ninja/timeline_cultab_newsindex.htm',
            'http://news.qq.com/ninja/timeline_societytab_newsindex.htm'
        ]

        process_indexpage(news_url)
        process_tab(newstab_url_list)
        for output_file_txt in file_name_list:
            command = 'gzip -f ' + output_file_txt
            print('command = ', command)
            os.system(command)

    except Exception as e:
        print(e)

    print('Program Complete')
