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
# news_sina_com_cn.py
#
# Purpose : Collect the top news from Sina
#
# Ticket Link : https://bn-fbdb01.nuance.com/f/cases/83729/
#
# Zhaodong Wang, for Nuance China Ltd., Chengdu, china
#
# Date Started: 2018-3-16
#
# Modules:
#
# Revision History:
#
#####################################################################
#

try:
    import py3paths
except ImportError:
    pass
from lmscraperkit_v3_1 import *
from lmtoolkit import Logger
import logging
import http.cookiejar
import urllib.parse
import os
import sys
import requests
import json
import chardet
import time
import re

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt=' %Y/%m/%d %H:%M:%S')
LOG = logging.getLogger(__name__)
LEVEL1 = 1
LEVEL2 = 2


######################################################################
def preprocessGB(inputHTML):
    try:
        charset = chardet.detect(inputHTML)
        inputHTML = inputHTML.decode(charset['encoding'], 'ignore')
    except UnicodeDecodeError as e:
        LOG.info(e)
        raise BadHTMLError
    else:
        return inputHTML


def processPage(soup, url, urlPayload, addUrl, addListOfUrls, printToFile):
    """
    Grab the text from the page as well as links to
    subsequent pages.

    Keyword arguments:
    soup        -- BeautifulSoup parsing of webpage
    url         -- URL of the webpage
    urlPayload  -- payload to carry information across webpage scrapes
    addUrl      -- function that adds to the list of URLs to scrape
    addListOfUrls -- function that adds a list of URLs to the list
                        of URLS to scrape
    printToFile -- function that prints text to a file

    """
    if not soup:
        if urlPayload == 'topUrl':
            log.error('No soup for topUrl %s' % url)
            raise 'No soup for topUrl %s' % url
        else:
            log.warning('No soup found for {0}, skipping {1}'.format(urlPayload, url))

    if len(urlPayload) > 0:
        page_type = urlPayload[0]

    try:
        if page_type == LEVEL1:
            zt_list = [WORLD, CHINA, SOCIETY, COMMENT]
            feed = [CULTURE, SPORTS]
            if urlPayload[1] == WORLD:
                for sub_domain in zt_list:
                    parse_json_ztlist(sub_domain, addUrl)
                for sub_domain in feed:
                    parse_json_feed(sub_domain, addUrl)
            if urlPayload[1] == MILITARY:
                parse_html_military(soup, urlPayload[1], addUrl)
        if page_type == LEVEL2:
            title_name, article = parse_content(soup)
            record = title_name + '\n\n\n' + ''.join(article) + '\n'
            LOG.info("record one news" + title_name)
            printToFile(urlPayload[1], record)
    except Exception as ex:
        LOG.error(ex)


def parse_json_ztlist(subdomain, addurl):
    if subdomain == WORLD:
        cat_1_para = "gjxw"
        cat_2_para = None
    if subdomain == CHINA:
        cat_1_para = "gnxw"
        cat_2_para = "=gdxw1||=gatxw||=zs-pl||=mtjj"
    if subdomain == SOCIETY:
        cat_1_para = "shxw"
        cat_2_para = "=zqsk||=qwys||=shwx||=fz-shyf"
    if subdomain == COMMENT:
        cat_1_para = "pl_wap"
        cat_2_para = None

    for num in range(4):
        data = dict()
        data[u"channel"] = "news"
        data[u"cat_1"] = cat_1_para
        data[u"cat_2"] = cat_2_para
        data[u"cat_3"] = None
        data[u"show_ext"] = "1"
        data[u"show_all"] = "1"
        data[u"show_num"] = "22"
        data[u"tag"] = "1"
        data[u"format"] = "json"
        data[u"page"] = str(num)
        data[u"callback"] = "newsloadercallback"

        url = "http://api.roll.news.sina.com.cn/zt_list"
        page_response = requests.get(url, params=data)

        if page_response.status_code == 200:
            try:
                page_data = (page_response.text.lstrip(' newsloadercallback(')).rstrip(');')
                page_json = json.loads(page_data)
            except Exception as e:
                LOG.error('error occured', e, 'when getting this page', url)
        else:
            LOG.error("Can't get the page", url, "go to the next page")
            return

        if page_json['result']:
            page_result = page_json['result']
            for line in page_result['data']:
                if line.get('url'):
                    url_link = line.get('url')
                    datestring = url_link.split('/')[-2]
                    if currentdate_compare(datestring) and "video" not in url_link:
                        addurl(url_link, payload=[LEVEL2, subdomain])
                        LOG.info("add url" + url_link + "to queue")


def parse_json_feed(subdomain, addurl):
    if subdomain == CULTURE:
        pageid_para = "411"
        lid_para = "2595"

    if subdomain == SPORTS:
        pageid_para = "13"
        lid_para = "2503"

    for num in range(10):
        data = dict()
        data[u"pageid"] = pageid_para
        data[u"lid"] = lid_para
        data[u"num"] = "22"
        data[u"encode"] = "utf-8"
        data[u"page"] = str(num)

        url = "http://feed.mix.sina.com.cn/api/roll/get"
        page_response = requests.get(url, params=data)

        if page_response.status_code == 200:
            try:
                page_data = page_response.text
                page_json = json.loads(page_data)
            except Exception as e:
                LOG.error('error occured', e, 'when getting this page', url)

        else:
            LOG.error("Can't get the page", url, "go to the next page")
            return

        if page_json['result']:
            page_result = page_json['result']
            for line in page_result['data']:
                if line.get('url'):
                    url_link = line.get('url')
                    datestring = url_link.split('/')[-2]
                    if currentdate_compare(datestring) and "video" not in url_link:
                        addurl(url_link, payload=[LEVEL2, subdomain])
                        LOG.info("add url" + url_link + "to queue")


def currentdate_compare(date):
    return (time.strftime("%Y-%m-%d", time.gmtime())) == date


def parse_html_military(soup, subdomain, addUrl):
    liObj = (soup.find(attrs={'class': 'main'})).findAll('li')
    for news in liObj:
        url_link = (news.find('a')).get('href')
        datestring = url_link.split('/')[-2]
        if currentdate_compare(datestring) and "video" not in url_link:
            addUrl(url_link, payload=[LEVEL2, subdomain])
            LOG.info("add url" + url_link + "to queue")


def parse_content(soup):
    content = []
    title = soup.find(attrs={'class': 'main-title'}).get_text()
    paras = (soup.find(attrs={'class': 'article'})).findAll('p')

    for para in paras:
        if not para.find('script'):
            para_content = para.get_text()
            if len(para_content) > 0:
                content.append(para_content.strip().rstrip() + "\n")
    return title, filter_content(content)


def filter_content(content):
    try:
        if '(' in content[-1] and len(content[-1]) < 10:
            content.remove(content[-1])
        delete_line = []
        filter_pool = ['来源', '资料图', ' 摄', '编辑', '报记者', 'END', '图：', '图片来源：', '原标题', '（来自', '更多精彩']

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
        LOG.error(e)


#######################################################################

########################################################################
usage = """
 python3 %prog [--debug] [--dateTag] [--restart]
 [--robots] [--basepath]
 <<NOTE>> basepath and robots should be set for other than /lm/data2/
            for testing purpose
"""
########################################################################

if __name__ == '__main__':

    cookies = http.cookiejar.LWPCookieJar("douban_cookie.text")
    handlers = [
        urllib.request.HTTPHandler(),
        urllib.request.HTTPSHandler(),
        urllib.request.HTTPCookieProcessor(cookies)
    ]
    opener = urllib.request.build_opener(*handlers)
    opener.addheaders = [('User-agent', 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)')]

    parser = OptionParser()
    parser.add_option('--basepath', '-b', dest='basepath', default='/lm/data2/')

    parser.add_option('--encoding', '-e', dest='encoding', default='utf8',
                      help='encoding of input and output; defaults to %default')

    parser.add_option('--restart', '-r', default=False, action='store_true',
                      help='Restart the scraper from a previous incomplete run.')

    parser.add_option('--html', default='', help='HTML databall that will be used as input')

    parser.add_option('--robots', default='', help='robots.zip file')  # Specify full path

    parser.add_option('--delay', type='int', dest='delay', default=5,
                      help='specify delay in seconds between acessing web pages')

    parser.add_option('--debug', action='store_true', dest='debug',
                      default=False, help='print status messages to stdout')

    parser.add_option('--dateTag', '-d', dest='dateTag', default='',
                      help='Date used for path creation; defaults to current date')

    parser.add_option('--URL', '-u', dest='URL',
                      default='http://www.news.sina.com',
                      help='input the URL to scrape; defaults to %default')
    parser.add_option('--small', action='store_true', dest='run_small',
                      default=False,
                      help='if run spider by small data set, this is for debug.')
    parser.add_option('--start', dest='start', default=0, type=int)

    parser.add_option('--increase', dest='increase', default=1, type=int)

    options, args = parser.parse_args()
    log = Logger(options.debug)
    if options.run_small:
        run_small = options.run_small

    if options.URL:
        url = options.URL
        DOMAIN = urllib.parse.urlparse(url).netloc
        DOMAIN = DOMAIN.replace('www.', '')

    myScraper = WebScraper(
        scraperType='scrapers',
        topic='news',
        lang='zho-CHN',
        name=DOMAIN,
        frequency='inc'
    )
    if options.robots:
        # set the robots.txt for the scraper
        myScraper.setRobotsTxt(url=options.URL,
                               zip=options.robots)
    # Set the base path ... over ride the default of /lm/data2 with the --basepath option
    myScraper.setBasePath(options.basepath)
    # Use the date specified at the command line if provided
    if options.dateTag:
        y, m, d = options.dateTag.split('_')
    else:
        # otherwise default to current date
        y, m, d = yearMonthDay()
    # if restarting scraper, set the rawDirectory
    if options.restart:
        myScraper.setRawDirectory(
            myScraper.generatePath(year=y, month=m, day=d, cleanState='raw')
        )

    WORLD = u'world'
    CHINA = u'china'
    SOCIETY = u'society'
    COMMENT = u'comment'
    CULTURE = u'culture'
    SPORTS = u'sports'
    MILITARY = u'military'

    outputsList = [WORLD, CHINA, SOCIETY, COMMENT, CULTURE, SPORTS, MILITARY]

    for domain in outputsList:
        filename = os.path.join(
            myScraper.generatePath(year=y, month=m, day=d, cleanState='text-train'),
            myScraper.generateFileName(subdomain=domain, fileType='tsv')
        )

        # create the path and file name while adding it to the scraper
        myScraper.addOutputFile(
            domain,
            filename,
            noTemp=True,
            fileType='tsv'
        )

    # add the seed URL to the scraper
    # FOR ALL JSON CATAGORIES
    myScraper.addUrl("http://news.sina.com.cn/world/", payload=[LEVEL1, WORLD])
    myScraper.addUrl("http://roll.mil.news.sina.com.cn/col/gjjq/index.shtml", payload=[LEVEL1, MILITARY])
    myScraper.addUrl("http://roll.mil.news.sina.com.cn/col/zgjq/index.shtml", payload=[LEVEL1, MILITARY])

    # start the scraping job
    try:
        log.info('Starting the scrape \n')
        myScraper.run(processPage,
                      restart=options.restart,
                      delay=options.delay,
                      urlOpener=opener,
                      HTMLpreprocessor=preprocessGB,
                      badUrlsFile='/lm/data2/scrapers/zho-CHN/news/news.sina.com/log.inc/news.sina.com.badUrls.lst'
                      )

        log.info('Finished the scrape \n')

    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)
