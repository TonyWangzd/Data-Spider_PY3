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
# iecity_com.py
#
# Purpose : Scrape IECITY China POIs
#
# Ticket Link : https://bn-fbdb01.nuance.com/f/cases/75966
#
# Jeff Jia, for Nuance China Ltd., Chengdu, china
#
# Date Started: 2017-12-7
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
from scraperkitProxyMod import Proxy
from lmtoolkit import Logger
import logging
import http.cookiejar
import urllib.parse
import os
import sys
import re

socket.setdefaulttimeout(300)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt=' %Y/%m/%d %H:%M:%S')
LOG = logging.getLogger(__name__)
LEVEL1 = 1
LEVEL2 = 2
LEVEL3 = 3
LEVEL4 = 4
run_small = False

restaurants_hotels=["餐饮美食", "酒店住宿"]
fieldTypes = ["restaurants_hotels", "other"]

######################################################################

def preprocessHTML(inputHTML):
    try:
        inputHTML = inputHTML.decode('GB2312', 'ignore')
    except UnicodeDecodeError:
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

    if page_type == LEVEL1:
        content_div = getItemFromTags(soup, 'find', 'div', 'class', 'content')
        city_ul_list = getItemFromTags(content_div, 'findAll', 'ul', 'class', 'siteul clearfix')
        if city_ul_list:
            city_ul_list_select = city_ul_list[options.start:options.start + options.increase]
            for city_ul in city_ul_list_select:
                city_web_li_list = getItemFromTags(city_ul, 'findAll', 'li')
                if city_web_li_list and len(city_web_li_list) > 1:
                    city_web_li = city_web_li_list[0]
                    city_web_a = getItemFromTags(city_web_li, 'find', 'a')
                    if city_web_a:
                        city_name = textFromSoupObj(city_web_a)
                        city_url = city_web_a["href"] + "life/Cate--------------------1.html"
                        LOG.debug("Add City %s with url %s" % (city_name, city_url))
                        addUrl(city_url, payload=[LEVEL2, city_name])

    if page_type == LEVEL2:
        city_name = urlPayload[1]
        table = getItemFromTags(soup, 'find', 'table', 'class', 'table10')
        filter_list = getItemFromTags(table, 'find', 'ul', 'class', 'FilterList')
        if filter_list:
            category_list = getItemFromTags(filter_list, 'findAll', 'li')[1:]
            if category_list:
                for category_li in category_list:
                    category_a = getItemFromTags(category_li, 'find', 'a')
                    category_name = textFromSoupObj(category_a)
                    category_url_sub = category_a["href"]
                    category_url = replace_page_url(url, category_url_sub)
                    LOG.debug("Add Category %s for City %s with url %s" % (category_name, city_name, category_url))
                    addUrl(category_url, payload=[LEVEL3, city_name, category_name])
    if page_type == LEVEL3:
        city_entity_name = urlPayload[1]
        city_name = urlPayload[1][:-3]
        category_name = urlPayload[2]
        poi_ul = getItemFromTags(soup, 'find', 'ul', 'class', 'LifeList')
        if poi_ul:
            poi_li_list = getItemFromTags(poi_ul, 'findAll', 'li', 'class', 'clearfix')
            if poi_li_list:
                for poi_li in poi_li_list:
                    poi_detail = getItemFromTags(poi_li, 'find', 'div', 'class', 'detail')
                    if poi_detail:
                        parse_print_detail(poi_detail, poi_li, city_name, category_name, printToFile)
        if url.endswith("---1.html") and not run_small:
            poi_page_info_div = getItemFromTags(soup, 'find', 'div', 'style', 'padding:10px')
            if poi_page_info_div:
                poi_page_info = textFromSoupObj(poi_page_info_div)
                poi_page_info_list = re.findall("\d+", poi_page_info)
                if poi_page_info_list and len(poi_page_info_list) > 1:
                    poi_page_number = int(poi_page_info_list[1])
                    if poi_page_number > 1000:
                        poi_page_number = 1000
                    for page in range(2, poi_page_number):
                        url_temp = url.replace("-1.html", "-%s.html")
                        page_url = url_temp % page
                        LOG.debug("Add next page for Category %s for City %s with url %s" % (
                            category_name, city_name, page_url))
                        addUrl(page_url, payload=[LEVEL3, city_entity_name, category_name])


def replace_page_url(url, page_url):
    url_list = url.split('/')[:-1]
    url_list.append(page_url)
    return "/".join(url_list)


def parse_print_detail(poi_detail, poi_li, city_name, category_name, printToFile):
    district_name = ""
    title_name = ""
    sub_category_name = ""
    rate = ""
    address_name = ""

    rate_div = getItemFromTags(poi_detail, 'find', 'div', 'class', 'rate')
    if rate_div:
        image = getItemFromTags(rate_div, 'find', 'img')
        if image:
            image_src = image['src']
            rate_tail = image_src.split("/")[-1]
            rate_num = int(rate_tail.replace("star.png", "")[3:])
            if rate_num > 10:
                rate_num = rate_num / 10
            rate = str(rate_num * 20)
    title_h = getItemFromTags(poi_li, 'find', 'h2')
    if title_h:
        title_name = textFromSoupObj(title_h)
        title_name = data_clean(title_name)
    sub_category_div = getItemFromTags(poi_detail, 'find', 'div', 'class', 'type')
    if sub_category_div:
        sub_category_span = getItemFromTags(sub_category_div, 'find', 'span')
        sub_category_name = textFromSoupObj(sub_category_span)
    district_div = getItemFromTags(poi_detail, 'find', 'div', 'class', 'tel')
    if district_div:
        district_span = getItemFromTags(district_div, 'find', 'span')
        district_name = textFromSoupObj(district_span)
    address_div = getItemFromTags(poi_detail, 'find', 'div', 'class', 'address')
    if address_div:
        address_span = getItemFromTags(address_div, 'find', 'span')
        address_name = textFromSoupObj(address_span)

    record = '\t'.join((title_name, category_name, sub_category_name, rate, city_name, district_name, address_name))
    if category_name in restaurants_hotels:
        printToFile('restaurants_hotels', record)
    else:
        printToFile('other', record)



def data_clean(title):
    if "《" in title:
        title = title.replace("《", " ").replace("》", " ")
    if "【" in title:
        title = title.replace("【", " ").replace("】", " ")
    if "{" in title:
        title = title.replace("{", "").replace("}", "")
    return title


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

    parser.add_option('--delay', type='int', dest='delay', default=2,
                      help='specify delay in seconds between acessing web pages')

    parser.add_option('--debug', action='store_true', dest='debug',
                      default=False, help='print status messages to stdout')

    parser.add_option('--dateTag', '-d', dest='dateTag', default='',
                      help='Date used for path creation; defaults to current date')

    parser.add_option('--URL', '-u', dest='URL',
                      default='http://www.iecity.com',
                      help='input the URL to scrape; defaults to %default')
    parser.add_option('--small', action='store_true', dest='run_small',
                      default=False,
                      help='if run spider by small data set, this is for debug.')
    parser.add_option('--start', dest='start', default=0, type=int)
    parser.add_option('--increase', dest='increase', default=1, type=int)

    options, args = parser.parse_args()
    log = Logger(options.debug)

    proxy = Proxy(filterDict={'number_connectiontime': 100, 'ageInDays': 3, 'module': ['gatherproxy_com']},
                  autoCycling=5, splash=False, retrys=3)
    proxy.decorateScraperkit(lmscraperkit_v3_1)

    if options.run_small:
        run_small = options.run_small

    if options.URL:
        url = options.URL
        DOMAIN = urllib.parse.urlparse(url).netloc
        DOMAIN = DOMAIN.replace('www.', '')

    myScraper = WebScraper(
        scraperType='scrapers',
        topic='poi',
        lang='zho-CHN',
        name=DOMAIN,
        frequency='versions'
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
    # create named variables for the categories
    header_list = ['poi_name',
                   'category',
                   'subcategory',
                   'number_rate',
                   'address_city',
                   'address_region',
                   'address']
    HEADER = '#scraper01 ' + '\t'.join(header_list)

    outputPath = myScraper.generatePath(year=y, month=m, day=d, cleanState='records')

    for field in fieldTypes:
        sub_domain = "%s.%d-%d" % (field, options.start, options.start + options.increase)
        filename = os.path.join(
            outputPath,
            myScraper.generateFileName(subdomain=sub_domain, fileType='tsv')
        )
        myScraper.addOutputFile(field, filename, fileType='tsv', noTemp=True)

    # add the seed URL to the scraper
    myScraper.addUrl("http://www.iecity.com/about/sitemap.html", payload=[LEVEL1])
    # myScraper.addUrl("http://www.iecity.com/wuzhishan/life/Cate-539-------------------1.html", payload=[LEVEL3, '五指山', '餐饮美食'])

    # start the scraping job
    try:
        # it need use sort to uniqt the row, so the header will be add later
        if not options.restart:
            for field in fieldTypes:
                myScraper.printToFile(field, HEADER)
        log.info('Starting the scrape \n')
        myScraper.run(processPage,
                      restart=options.restart,
                      delay=options.delay,
                      urlOpener=opener,
                      HTMLpreprocessor=preprocessHTML,
                      badUrlsFile='/lm/data2/scrapers/zho-CHN/poi/iecity.com/log.inc/iecity.com.badUrls.lst'
                      )

        log.info('Finished the scrape \n')
    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)
