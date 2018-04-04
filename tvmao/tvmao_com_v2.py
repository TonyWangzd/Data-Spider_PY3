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
# tvmao_com_v2.py
#
# Purpose : Scraper TV station and program name for China
#
# Ticket Link : https://bn-fbdb01.nuance.com/f/cases/83841
#
# Jeff Jia, for Nuance China Ltd., Chengdu, china

# Date Started: 2017-12-01
#
# Modules:
#
# Revision History:
#
#####################################################################
#
# try: import lmtoolspath
# except ImportError: pass

from lmscraperkit_v3_1 import *
from lmtoolkit import Logger
# from FileUtils import *
import http.cookiejar
import urllib.parse
import requests
import os
import sys
import time

LEVEL1 = 1
LEVEL2 = 2
LEVEL3 = 3
LEVEL4 = 4
run_small = False

######################################################################

base_url = "http://www.tvmao.com"
clean_data_list = [
    "剧场",
    "精彩节目",
    "鳳凰氣象站",
    "电视剧",
]
######################################################################

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
    # time.sleep(30)
    if not soup:
        if urlPayload == 'topUrl':
            log.error('No soup for topUrl %s' % url)
            raise 'No soup for topUrl %s' % url
        else:
            log.warning('No soup found for {0}, skipping {1}'. \
                        format(urlPayload, url))

    if len(urlPayload) > 0:
        page_type = urlPayload[0]

    if page_type == LEVEL1:
        region_main_page_dd = getItemFromTags(soup, 'find', 'dd', 'class', 'bwlink')
        if region_main_page_dd:
            region_main_page_a = getItemFromTags(region_main_page_dd, 'find', 'a')
            region_name = textFromSoupObj(region_main_page_a)
            get_print_programe_detail(soup, printToFile, region_name)
            add_channels(addUrl, soup, region_name)

        main_category_dl = getItemFromTags(soup, 'find', 'dl', 'class', 'chntypetab clear')
        main_category_a_list = getItemFromTags(main_category_dl, 'findAll', 'a')
        if main_category_a_list:
            # don't need scrape the first two category
            scrape_category_list = main_category_a_list[2:-1]
            for scrape_category in scrape_category_list:
                category_url = scrape_category['href']
                category_name = textFromSoupObj(scrape_category)
                if category_url:
                    print("get category url %s" % base_url + category_url)
                    addUrl(base_url + category_url, payload=[LEVEL2, category_name])

        region_code_list = getItemFromTags(soup, 'findAll', 'option')
        if region_code_list:
            for region_code_option in region_code_list[1:]:
                region_code_str = region_code_option['value']
                if region_code_str and region_code_str.isdigit():
                    region_code = int(region_code_str)
                    if region_code != 0:
                        query_region_channel(region_code, addUrl)

    if page_type == LEVEL2:
        region_name = urlPayload[1]
        main_channel_div = getItemFromTags(soup, 'find', 'div', 'class', 'chlsnav')
        if main_channel_div:
            main_channel_a_links = getItemFromTags(main_channel_div, 'findAll', 'a')
            for main_channel in main_channel_a_links:
                main_channel_link = main_channel['href']
                if main_channel_link and not main_channel_link.endswith(".html"):
                    addUrl(base_url + main_channel_link, payload=[LEVEL3, region_name])

    if page_type == LEVEL3:
        region_name = urlPayload[1]
        add_channels(addUrl, soup, region_name)

    if page_type == LEVEL4:
        region_name = urlPayload[1]
        get_print_programe_detail(soup, printToFile, region_name)


def query_region_channel(region_code, addUrl):
    time.sleep(5)
    query_url = base_url + '/program/channels/'
    headers = {'content-type': 'application/x-www-form-urlencoded',
               'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:22.0)'
                             ' Gecko/20100101 Firefox/22.0'}

    params = {"prov": region_code}
    try:
        response = requests.post(query_url, headers=headers, data=params, timeout=20)
    except Exception as ex:
        print(ex)
        response = requests.Response()
        response.status_code = 504
    if response.status_code == 200:
        region_soup = BeautifulSoup(response.text)
        region_main_page_dd = getItemFromTags(region_soup, 'find', 'dd', 'class', 'bwlink')
        if region_main_page_dd:
            region_main_page_a = getItemFromTags(region_main_page_dd, 'find', 'a')
            if region_main_page_a:
                region_url = region_main_page_a['href']
                region_name = textFromSoupObj(region_main_page_a)
                if region_url:
                    print("get region url %s" % base_url + region_url)
                    addUrl(base_url + region_url, payload=[LEVEL2, region_name])


def add_channels(addUrl, soup, region_name):
    main_channel_div = getItemFromTags(soup, 'find', 'div', 'class', 'chlsnav')
    channel_ul = getItemFromTags(main_channel_div, 'find', 'ul', 'class', 'r')
    channel_a_list = getItemFromTags(channel_ul, 'findAll', 'a')
    if channel_a_list:
        for channel_a in channel_a_list:
            channel_url = channel_a['href']
            channel_title = textFromSoupObj(channel_a)
            addUrl(base_url + channel_url, payload=[LEVEL4, region_name])


def get_print_programe_detail(soup, printToFile, region_name):
    main_channel_div = getItemFromTags(soup, 'find', 'div', 'class', 'chlsnav')
    channel_category_div = getItemFromTags(main_channel_div, 'find', 'div', 'class', 'pbar')
    channel_category = textFromSoupObj(channel_category_div)
    channel_title_li = getItemFromTags(soup, 'find', 'li', 'class', 'curchn')
    channel_title = textFromSoupObj(channel_title_li)
    channel_program_div_list = getItemFromTags(soup, 'findAll', 'div', 'class', 'over_hide')
    if channel_program_div_list:
        for channel_program_div in channel_program_div_list:
            time_program_spans = getItemFromTags(channel_program_div, 'findAll', 'span')
            program_time = textFromSoupObj(time_program_spans[0])
            program_name = textFromSoupObj(time_program_spans[1])
            program_name_clean = clean_program_name(program_name)
            record = '\t'.join((region_name, channel_category, channel_title, program_time, program_name_clean))
            printToFile('', record)


def clean_program_name(program_name):
    if program_name in clean_data_list:
        return ""
    return program_name

#######################################################################

########################################################################
usage = """
 python2.6 %prog [--debug] [--dateTag] [--restart]
 [--robots] [--basepath]
 <<NOTE>> basepath and robots should be set for other than /lm/data2/
            for testing purpose
"""
########################################################################

if __name__ == '__main__':

    cookies = http.cookiejar.LWPCookieJar()
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
                      default='http://www.tvmao.com',
                      help='input the URL to scrape; defaults to %default')
    parser.add_option('--small', action='store_true', dest='run_small',
                      default=False,
                      help='if run spider by small data set, this is for debug.')

    options, args = parser.parse_args()
    log = Logger(options.debug)
    if options.run_small:
        run_small = options.run_small

    if options.URL:
        url = options.URL
        DOMAIN = urllib.parse.urlparse(url).netloc
        DOMAIN = DOMAIN.replace('www.', '')
    if options.html:
        myScraper = HTMLScraper(
            scraperType='scrapers',
            topic='epg',
            lang='zho-CHN',
            name=DOMAIN,
            frequency='inc'
        )
        myScraper.inputDataBall(options.html)
    else:
        myScraper = WebScraper(
            scraperType='scrapers',
            topic='epg',
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
    # create named variables for the categories
    header_list = [
                   'address_region',
                   'epg_station_category',
                   'epg_station',
                   'date_time',
                   'epg_program',
                   ]
    HEADER = '#scraper01 ' + '\t'.join(header_list) + '\n'

    # Use one of the three following sections for adding output files (I like #1)
    # 1 ****** Use this section for one or multiple files ********
    # This is a nice way to add multiple files to the scraper with a loop
    fieldTypes = ['']
    outputPath = myScraper.generatePath(year=y, month=m, day=d, cleanState='records')
    for field in fieldTypes:
        filename = os.path.join(
            outputPath,
            myScraper.generateFileName(subdomain=field, fileType='tsv')
        )
        myScraper.addOutputFile(field, filename, fileType='tsv')
    output_file = os.path.join(outputPath, myScraper.generateFileName(fileType='tsv'))
    # add the seed URL to the scraper

    myScraper.addUrl("http://www.tvmao.com/program/CCTV", payload=[LEVEL1])
    # start the scraping job
    try:
        # it need use sort to uniqt the row, so the header will be add later
        if not options.restart:
            myScraper.printToFile('', HEADER)
        log.info('Starting the scrape \n')
        myScraper.run(processPage,
                      restart=options.restart,
                      delay=options.delay,
                      urlOpener=opener,
                      badUrlsFile='/lm/data2/scrapers/zho-CHN/epg/tvmao.com/log.inc/twse.com.tw.badUrls.lst'  # <-- please provide the full path
                      )

        log.info('Finished the scrape \n')
    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)
