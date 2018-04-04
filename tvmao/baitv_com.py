#!/usr/bin/python3.3
# -*- coding: utf-8 -*-

######################################################################
#                                                                    #
# Trade secret and confidential information of                       #
# Nuance Communications, Inc.                                        #
#                                                                    #
# Copyright (c) 2001-2017 Nuance Communications, Inc.                #
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
# baitv_com.py
#
# Purpose : online grocery store inventory
#
# Ticket Link : https://bn-fbdb01.nuance.com/f/cases/82270
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
# try: import lmtoolspath
# except ImportError: pass
try: import py3paths
except ImportError: pass

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

base_url = "https://www.baitv.com"

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
            log.warning('No soup found for {0}, skipping {1}'.format(urlPayload, url))

    if len(urlPayload) > 0:
        page_type = urlPayload[0]

    if page_type == LEVEL1:
        region_main_page_ul = getItemFromTags(soup, 'find', 'ul', 'class', 'nav')
        first_main_channel_div = getItemFromTags(soup, 'find', 'div', 'class', 'channel-group open')
        if region_main_page_ul:
            # handel the contents on first page
            first_region_li = getItemFromTags(region_main_page_ul, 'find', 'li', 'class', 'active')
            first_region_name = textFromSoupObj(first_region_li)
            first_main_channel_a = getItemFromTags(first_main_channel_div, 'find', 'a')
            station_category_name = textFromSoupObj(first_main_channel_a)
            first_open_channel_div = getItemFromTags(first_main_channel_div, 'find', 'li', 'class', 'active')
            channel_name = textFromSoupObj(first_open_channel_div)

            add_channels(addUrl, soup, first_region_name, station_category_name)
            # get_print_program_detail(soup, printToFile, first_region_name, station_category_name, channel_name)
            if not run_small:
                # handel other nav bar
                region_main_page_a_list = getItemFromTags(region_main_page_ul, 'findAll', 'a')
                if region_main_page_a_list:
                    for region_main_page_a in region_main_page_a_list:
                        region_main_url = region_main_page_a['href']
                        region_name = textFromSoupObj(region_main_page_a)
                        if region_main_url and region_main_url != "#":
                            print("add main page %s for %s" % (region_main_url, region_name))
                            addUrl(base_url + region_main_url, payload=[LEVEL2, region_name])

    if page_type == LEVEL2:
        region_name = urlPayload[1]
        main_channel_div_list = getItemFromTags(soup, 'findAll', 'div', 'class', 'channel-group')
        if main_channel_div_list:
            for main_channel_div in main_channel_div_list:
                main_channel_a = getItemFromTags(main_channel_div, 'find', 'a')
                if main_channel_a:
                    station_category_url = main_channel_a['href']
                    station_category_name = textFromSoupObj(main_channel_a)
                    if station_category_url:
                        print("add station category page %s for %s for %s" % (
                        station_category_url, region_name, station_category_name))
                        addUrl(base_url + station_category_url, payload=[LEVEL3, region_name, station_category_name])

    if page_type == LEVEL3:
        region_name = urlPayload[1]
        station_category_name = urlPayload[2]
        add_channels(addUrl, soup, region_name, station_category_name)

    if page_type == LEVEL4:
        region_name = urlPayload[1]
        station_category_name = urlPayload[2]
        channel_name = urlPayload[3]
        get_print_program_detail(soup, printToFile, region_name, station_category_name, channel_name)


def add_channels(addUrl, soup, region_name, station_category_name):
    main_channel_div = getItemFromTags(soup, 'find', 'div', 'class', 'channel-group open')
    if main_channel_div:
        channel_li_list = getItemFromTags(main_channel_div, 'findAll', 'li')
        if channel_li_list:
            for channel_li in channel_li_list:
                channel_a = getItemFromTags(channel_li, 'find', 'a')
                if channel_a:
                    channel_url = channel_a['href']
                    channel_name = textFromSoupObj(channel_a)
                    print("add Channel page %s for %s for %s for %s" % (
                    channel_url, region_name, station_category_name, channel_name))
                    addUrl(base_url + channel_url, payload=[LEVEL4, region_name, station_category_name, channel_name])


a1 = re.compile('\(.*\)')
a2 = re.compile('-*\d{4}-\d+')


def get_print_program_detail(soup, printToFile, region_name, station_category_name, channel_name):
    schedule_ul_list = getItemFromTags(soup, 'findAll', 'ul', 'class', 'schedule-list')
    if schedule_ul_list:
        for schedule_ul in schedule_ul_list:
            schedule_li_list = getItemFromTags(schedule_ul, 'findAll', 'li')
            if schedule_li_list:
                for schedule_li in schedule_li_list:
                    schedule_div = getItemFromTags(schedule_li, 'find', 'div')
                    time_bock = getItemFromTags(schedule_div, 'find', 'time')
                    program_time = textFromSoupObj(time_bock)
                    program_name_with_time = textFromSoupObj(schedule_div)
                    program_name = program_name_with_time.replace(program_time, '').strip()
                    program_name_clean = clean_program_name(program_name)
                    record = '\t'.join(
                        (region_name, station_category_name, channel_name, program_time, program_name_clean))
                    record = re.sub(a1, '', record)
                    record = re.sub(a2, '', record)
                    print(record)
                    if record:
                        printToFile('', record)


def clean_program_name(program_name):
    if program_name in clean_data_list:
        return ""
    return program_name


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
                      default='https://www.baitv.com',
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
    header_list = ['address_region',
                   'epg_station_category',
                   'epg_station',
                   'date_time',
                   'epg_program']
    HEADER = '#scraper01 ' + '\t'.join(header_list)

    outputPath = myScraper.generatePath(year=y, month=m, day=d, cleanState='records')

    filename = os.path.join(
        outputPath,
        myScraper.generateFileName(fileType='tsv')
    )
    myScraper.addOutputFile('', filename, fileType='tsv')
    # add the seed URL to the scraper

    myScraper.addUrl("https://www.baitv.com/program/", payload=[LEVEL1])
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
                      badUrlsFile='/lm/data2/scrapers/zho-CHN/epg/movie.baitv.com/log.inc/baitv.com.badUrls.lst'
                      )

        log.info('Finished the scrape \n')
    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)
