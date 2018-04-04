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
# szsn_cn.py
#
# Purpose : scrape the stock number and stock name in szsn.cn
#
# Ticket Link : https://bn-fbdb01.nuance.com/f/cases/85708
#
# Zhaodong Wang, for Nuance China Ltd., Chengdu, china
#
# Date Started: 2018-4-2
#
# Modules:
#
# Revision History:
#
#####################################################################

try:
    import py3paths
except ImportError:
    pass
from optparse import OptionParser
from tempfile import mkstemp
from lmscraperkit_v3_1 import *
from lmtoolkit import Logger
import xlrd
import logging
import http.cookiejar
import urllib.parse
import os
import sys

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt=' %Y/%m/%d %H:%M:%S')
LOG = logging.getLogger(__name__)
LEVEL1 = 1
fh, TMPFILE = mkstemp()
os.close(fh)

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
    try:
        if soup:
            # To process top url and to print data
            if urlPayload[0] == LEVEL1:
                trObj = soup.find('tr', {'class':'cls-title-tr'})
                if trObj and trObj.a and '下载' in str(trObj.a):
                    link = 'http://www.szse.cn/' + trObj.a['href']
                    os.system('wget "{0}" -O {1}'.format(link, TMPFILE))
                    book = xlrd.open_workbook(TMPFILE)
                    sheet_name = book.sheet_names()[0]
                    sheet1 = book.sheet_by_name(sheet_name)
                    for i in range(1, sheet1.nrows):
                        code = sheet1.cell_value(i, 0)
                        name = sheet1.cell_value(i, 1)
                        record = u'\t'.join((code, name))
                        printToFile('', record)
        else:
            log.error('No soup for topUrl %s' %url)
            raise 'No soup for topUrl %s' %url

    except Exception as e:
        print(e)

########################################################################
usage = """
 python3 %prog [--debug] [--dateTag] [--restart]
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

    parser.add_option('--delay', type='int', dest='delay', default=5,
                      help='specify delay in seconds between acessing web pages')

    parser.add_option('--debug', action='store_true', dest='debug',
                      default=False, help='print status messages to stdout')

    parser.add_option('--dateTag', '-d', dest='dateTag', default='',
                      help='Date used for path creation; defaults to current date')

    parser.add_option('--URL', '-u', dest='URL',
                      default='http://www.szse.cn',
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
        scraperType = u'scrapers',
        topic       = u'finance',
        lang        = u'zho-CHN',
        name        = u'szse.cn',
        frequency   = u'inc',
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

    outputPath = myScraper.generatePath(year=y, month=m, day=d,
                                        cleanState=u'records')
    myScraper.addOutputFile('', os.path.join(outputPath,
                            myScraper.generateFileName(fileType='tsv',
                            )), noTemp=True)
    link = u'http://www.szse.cn/main/marketdata/hqcx/zsybg/'
    myScraper.addUrl(link, payload=[LEVEL1])

    # start the scraping job
    try:
        log.info('Starting the scrape\n')
        if not options.restart:
            myScraper.printToFile('', '#scraper01 code_stock\tproduct_stockindex')
        myScraper.run(processPage,
                      restart=options.restart,
                      urlOpener=opener,
                      badUrlsFile='/lm/data2/scrapers/zho-CHN/finance/szse.cn/log.inc/szse.badUrls.lst'
                      )
        log.info('Finished the scrape \n')

    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)
