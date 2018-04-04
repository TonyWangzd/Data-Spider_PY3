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
# myprog.py
#
# Purpose :
#
# Ticket Link  :
#
# Your Name Here , for Nuance Corporation, Burlington,MA
#
# Date Started:
#
# Modules:
#
# Revision History:
#
#####################################################################

try: import lmtoolspath
except ImportError: pass

from lmscraperkit_v03 import *
from lmtoolkit import Logger
import http.cookiejar
import urllib.request, urllib.error, urllib.parse

LEVEL1 = 1

######################################################################


def preprocessHTML(inputHTML):
    try:
        inputHTML = inputHTML.decode('GB2312', 'ignore')
    except UnicodeDecodeError:
        raise BadHTMLError
    else:
        return inputHTML.encode('utf8')

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
            log.error('No soup for topUrl %s' %url)
            raise 'No soup for topUrl %s' %url
        else:
            log.warning('No soup found for {0}, skipping {1}'.\
                       format(urlPayload, url))

    if len(urlPayload)>0:pageType = urlPayload[0]

    if pageType == LEVEL1:
        #Call this function to process table and print data
        processTable(soup, SPORTS, 'table', 'cellpadding', '0')

######################################################################

def processTable(soup, label, parent, attr, value):
    if parent in ['div', 'table']:
        tableObj = getItemFromTags(soup,'find', parent, attr, value)
        if not tableObj or not len(tableObj.findAll('tr')) > 1: return

        for trObj in tableObj.findAll('tr')[1:]:
            if len(trObj.findAll('td')) <3: continue
            values = [''] * 3
            tdObjs = trObj.findAll('td')
            for index in range(0, 3):
                if tdObjs[index].a:
                    values[index] = tdObjs[index].a.string
                else:
                    values[index] = textFromSoupObj(tdObjs[index])

            textToPrint = '\t'.join(values)
            myScraper.printToFile(label, textToPrint)

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
    opener.addheaders = [('User-agent' , 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)')]
    parser = OptionParser()
    parser.add_option('--basepath', '-b', dest='basepath',  default='/lm/data2/')

    parser.add_option('--encoding', '-e', dest='encoding', default='utf8',
                      help='encoding of input and output; defaults to %default')

    parser.add_option('--restart', '-r', default=False, action='store_true',
                     help='Restart the scraper from a previous incomplete run.')

    parser.add_option('--html', default='', help='HTML databall that will be used as input')

    parser.add_option('--robots', default='', help='robots.zip file' ) # Specify full path

    parser.add_option('--delay',type = 'int', dest='delay', default=2,
                     help='specify delay in seconds between acessing web pages')

    parser.add_option('--debug', action='store_true', dest='debug',
                     default=False, help='print status messages to stdout')

    parser.add_option('--dateTag', '-d', dest='dateTag', default='',
                     help='Date used for path creation; defaults to current date')

    parser.add_option('--URL', '-u', dest='URL', default='http://basket.co.il'
                      '/StatsPage_Individual.asp?c=1&sType=VAL&cYear=2016&'
                      'local=0&StatsBoard=0',
                      help='input the URL to scrape; defaults to %default')
    options, args = parser.parse_args()
    log = Logger(options.debug)
    if options.URL:
        url = options.URL
        DOMAIN = urllib.parse.urlparse(url).netloc
        DOMAIN = DOMAIN.replace('www.', '')
    if options.html:
        myScraper = HTMLScraper(
            scraperType = 'scrapers',
            topic       = 'news',
            lang        = 'dan-DNK',
            name        = DOMAIN,
            frequency   = 'versions'
        )
        myScraper.inputDataBall(options.html)
    else:
        myScraper = WebScraper(
            scraperType = 'scrapers',
            topic       = 'sports',
            lang        = 'heb-ISR',
            name        = DOMAIN,
            frequency   = 'inc'
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
    SPORTS = 'sports'
    HEADER = '#scraper01 number_rank\tname_athlete\tname_team'

    # Use one of the three following sections for adding output files (I like #1)
    #1 ****** Use this section for one or multiple files ********
    # This is a nice way to add multiple files to the scraper with a loop
    fieldTypes = [SPORTS]
    outputPath = myScraper.generatePath(year=y, month=m, day=d, cleanState='records')
    for field in fieldTypes:
        filename = os.path.join(
            outputPath,
            myScraper.generateFileName(subdomain=field, fileType='tsv')
        )
        myScraper.addOutputFile(field, filename, fileType='tsv', noTemp=False)
    # add the seed URL to the scraper
    myScraper.addUrl(str(options.URL), payload=[LEVEL1])
    # start the scraping job
    try:
        if not options.restart:
            myScraper.printToFile(SPORTS, HEADER)
        log.info('Starting the scrape \n')
        myScraper.run(processPage,
                      restart=options.restart,
                      delay=options.delay,
                      # siteEncoding='GB2312',
                      HTMLpreprocessor=preprocessHTML,
        #              badUrlsFile='../log.inc/doktoronline.no.badUrls.lst' # <-- please provide the full path
                    )
        log.info('Finished the scrape \n')
    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)
