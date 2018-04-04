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
# flights_ctrip_com.py
#
# Purpose : Collect hot airlines and airports in the world
#
# Ticket Link : https://bn-fbdb01.nuance.com/f/cases/85699
#
# Zhaodong Wang, for Nuance China Ltd., Chengdu, china
#
# Date Started: 2018-4-3
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
from lmscraperkit_v3_1 import *
from lmtoolkit import Logger
import http.cookiejar
import urllib.parse
import os
import sys
import socket
import chardet
socket.setdefaulttimeout(120)

LEVEL1 = 1

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
    if len(urlPayload)>0:pageType = urlPayload[0]
    if len(urlPayload)>1:label = urlPayload[1]
    if pageType == LEVEL1:
        ulObj = getItemFromTags(soup,'find','ul','class','schedule_detail_list clearfix')
        if ulObj:
            aObjs = getItemFromTags(ulObj,'findAll','a','target','_blank')
            if aObjs:
                for aObj in aObjs:
                    name = textFromSoupObj(aObj)
                    if name == u"ERO SUN D'OR航空":
                        code = u'2u'
                    else:
                        code = aObj['href'][-2:]

                    line_record = '\t'.join((name, code))
                    printToFile(AIRLINES, line_record)
            else:
                airportObjs = getItemFromTags(ulObj,'findAll','a')
                if airportObjs:
                    for airportObj in airportObjs:
                        nameAirport = textFromSoupObj(airportObj)
                        threeLetterCode = airportObj['href'][0:3]
                        port_record = '\t'.join((nameAirport, threeLetterCode))
                        printToFile(AIRPORTS, port_record)

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
                      default='http://flights.ctrip.com',
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
        scraperType=u'scrapers',
        topic=u'poi',
        lang=u'zho-CHN',
        name=u'flights.ctrip.com',
        frequency=u'inc'
    )
    if options.robots:
        # set the robots.txt for the scraper
        myScraper.setRobotsTxt(url=options.URL,
                               zip=options.robots)
        myScraper.setBasePath(options.basepath)
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

    AIRLINES = u'AIRLINES'
    AIRPORTS = u'AIRPORTS'

    # This is a nice way to add multiple files to the scraper with a loop
    #
    fieldTypes = [AIRLINES, AIRPORTS]

    outputPath = myScraper.generatePath(year=y, month=m, day=d, cleanState='records')
    for field in fieldTypes:
        filename = os.path.join(
            outputPath,
            myScraper.generateFileName(subdomain=field, fileType='tsv')
        )
        myScraper.addOutputFile(field, filename)

    # add the seed URL to the scraper

    myScraper.addUrl(u'http://flights.ctrip.com/international/hot-airlines.html',
                     payload=[LEVEL1, AIRLINES])
    myScraper.addUrl(u'http://flights.ctrip.com/international/hot-airports.html',
                     payload=[LEVEL1, AIRPORTS])

    # start the scraping job
    try:
        log.info('Starting the scrape \n')
        if not options.restart:
            myScraper.printToFile(AIRLINES,u'#scraper01 poi_company_airline\tcode_IATA')
            myScraper.printToFile(AIRPORTS,u'#scraper01 poi_airport\tcode_IATA')
            myScraper.run(processPage,
                          HTMLpreprocessor=preprocessGB,
		                  restart=options.restart,
                          badUrlsFile='/lm/data2/scrapers/zho-CHN/music/'
                                  'flights.ctrip.com/log.inc/flights.ctrip.com.badUrls.lst'
		                 )
        log.info('Finished the scrape \n')
    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)
