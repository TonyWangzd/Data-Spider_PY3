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
# radiotuner_jp.py
#
# Purpose : #85070-Scrape Japan radio stations
#
# Ticket Link  : https://bn-fbdb01.nuance.com/f/cases/85070
#
# Zhaodong Wang , for Nuance Corporation, Chengdu, China
#
# Date Started: 2018-03-16
#
# Modules:
#
# Revision History:
#
######################################################################
try:
    import py3paths
except ImportError:
    pass

from lmscraperkit_v3_1 import *
import traceback
from lmtoolkit import Logger
import re

LEVEL1 = 1
STATIONS = 2


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
    base = extractDomain(url)
    if len(urlPayload) > 0:
        pageType = urlPayload[0]
    # To process the topurl and collect the radio stations#
    if pageType == LEVEL1:
        trObjs = getItemFromTags(soup, 'findAll', 'tr', 'height', '12')
        if trObjs:
            for trObj in trObjs:
                aObjs = getItemFromTags(trObj, 'find', 'a', 'href')
                if aObjs:
                    url = aObjs.get("href")
                    addUrl(url, base, payload=[STATIONS])
    # To collect the details from each radio station#
    if pageType == STATIONS:
        stationObjs = getItemFromTags(soup, 'findAll', 'table', 'width', '510')
        if stationObjs:
            for stationObj in stationObjs:
                if stationObj.find(attrs={'class': 'midashi'}):
                    category, callsign, region, stationName1, stationName2, frequency = [''] * 6
                    trObjs1 = getItemFromTags(stationObj, 'findAll', 'tr')
                    h2Objs, callsignObjs, regionObjs, regionObjs1 = [''] * 4
                    if trObjs1 and len(trObjs1) > 3:
                        categoryObjs = getItemFromTags(trObjs1[0], 'find', 'td',
                                                       'colspan', '4')
                        if categoryObjs:
                            category = categoryObjs['background']
                            category = category[7:9].upper()
                        callsignWord = str(trObjs1[1]).find('コールサイン')
                        if callsignWord == -1:
                            callsignObjs = getItemFromTags(trObjs1[2], 'findAll', 'td')
                        else:
                            callsignObjs = getItemFromTags(trObjs1[1], 'findAll', 'td')
                        h2Objs = getItemFromTags(trObjs1[1], 'find', 'h2', 'class', 'midashi')

                        if getItemFromTags(trObjs1[2], 'find', 'td', 'class', 'location'):
                            regionObjs = getItemFromTags(trObjs1[2], 'find', 'td', 'class',
                                                         'location')
                        else:
                            regionObjs1 = getItemFromTags(trObjs1[2], 'find', 'td', 'width', '250')

                        frequencyObjs1 = getItemFromTags(trObjs1[3], 'find', 'td', 'colspan', '3')
                        if frequencyObjs1:
                            frequencyData = textFromSoupObj(frequencyObjs1)
                            frequencyList = re.findall(r"\d+\W?\d", frequencyData)
                            frequency = ','.join(frequencyList)

                    if len(trObjs1) == 3:
                        category = 'SW'
                        callsignObjs = getItemFromTags(trObjs1[0], 'findAll', 'td')
                        h2Objs = getItemFromTags(trObjs1[0], 'find', 'h2', 'class', 'midashi')
                        regionObjs1 = getItemFromTags(trObjs1[1], 'find', 'td', 'width', '250')
                    if callsignObjs and len(callsignObjs) > 1:
                        callsign = textFromSoupObj(callsignObjs[-1])
                        if len(callsign) > 1 and callsign[-1] == u'他':
                            callsign = callsign[0:-1]
                        else:
                            callsign = callsign
                    if h2Objs:
                        stationName = textFromSoupObj(h2Objs)
                        s = stationName.find('(')
                        if s == -1:
                            stationName1 = stationName
                        else:
                            stationName1 = stationName[0:s]
                            stationName2 = stationName[s + 1:-1]
                    if regionObjs1:
                        region = textFromSoupObj(regionObjs1)
                    else:
                        region = textFromSoupObj(regionObjs)

                    if stationName2:
                        record1 = '\t'.join(
                            (
                                category, stationName1, callsign, region, frequency))
                        record2 = '\t'.join(
                            (
                                category, stationName2, callsign, region, frequency))
                        printToFile(STATIONS, record1)
                        printToFile(STATIONS, record2)
                    else:

                        record = '\t'.join(
                            (
                                category, stationName1, callsign, region, frequency))
                        printToFile(STATIONS, record)


######################################################################
if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option('--basepath', '-b', dest='basepath', default='/lm/data2/')

    parser.add_option('--encoding', '-e', dest='encoding', default='utf8',
                      help='encoding of input and output; defaults to %default')

    parser.add_option('--restart', '-r', default=False, action='store_true',
                      help='Restart the scraper from a previous incomplete run.')

    parser.add_option('--html', default=None,
                      help='HTML databall that will be used as input')

    parser.add_option('--robots', default='', help='robots.zip file')  # Specify full path

    parser.add_option('--debug', action='store_true', dest='debug',
                      default=False, help='print status messages to stdout')

    parser.add_option('--dateTag', '-d', dest='dateTag', default=None,
                      help='Date used for path creation; defaults to current date')

    parser.add_option('--delay', type='int', dest='delay', default=10,
                      help='specify delay in seconds between acessing web pages')

    parser.add_option('--URL', '-u', dest='URL',
                      default='http://www.iecity.com',
                      help='input the URL to scrape; defaults to %default')

    options, args = parser.parse_args()
    log = Logger(options.debug)

    myScraper = WebScraper(
        scraperType=u'scrapers',
        topic=u'epg',
        lang=u'jpn-JPN',
        name=u'radiotuner.jp',
        frequency=u'inc'
    )
    if options.robots:
        # set the robots.txt for the scraper
        myScraper.setRobotsTxt(url='http://radiotuner.jp/', zip=options.robots)

    # Set the base path ... over ride the default of /lm/data2 with the --basepath option
    myScraper.setBasePath(options.basepath)

    # Use the date specified at the command line if provided
    if options.dateTag:
        y, m, d = options.dateTag.split(u'_')
    else:
        # otherwise default to current date
        y, m, d = yearMonthDay()

    # if restarting scraper, set the rawDirectory
    if options.restart:
        myScraper.setRawDirectory(
            myScraper.generatePath(year=y, month=m, day=d, cleanState='raw')
        )

    # create named variables for the categories
    STATIONS = u'STATIONS'

    filename = os.path.join(
        myScraper.generatePath(year=y, month=m, day=d, cleanState='records'),
        myScraper.generateFileName(subdomain=STATIONS, fileType='tsv')
    )

    # create the path and file name while adding it to the scraper
    myScraper.addOutputFile(
        STATIONS,
        filename,
        noTemp=True,
        fileType='tsv'
    )

    # add the seed URL to the scraper
    myScraper.addUrl(u'http://radiotuner.jp/fm_net_index.html', payload=[LEVEL1])
    myScraper.addUrl(u'http://radiotuner.jp/am_net_index.html', payload=[LEVEL1])
    myScraper.addUrl(u'http://radiotuner.jp/sw_index.html', payload=[STATIONS])
    # start the scraping job
    try:
        if not options.restart:
            header = u'#scraper01 category\tepg_station\tcode_callsign\taddress\tfrequency'
            myScraper.printToFile(STATIONS, header)

        myScraper.run(processPage,
                      delay=options.delay,
                      restart=options.restart,
                      badUrlsFile='/lm/data2/scrapers/jpn-JPN/epg/'
                                  'radiotuner/log.inc/radiotuner.jp.badUrls.lst'
                      )

    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)
