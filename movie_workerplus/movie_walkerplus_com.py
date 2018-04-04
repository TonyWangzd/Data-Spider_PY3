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
# movie_walkerplus_com.py  
#
# Ticket : Request 85069: Fix JPN movie scraper
#
# see: https://bn-fbdb01.nuance.com/f/cases/85069
#
# Output: UTF-8; tsv file with the following format:  
#         rank \t movie \t director \t pronunciation \t actor \t pronunciation
#
# Frequency: weekly
#
# Zhaodong Wang, for Nuance Communications, Chengdu, China
#
# Date Written: 2018-03-27
#
# Revision History:
# v2 fix issue
#
#####################################################################

try:
    import py3paths
except ImportError:
    pass
import os
import sys
from collections import defaultdict
from optparse import OptionParser
from lmscraperkit_v3_1 import *
import traceback
from lmtoolkit import Logger
import re

from bs4 import BeautifulSoup as BS

from datetime import date
def processPage(soup, url, urlPayload, addUrl, addListOfUrls, printToFile):

    """
    Grab the text from the page as well as links to
    subsequent pages.
    
    Keyword arguments:
    soup        -- BeautifulSoup parsing of webpage
    url         -- URL of the webpage
    urlPayload  -- payload to carry information across webpage scrapes
    addUrl      -- function that adds to the list of URLs to scrape
    printToFile -- function that prints text to a file
    
    """
    baseUrl = extractDomain(url) 
    urlLevel = urlPayload[0]

    #__________________________________TOP LEVEL____________________
    if urlLevel == 'topUrl':
        for containerName in outputsList:
            printToFile(containerName,'#scraper01 number_rank\ttitle_movie\tname_director\tpronunciation\tname_actor\tpronunciation', noNewLine = True)

        for containerName in outputsList:
            containerObj = soup.find('div', {'class' : containerName})
            if containerObj and containerObj.table:
                table = containerObj.table
                trObjs = table.findAll('tr')
                for trObj in reversed(trObjs):
                    (rank, movie) = ('', '')
                    rankObj = trObj.find('span', {'class' : re.compile(r'ranking\d*')})
                    if rankObj:
                        rank = textFromSoupObj(rankObj).strip()
                    tdObj = trObj.td
                    if tdObj:
                        if tdObj.a:
                            movie = (textFromSoupObj(tdObj.a))
                            movie = ''.join(movie.split())
                            newUrl = tdObj.a['href']
                            newPayload = urlPayload[:]
                            newPayload[0] = 'movie'
                            newPayload.append(containerName)
                            newPayload.append(rank)
                            newPayload.append(movie)

                            sub_Url = baseUrl+newUrl
                            addUrl(sub_Url, payload = newPayload, checkForDuplicates = False, priority = True)
                        else:
                            movie = textFromSoupObj(tdObj).strip()

    elif urlLevel == 'movie':
        log.debug('urlLevel: {0}'.format(urlLevel))
        ( directors , directorsPronunc) = ([], [])
        (filename, rank, movie) = tuple(urlPayload[1:4])
        def findDirector(tag):
            if tag and tag.name == 'th' and tag.string == u'監督':
                return True
            return False

        directorObj = soup.find(findDirector)
        if directorObj:
            tdObj = directorObj.findNextSibling('td')
            if tdObj:
                directorString = str(tdObj)
                directorStrings = directorString.split('、')

                for string in directorStrings:
                    soupObj = BS(string)
                    directors.append(textFromSoupObj(soupObj).strip())
                    if soupObj.a and soupObj.a.has_key('title'):
                        directorsPronunc.append( soupObj.a['title'].strip())
                    else:
                        directorsPronunc.append('')


        tableObj = soup.find('table', {'id' : 'castTable'})
        if tableObj:
            trObjs = tableObj.findAll('tr')
            for (director, directorPronunc) in zip(directors, directorsPronunc):
                for trObj in trObjs:
                    (actor, actorPronunc) = ('', '')
                    tdObj= trObj.td
                    if tdObj:
                        actor = textFromSoupObj(tdObj).strip()
                        if tdObj.a and tdObj.a.has_key('title'):
                            actorPronunc = tdObj.a['title'].strip()
                        record = '\t'.join((rank, movie, director, directorPronunc, actor, actorPronunc))

                        printToFile(urlPayload[1],record)

    #__________________________________UNKNOWN LEVEL_________________
    else:
        log.debug('urlLevel: {0}'.format(urlLevel))
        assert( "Unknown Payload" == 0) 

    
# _________________________________SUBROUTINES_______________________

def staticCounter( static = {'counter' : 0}):
    static['counter'] +=1
    return static['counter']

########################################################################
usage = """
 python3 %prog [--debug] [--dateTag] [--restart]
 [--robots] [--basepath]
 <<NOTE>> basepath and robots should be set for other than /lm/data2/
            for testing  purpose
"""
########################################################################

if __name__ == '__main__':

    parser = OptionParser()

    parser.add_option('--basepath', '-b', dest='basepath',  default='/lm/data2/')

    parser.add_option('--encoding', '-e', dest='encoding', default='utf8',
                     help='encoding of input and output; defaults to %default')

    parser.add_option('--restart', '-r', default=False, action='store_true',
                     help='Restart the scraper from a previous incomplete run.')

    parser.add_option('--html', default=None,
                     help='HTML databall that will be used as input')

    parser.add_option('--robots',
                      dest='robots',
                      default='/lm/data2/scrapers/jpn-JPN/epg/movie.walkerplus.com/log.inc/robots.txt.zip',
                      help='robots.zip file [Default %default]'
                     )# Specify full path

    parser.add_option('--debug', action='store_true', dest='debug',
                     default=False, help='print status messages to stdout')

    parser.add_option( '--dateTag', '-d', dest='dateTag', default=None,
                     help='Date used for path creation; defaults to current date')

    parser.add_option('--delay', type='int', dest='delay', default=3,
                      help='specify delay in seconds between acessing web pages')
    # [OPTIONAL] limit output for debugging
    parser.add_option('--limitOutput', action='store_true', dest='limitOutput', default=False,
                  help='Limit output for debugging')

    # [REQUIRED] Output Directory
    parser.add_option('--outdir', action='store',
                      help='[REQUIRED] path to output directory')

    options, args = parser.parse_args()
    log = Logger(options.debug)

    myScraper = WebScraper(
        scraperType=u'scrapers',
        topic=u'movies',
        lang=u'jpn-JPN',
        name=u'movie.walkerplus.com',
        frequency=u'inc'
    )

    if options.robots:
        # set the robots.txt for the scraper
        myScraper.setRobotsTxt(url='http://movie.walkerplus.com/',zip=options.robots)

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
    JP = u'japanRanking'
    USA = u'usaRanking'
    outputsList = [JP, USA]

    for domain in outputsList:
        filename = os.path.join(
                 myScraper.generatePath(year=y, month=m, day=d, cleanState='records'),
                 myScraper.generateFileName(subdomain=domain, fileType='tsv')
        )

        # create the path and file name while adding it to the scraper
        myScraper.addOutputFile(
             domain,
             filename,
             noTemp=True,
             fileType='tsv'
        )

    #add the seed URL to the scraper

    myScraper.addUrl(u'http://movie.walkerplus.com/ranking/',payload=['topUrl'])

    # start the scraping job
    try:
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


