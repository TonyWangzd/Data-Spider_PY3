#!/usr/bin/python2.6
# -*- coding: utf-8 -*-

######################################################################
#                                                                    #
# Trade secret and confidential information of                       #
# Nuance Communications, Inc.                                        #
#                                                                    #
# Copyright (c) 2001-2016 Nuance Communications, Inc.                #
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
# gatherproxy_com.py
#
# Purpose : #65015 - Look for and scrape sources of proxy servers and ports
#
# Ticket Link  : http://bn-fbdb01.nuance.com/default.asp?65015
#
# A Meena , for Nuance Corporation, Bangalore, India
#
# Date Started: 2016-06-01
#
# Modules:
#
# Revision History:
#
#####################################################################
try: import lmtoolspathpy3
except ImportError: pass

import os, sys
from datetime import datetime,timedelta
import time, datetime
from optparse import OptionParser
from lmscraperkit_v3_1 import *
from lmtoolkit import Logger
from scraperkitProxyMod import GetProxiesBase
import http.cookiejar
import urllib.request, urllib.error, urllib.parse
import socket
socket.setdefaulttimeout(60)

proxy_re   = re.compile(r'"PROXY_IP":"(.*?)"')
port_re    = re.compile(r'"PROXY_PORT":"(.*?)"')
type_re    = re.compile(r'"PROXY_TYPE":"(.*?)"')
up_time_re = re.compile(r'"PROXY_UPTIMELD":"(.*?)"')


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
    if soup:
        # To process top url and to add movie links
        if urlPayload[0] == 'topUrl':
            ulObj = getItemFromTags(soup, 'find', 'ul', 'class', 'pc-list')
            if ulObj and ulObj.a:
                for aObj in ulObj.findAll('a'):
                    country_url = 'http://gatherproxy.com/' + aObj['href']
                    country     = aObj.string.split('(')[0]
                    country     = country.encode('utf-8')
                    addUrl(country_url, payload=['country', country])
        if urlPayload[0] == 'country':
            country = urlPayload[1]
            tableObj = getItemFromTags(soup, 'find', 'table', 'id', 'tblproxy')
            if tableObj:
                for scriptObj in tableObj.findAll('script'):
                    proxy, port, type, up_time = 4 *['']
                    text = scriptObj.string
                    if proxy_re.search(text):
                        proxy = proxy_re.search(text).group(1)
                    if port_re.search(text):
                        port = port_re.search(text).group(1)
                        # Convert the port num from hexadecimal to decimal
                        port = str(int(port, 16))
                    if up_time_re.search(text):
                        up_time = up_time_re.search(text).group(1)
                        [up, dn] = up_time.split('/')
                        up_time = int(float(float(up)/float(int(up) + int(dn))) * 100)
                    if type_re.search(text):
                        type = type_re.search(text).group(1)
                    if not type == 'Transparent' and up_time > 35:
                        textToPrint = '{0}\t{1}\t{2}\t{3}\t{4}'.format(proxy,
                                      port, country, type, up_time)
                        try:
                            printToFile('', textToPrint)
                        except: pass

    else:
        if urlPayload[0] == 'topUrl':
            log.error('No soup for topUrl %s' %url)
            raise 'No soup for topUrl %s' %url
        else:
            log.warning('No soup found for {0}, skipping {1}'.format(urlPayload, url))

###############################################################################
usage = '''
 python2.6 %prog [--debug] [--dateTag] [--restart] [--robots] [--basepath]

 <<NOTE>> basepath and robots should be set for other than /lm/data2/
 '''
###############################################################################

class GetProxies(GetProxiesBase):
    """A class providing a function to access the scraped ip data.

    This class is imported by scraperkitProxyMod.py

    """
    number_port = GetProxiesBase.Scalar('eq', 'float') + GetProxiesBase.List('eq', 'float')
    address_country =  GetProxiesBase.Scalar('eq', 'ignoreCase') + GetProxiesBase.List('eq', 'ignoreCase')
    number_connectiontime = GetProxiesBase.Scalar('ge', 'float', 'scaled')
    number_anonymity = GetProxiesBase.DiscreteToScaledScalar(
                        {'Anonymous' : 100, 'Elite' : 50},
                        'ge', 'float', 'scaled'
                        )
    fieldStructure = [
        'number_port',
        'address_country',
        'number_anonymity',
        'number_connectiontime'
    ]

    module = GetProxiesBase.ConstScalar(__module__) + GetProxiesBase.ConstList(__module__)
    #no actual meaning, but more transparent
    ageInDays = GetProxiesBase.Scalar('le', 'float')

    metaStructure = ['module', 'ageInDays']

    def __init__(self):
        path = WebScraper(**scraperKwargs).generatePath(year=1, month=2, day=3, cleanState='records')

        self.path = self.eliminateTrailingDate(path)

    def postProcessor(self,proxyList):
        return proxyList

#####################################################################################################

scraperKwargs = dict(
            scraperType='scrapers',
            topic='urls',
            lang='xxx-XXX',
            name='gatherproxy.com',
            frequency='inc'
)

if __name__ == '__main__':

    parser = OptionParser(usage=usage)
    cookies = http.cookiejar.LWPCookieJar()
    handlers = [
               urllib.request.HTTPHandler(),
               urllib.request.HTTPSHandler(),
               urllib.request.HTTPCookieProcessor(cookies)
               ]
    opener = urllib.request.build_opener(*handlers)
    opener.addheaders = [('User-agent' , 'Mozilla/5.0 (X11; U; Linux i686; ru; rv:1.9.0.8) Gecko/2009032711')]

    parser.add_option(
        '--dateTag',
        '-d',
        dest='dateTag',
        default=None,
        help='Date used for path creation; [Default %default]'
    )

    parser.add_option(
        '--restart',
        '--restart',
        default=False,
        action='store_true',
        help='Restart the scraper from a previous incomplete run.[Default %default]'
    )

    parser.add_option(
        '--debug',
        action='store_true',
        dest='debug',
        default=False,
        help='print status messages to stdout [Default %default]'
    )

    parser.add_option(
        '--robots',
        '--robots',
        dest='robots',
        default='/lm/data2/scrapers/eng-USA/epg/www.tv.com'
                '/log/robots.zip',
        help='Set the robots for robots.zip file [Default %default]'
    )
    parser.add_option(
        '--basepath',
        '-b',
        dest='basepath',
        default='/lm/data2/',
        help='Set the basepath for outputfile location [Default %default]'
    )

    options, args = parser.parse_args()

    log = Logger(options.debug)

    myScraper = WebScraper(
        scraperType = 'scrapers',
        topic       = 'urls',
        lang        = 'xxx-XXX',
        name        = 'gatherproxy.com',
        frequency   = 'inc',
    )
    if options.robots:
        # set the robots.txt for the scraper
        myScraper.setRobotsTxt(url='http://gatherproxy.com/',
                               zip=options.robots)

    myScraper.setBasePath(options.basepath)

    # Use the date specified at the command line if provided
    if options.dateTag:
        log.debug(options.dateTag)
        y, m, d = options.dateTag.split('_')
    else:
        # otherwise default to current date
        y, m, d = yearMonthDay()

    # if restarting scraper, set the rawDirectory
    if options.restart:
        myScraper.setRawDirectory(
            myScraper.generatePath(year=y, month=m, day=d, cleanState='raw')
        )
    now = datetime.datetime.now()
    fieldTypes = [
        '{0}:{1}'.format(str(now.hour).zfill(2), str(now.minute).zfill(2))
    ]
    outputPath = myScraper.generatePath(year=y, month=m, day=d,
                                        cleanState='records')

    HEADER = '#scraper01 number_ip\tnumber_port\taddress_country\tcode_anonymity\tnumber_connectiontime'
    for field in fieldTypes:
        myScraper.addOutputFile('', os.path.join(outputPath,
                                myScraper.generateFileName(subdomain=field, fileType='tsv',
                                )))
    if not options.restart:
        myScraper.printToFile('', HEADER)

    myScraper.addUrl('http://gatherproxy.com/proxylistbycountry', payload=['topUrl'])
    # start the scraper
    try:
        log.info('Starting the scrape \n')
        myScraper.run(processPage, restart=options.restart, urlOpener=opener, urlEncoding='utf-8')
        log.info('Finished the scrape \n')
    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)

