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
# freshdirect_com.py
#
# Purpose : online grocery store inventory
#
# Ticket Link : https://bn-fbdb01.nuance.com/f/cases/81463/Scrape-online-grocery-store-inventory
#
# Meena A, for Nuance India Pvt. Ltd., Bengaluru, India

# Date Started: 2017-09-20
#
# Modules:
#
# Revision History:
#
#####################################################################

try:
    import lmtoolspath
except ImportError:
    pass

from lmscraperkit_v3_1 import *
from lmtoolkit import Logger
import http.cookiejar
import urllib.request, urllib.error, urllib.parse

LEVEL1 = 1
LEVEL2 = 2
LEVEL3 = 3
BASE = 'https://www.freshdirect.com/'
SUB_CAT_URL_LATER_PART = 'true&activePage=0&sortBy=Sort_PopularityUp&orderAsc=true&activeTab=product'
PROCESSED_URLS = []


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

    if not soup:
        if urlPayload == 'topUrl':
            log.error('No soup for topUrl %s' % url)
            raise 'No soup for topUrl %s' % url
        else:
            log.warning('No soup found for {0}, skipping {1}'. \
                        format(urlPayload, url))

    if len(urlPayload) > 0: pageType = urlPayload[0]

    if pageType == LEVEL1:
        PROCESSED_URLS.append(url)
        # To process top URL and to add main categories
        span_objs = getItemFromTags(soup, 'findAll', 'span', 'class', 'top-item-link')
        if not span_objs: return
        for span_obj in span_objs:
            if span_obj.a:
                cat_url = BASE + span_obj.a['href']
                category = textFromSoupObj(span_obj.a)
                if not cat_url in PROCESSED_URLS:
                    PROCESSED_URLS.append(cat_url)
                    addUrl(cat_url, payload=[LEVEL2])
    if pageType == LEVEL2:
        PROCESSED_URLS.append(url)
        URLS = processCatPage(soup, url)
        if URLS:
            for sub_cat_url in URLS:
                addUrl(sub_cat_url, payload=[LEVEL2])
        else:
            cat_list = []
            ul_obj = getItemFromTags(soup, 'find', 'ul', 'class', 'breadcrumbs')
            if ul_obj and ul_obj.findAll('li'):
                for li in ul_obj.findAll('li'):
                    cat_list.append(textFromSoupObj(li))
            li_objs = getItemFromTags(soup, 'findAll', 'li', 'data-component', 'product')
            if not li_objs: return
            for li_obj in li_objs:
                name, name1, rating, desc, price = [''] * 5
                div_obj = li_obj.find('div', {'class': 'portrait-item-header'})
                if div_obj and div_obj.a and div_obj.a.b:
                    name = textFromSoupObj(div_obj.a.b)
                elif div_obj and div_obj.a and textFromSoupObj(div_obj.a):
                    name = textFromSoupObj(div_obj.a)
                span_obj = getItemFromTags(div_obj, 'find', 'div', 'class', 'product-name-no-brand')
                if span_obj:
                    name1 = (textFromSoupObj(span_obj))
                rating_div = getItemFromTags(div_obj, 'find', 'div', 'class', 'rating')
                if rating_div and textFromSoupObj(rating_div):
                    rating = textFromSoupObj(rating_div).strip()
                desc_div = getItemFromTags(div_obj, 'find', 'div', 'class', 'configDescr')
                if desc_div and textFromSoupObj(desc_div):
                    desc = textFromSoupObj(desc_div).strip()
                price_div = li_obj.find('div', {'class': 'portrait-item-price'})
                if price_div and textFromSoupObj(price_div):
                    price = textFromSoupObj(price_div).strip()
                    if price.startswith('$0.00'):
                        price = ''
                details = []
                for val in [name1, rating, desc, price]:
                    if val.strip():
                        details.append(val)
                print(cat_list)
                cat_list = cat_list + [''] * (4 - len(cat_list))
                print(cat_list)
                details = ', '.join(details)
                record = '\t'.join([name, details] + cat_list)
                printToFile('', record)


######################################################################
def processCatPage(soup, url):
    URLS = {}
    div_obj = ''
    div_objs = getItemFromTags(soup, 'findAll', 'div', 'class', 'menuBox')
    for obj in div_objs:
        if 'SUB_CATEGORY' in str(obj):
            div_obj = obj
            break
    if not div_obj and len(div_objs) > 0:
        div_obj = div_objs[0]
    if not div_obj: return
    li_objs = getItemFromTags(div_obj, 'findAll', 'li', 'data-component', 'menuitem')
    if li_objs:
        for li_obj in li_objs:
            item_id = ''
            sub_cat_url = ''
            if li_obj.has_attr('data-urlparameter'):
                item_id = li_obj['data-urlparameter']
                if item_id and not item_id == 'all':
                    if re.search(r'&id=.*?&pageSize=30&all=.*', url):
                        sub_cat_url = re.sub(r'&id=.*?&pageSize=30&all=.*',
                                             r'&id={0}&pageSize=30&all={1}'.format(item_id, SUB_CAT_URL_LATER_PART),
                                             url)
                    elif re.search(r'id=.*', url):
                        sub_cat_url = re.sub(r'id=.*',
                                             r'id={0}&pageSize=30&all={1}'.format(item_id, SUB_CAT_URL_LATER_PART), url)
                    if not sub_cat_url in PROCESSED_URLS:
                        URLS[sub_cat_url] = ''
                        PROCESSED_URLS.append(sub_cat_url)
                    else:
                        print('DUPLICATE:::::::::::::::', item_id)
    return URLS


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
                      default='https://www.freshdirect.com/srch.jsp?pageType=pres_picks&id=picks_love',
                      help='input the URL to scrape; defaults to %default')
    options, args = parser.parse_args()
    log = Logger(options.debug)
    if options.URL:
        url = options.URL
        DOMAIN = urllib.parse.urlparse(url).netloc
        DOMAIN = DOMAIN.replace('www.', '')
    if options.html:
        myScraper = HTMLScraper(
            scraperType='scrapers',
            topic='shopping',
            lang='eng-USA',
            name=DOMAIN,
            frequency='inc'
        )
        myScraper.inputDataBall(options.html)
    else:
        myScraper = WebScraper(
            scraperType='scrapers',
            topic='shopping',
            lang='eng-USA',
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
    HEADER = '#scraper01 name\tdescription\tcategory_level1\tcategory_level2\tcategory_level3\tcategory_level4'

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
    # add the seed URL to the scraper
    myScraper.addUrl('https://www.freshdirect.com//browse.jsp?id=dai&trk=gnav', payload=[LEVEL1])
    # start the scraping job
    try:
        if not options.restart:
            myScraper.printToFile('', HEADER)
        log.info('Starting the scrape \n')
        myScraper.run(processPage,
                      restart=options.restart,
                      delay=options.delay,
                      urlOpener=opener
                      #              badUrlsFile='../log.inc/doktoronline.no.badUrls.lst' # <-- please provide the full path
                      )
        log.info('Finished the scrape \n')
    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)
