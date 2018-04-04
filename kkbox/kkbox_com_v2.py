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
# kkbox_com_v2.py
#
# Purpose : 85888 - Rewrite kkbox scraper
#
# Ticket Link : https://bn-fbdb01.nuance.com/f/cases/85888
#
# Jeff Jia, for Nuance China Ltd., Chengdu, china
#
# Date Started: 2018-03-01
#
# Modules:
#
# Revision History:
# 1. rewrite the kkobx scraper since the web site change
#
#####################################################################
try:
    import py3paths
except ImportError:
    pass

from lmwebapilib_v3 import RequestScraper
from lmscraperkit_v3_1 import *
from lmtoolkit import Logger
import logging
from datetime import date

LOG = logging.getLogger(__name__)
import json
import re

LEVEL1 = 1

SONG_CHART_TYPES = {
    "daily": ["song", "newrelease"],
    "weekly": ["song", "newrelease", "album"]
}

SONG_URL = {
    "daily": "https://kma.kkbox.com/charts/api/v1/daily",
    "weekly": "https://kma.kkbox.com/charts/api/v1/weekly"
}

SONG_CATEGORIES = {
    "chinese": 297,
    "western": 390,
    "japanese": 308,
    "korean": 314,
    "cantonese": 320,
    "taiwan": 304,
}

country_mapping = {
    "hk": u"xxx-HKG",
    "tw": u"xxx-TWN",
    "my": u"xxx-MYS",
    "sg": u"xxx-SGP"
}

songCleanRegex = re.compile('\(.*\)|-.*|\[.*\]|【.*】|《.*》')


#############################################################################

def processResponse(response, url, urlPayload, addUrl, printToFile):
    """
    Grab the text from the page as well as links to
    subsequent pages.

    Keyword arguments:
    soup        -- BeautifulSoup parsing of webpage
    url         -- URL of the webpage
    urlPayload  -- payload to carry information across webpage scrapes
    addUrl      -- function that adds to the list of URLs to scrape
    printToFile -- function that prints text to a file
stock
    """

    if not response or response.status_code != 200:
        log.warning('No Response found for {0}, skipping {1}'.format(urlPayload, url))

    if len(urlPayload) > 0:
        pageType = urlPayload[0]

    if pageType == LEVEL1:
        response_json = json.loads(response.text)
        sub_domain_label = urlPayload[1]
        song_type_name = urlPayload[2]
        song_list = list()
        data = response_json.get("data")
        if data:
            chart = data.get("charts")
            if chart:
                item_list = chart.get(song_type_name)
                if item_list:
                    for item in item_list:
                        item_type = item.get("type")
                        song_name = item.get("song_name")
                        artist_name = item.get("artist_name")
                        album_name = item.get("album_name")
                        release_date_timestamp = item.get("release_date")
                        release_date = ""
                        if release_date_timestamp:
                            release_date = date.fromtimestamp(release_date_timestamp).strftime("%Y-%m-%d")
                        song_list.append((item_type, song_name, album_name, artist_name, release_date))
        parse_print_song(printToFile, sub_domain_label, song_list)


def parse_print_song(printToFile, sub_domain_label, song_list):
    for index, (item_type, song_name, album_name, artist_name, release_date) in enumerate(song_list):
        title_album = songCleanRegex.sub('', album_name)

        if re.search('\((.*)\)', album_name):
            title_album_alternate = re.search('\((.*)\)', album_name).group(1)
        else:
            title_album_alternate = ''

        name_artist_list = list()
        name_artist_alternate_list = list()
        artist_name_list = artist_name.split(",")
        for artist_name_item in artist_name_list:
            name_artist = songCleanRegex.sub('', artist_name_item)
            if re.search('\((.*)\)', artist_name_item):
                name_artist_alternate = re.search('\((.*)\)', artist_name_item).group(1)
            else:
                name_artist_alternate = ''

            name_artist_list.append(name_artist.strip().rstrip())
            if name_artist_alternate:
                name_artist_alternate_list.append(name_artist_alternate.strip().rstrip())

        name_artist = ",".join(name_artist_list)
        name_artist_alternate = ','.join(name_artist_alternate_list)

        if item_type == "song":
            title_song = songCleanRegex.sub('', song_name)
            if re.search('\((.*)\)', song_name):
                title_song_alternate = re.search('\((.*)\)', song_name).group(1)
            else:
                title_song_alternate = ''
            record = '\t'.join((str(index + 1), title_song, title_song_alternate, title_album, title_album_alternate,
                                name_artist, name_artist_alternate, release_date))

        else:
            record = '\t'.join(
                (str(index + 1), title_album, title_album_alternate, name_artist, name_artist_alternate, release_date))
        printToFile(sub_domain_label, record)


def generate_query_parameters(category, terr, par_song_type):
    param = dict()
    param["category"] = category
    param["lang"] = "tc"
    param["limit"] = 50
    param["terr"] = terr
    param["type"] = par_song_type
    return param


###############################################################################

usage = """

 python3.3 %prog [--debug] [--dateTag] [--restart]
 [--robots] [--basepath]

 <<NOTE>> basepath and robots should be set for other than /lm/data2/

"""
################################################################################

if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option(
        '--basepath',
        '-b',
        dest='basepath',
        default='/lm/data2/')

    parser.add_option(
        '--restart',
        '-r',
        default=False,
        action='store_true',
        help='Restart the scraper from a previous incomplete run.'
    )

    parser.add_option(
        '--html',
        default=None,
        help='HTML databall that will be used as input'
    )

    parser.add_option(
        '--robots',
        default='/lm/data2/scrapers/zho-CHN/epg/tingban.cn/log.inc/'
                'robots.txt.zip',
        help='robots.zip file'
    )

    parser.add_option(
        '--delay',
        type='int',
        dest='delay',
        default=2,
        help='specify delay in seconds between acessing web pages'
    )

    parser.add_option(
        '--debug',
        action='store_true',
        dest='debug',
        default=False,
        help='print status messages to stdout'
    )

    parser.add_option(
        '--dateTag',
        '-d',
        dest='dateTag',
        default=None,
        help='Date used for path creation; defaults to current date'
    )

    parser.add_option(
        '--badUrlsFile',
        dest='badUrlsFile',
        default='/lm/data2/scrapers/zho-CHN/epg/tingban.cn'
                '/log.inc/tingban.cn.badUrls.lst',
        help='Prints unusable URLs to external file instead of halting the scraper.'
    )

    parser.add_option(
        '--small',
        action='store_true',
        dest='run_small',
        default=False,
        help='if run spider by small data set, this is for debug.'
    )

    parser.add_option(
        '--country_code',
        dest='country_code',
        default='',
        help='input the country code for this scraper, the options are tw, hk, my and sg'
    )

    options, args = parser.parse_args()
    log = Logger(options.debug)
    if options.run_small:
        run_small = options.run_small

    if options.country_code in country_mapping:
        lang_code = country_mapping[options.country_code]
        # add the seed URL to the scraper
        myScraper = RequestScraper(
            scraperType='web',
            topic='music',
            lang=lang_code,
            name='kkbox',
            frequency='inc'
        )

        # Set the base path ...
        # over ride the default of /lm/data2 with the --basepath option
        myScraper.setBasePath(options.basepath)

        # Use the date specified at the command line if provided
        if options.dateTag:
            y, m, d = options.dateTag.split(u'_')
        else:
            # otherwise default to current date
            y, m, d = yearMonthDay()

        song_header = ['number_rank',
                       'title_song',
                       'title_song_alternate',
                       'title_album',
                       'title_album_alternate',
                       'name_artist',
                       'name_artist_alternate',
                       'date_release']

        album_header = ['number_rank',
                        'title_album',
                        'title_album_alternate',
                        'name_artist',
                        'name_artist_alternate',
                        'date_release']

        outputPath = myScraper.generatePath(year=y, month=m, day=d, cleanState=u'records')
        for song_category in SONG_CATEGORIES:
            for song_chart_type in SONG_CHART_TYPES:
                song_type_list = SONG_CHART_TYPES.get(song_chart_type)
                for song_type in song_type_list:
                    sub_domain = '.'.join((song_category, song_chart_type, song_type))
                    filename = os.path.join(
                        outputPath,
                        myScraper.generateFileName(subdomain=sub_domain, fileType='tsv')
                    )
                    myScraper.addOutputFile(sub_domain, filename, fileType='tsv', noTemp=True)

                    if not options.restart:
                        if song_type == "album":
                            myScraper.printToFile(sub_domain, u'#scraper01 ' + u'\t'.join(album_header))
                        else:
                            myScraper.printToFile(sub_domain, u'#scraper01 ' + u'\t'.join(song_header))
                    params = generate_query_parameters(SONG_CATEGORIES[song_category], options.country_code, song_type)
                    url = "https://kma.kkbox.com/charts/api/v1/%s" % song_chart_type
                    # add the seed URL to the scraper
                    myScraper.addUrl(
                        url,
                        method='GET',
                        params=params,
                        payload=[LEVEL1, sub_domain, song_type]
                    )

        # start the scraping job
        try:
            log.info('Starting the scrape \n')

            myScraper.run(
                processResponse,
                restart=options.restart,
                badUrlsFile=options.badUrlsFile
            )
            log.info('Finished the scrape \n')
        except Exception as error:
            traceback.print_exc()
            log.error(error)
            if options.debug:
                raise
            sys.exit(2)
    else:
        LOG.error("must input a country code, the options are tw, hk, my and sg")
