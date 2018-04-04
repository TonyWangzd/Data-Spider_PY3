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
#
# try: import lmtoolspath
# except ImportError: pass

from lmscraperkit_v3_1 import *
from lmtoolkit import Logger
# from FileUtils import *
import http.cookiejar
import urllib.request, urllib.error, urllib.parse
import requests
import json
import os
import sys

LEVEL1 = 1
LEVEL2 = 2
run_small = False



######################################################################

categoryDict = {'director': '导演',
                'actor': '主演',
                'screenwriter': '编剧',
                'category': '类型:',
                'address_country_producing': '制片国家/地区:',
                'date_release_bycountry': '上映日期:',
                'date_time_runtime': '',
                'title_movie_bycountry': '又名:',
                'language': '语言:',
                'imdb': 'IMDb链接:'
                }


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

    if len(urlPayload) > 0:
        page_type = urlPayload[0]

    if page_type == LEVEL1:
        content_type = "movie"
        tag_list = get_tag_list(content_type)
        if tag_list:
            for tag in tag_list:
                get_movie_detail_add_to_queue(content_type, tag, addUrl, printToFile)
        content_type = "movie"
        tv_tag_list = get_tag_list(content_type)
        if tv_tag_list:
            for tv_tag in tv_tag_list:
                get_movie_detail_add_to_queue(content_type, tv_tag, addUrl, printToFile)
        # title = '怨灵2'
        # rate = '21'
        # movie_url = 'https://movie.douban.com/subject/26808505/'
        # addUrl(movie_url, payload=[LEVEL2, title, rate])

    if page_type == LEVEL2:
        title = urlPayload[1]
        rate = urlPayload[2]
        detail_info = process_detail_page(soup, title, url)
        detail_info.insert(13, url)
        detail_info.insert(12, rate)
        detail_info.insert(0, title)

        record = '\t'.join(detail_info)
        printToFile('', record)


def process_detail_page(soup, title, url):
    # init return value
    title_movie_original = ""
    url_image = ""
    date_year_release = ""
    date_release_bycountry_csv = ""
    category = ""
    date_time_runtime_bycountry_csv = ""
    address_country_producing = ""
    title_movie_bycountry_csv = ""
    language_csv = ""
    name_actor_csv = ""
    name_director_csv = ""
    name_screenwriter_csv = ""
    number_raters = ""
    url_imdb = ""
    description = ""

    try:
        content_div = getItemFromTags(soup, 'find', 'div', 'id', 'content')

        topic_div = getItemFromTags(content_div, 'find', 'h1')
        title_span = getItemFromTags(topic_div, 'find', 'span', 'property', 'v:itemreviewed')
        title_all = textFromSoupObj(title_span).strip()
        year_release_span = getItemFromTags(topic_div, 'find', 'span', 'class', 'year')
        year_release = textFromSoupObj(year_release_span).strip()

        image_div = getItemFromTags(content_div, 'find', 'div', 'id', 'mainpic')
        image_img = getItemFromTags(image_div, 'find', 'img')

        info_div = getItemFromTags(content_div, 'find', 'div', 'id', 'info')

        director_a_list = getItemFromTags(info_div, 'findAll', 'a', 'rel', 'v:directedBy')
        print([textFromSoupObj(director_a) for director_a in director_a_list])

        actor_a_list = getItemFromTags(info_div, 'findAll', 'a', 'rel', 'v:starring')

        info_spans = getItemFromTags(info_div, 'findAll', 'span', 'class', 'pl')

        screenwriter_a_list = None
        producing_country_str = None
        language_str = None
        alias_country_str = None

        for info_span in info_spans:
            if textFromSoupObj(info_span).startswith(categoryDict['screenwriter']):
                screenwriter_span = info_span.next_sibling
                screenwriter_a_list = getItemFromTags(screenwriter_span.parent, 'findAll', 'a')
                print([textFromSoupObj(screenwriter_a) for screenwriter_a in screenwriter_a_list])
            if textFromSoupObj(info_span).startswith(categoryDict['address_country_producing']):
                producing_country_str = info_span.next_sibling
                print(producing_country_str)
            if textFromSoupObj(info_span).startswith(categoryDict['language']):
                language_str = info_span.next_sibling
                print(language_str)
            if textFromSoupObj(info_span).startswith(categoryDict['title_movie_bycountry']):
                alias_country_str = info_span.next_sibling
                print(alias_country_str)

        category_span_list = getItemFromTags(info_div, 'findAll', 'span', 'property', 'v:genre')
        print([textFromSoupObj(category_span) for category_span in category_span_list])

        release_date_span_list = getItemFromTags(info_div, 'findAll', 'span', 'property', 'v:initialReleaseDate')
        print([textFromSoupObj(release_date_span) for release_date_span in release_date_span_list])

        run_time_span_list = getItemFromTags(info_div, 'findAll', 'span', 'property', 'v:runtime')
        print([textFromSoupObj(run_time_span) for run_time_span in run_time_span_list])

        imdb_a = getItemFromTags(info_div, 'find', 'a', 'rel', 'nofollow')
        print(imdb_a)

        rating_div = getItemFromTags(content_div, 'find', 'div', 'class', 'rating_sum')
        rating_people_a = getItemFromTags(rating_div, 'find', 'span', 'property', 'v:votes')

        relate_info_div = getItemFromTags(content_div, 'find', 'div', 'class', 'related-info')
        description_span = getItemFromTags(relate_info_div, 'find', 'span', 'property', 'v:summary')

        if title_all:
            title_movie_original = title_all.replace(title, "").strip()
            if not title_movie_original:
                title_movie_original = title_all
        if image_img:
            url_image = image_img['src']
        if year_release:
            date_year_release = year_release[1:-1]
        if release_date_span_list:
            date_release_bycountry_csv = ','.join(
                [textFromSoupObj(release_date_span) for release_date_span in release_date_span_list])
        if category_span_list:
            category = ','.join([textFromSoupObj(category_span) for category_span in category_span_list])
        if run_time_span_list:
            date_time_runtime_bycountry_csv = ','.join(
                [textFromSoupObj(run_time_span) for run_time_span in run_time_span_list])
        if producing_country_str:
            address_country_producing = ','.join(
                [producing_country.strip() for producing_country in producing_country_str.split("/")])
        if alias_country_str:
            title_movie_bycountry_csv = ','.join([alias_country.strip() for alias_country in alias_country_str.split("/")])
        if language_str:
            language_csv = ','.join([language.strip() for language in language_str.split("/")])
        if actor_a_list:
            name_actor_csv = ','.join([textFromSoupObj(actor_a) for actor_a in actor_a_list])
        if director_a_list:
            name_director_csv = ','.join([textFromSoupObj(director_a) for director_a in director_a_list])
        if screenwriter_a_list:
            name_screenwriter_csv = ','.join([textFromSoupObj(screenwriter_a) for screenwriter_a in screenwriter_a_list])
        if rating_people_a:
            number_raters = textFromSoupObj(rating_people_a)
        if imdb_a:
            url_imdb = imdb_a.get('href')
        if description_span:
            description = textFromSoupObj(description_span)
    except Exception as ex:
        print('parse error for %s url' % url)

    return [title_movie_original,
            url_image,
            date_year_release,
            date_release_bycountry_csv,
            category,
            date_time_runtime_bycountry_csv,
            address_country_producing,
            title_movie_bycountry_csv,
            language_csv,
            name_actor_csv,
            name_director_csv,
            name_screenwriter_csv,
            number_raters,
            url_imdb,
            description]


def query_data_by_api(query_url, params):
    # headers = {'content-type': 'application/json',
    #            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:22.0) '
    #                          'Gecko/20100101 Firefox/22.0'}
    response = requests.get(query_url, params=params, timeout=20)
    if response.status_code == 200:
        return response
    else:
        print("fail to query the data by %s with error code % and error info %s" % (
            query_url, response.status_code, response.text))


def get_tag_list(content_type="movie"):
    query_url = u"https://movie.douban.com/j/search_tags"
    params = {"type": content_type}
    print("get detail data from: %s with parameter %s" % (query_url, params))
    response = query_data_by_api(query_url, params)
    tag_list = list()
    if response and response.text:
        tag_json = json.loads(response.text)
        tags = tag_json.get('tags')
        if tags:
            for tag in tags:
                tag_list.append(tag)
    return tag_list


def get_movie_detail_add_to_queue(content_type, tag, addUrl, printToFile):
    """
    https://movie.douban.com/explore#!type=movie&tag=%E7%83%AD%E9%97%A8&sort=time&page_limit=20&page_start=0
    :param tag:
    :return:
    """
    query_url = u"https://movie.douban.com/j/search_subjects"
    has_reply_movie = True
    page_step = 20
    page_number = 0
    movie_list = list()
    while has_reply_movie:
        # for small run only query on page for each tag
        if run_small:
            has_reply_movie = False
        page_start = page_number * page_step
        movie_list.clear()
        params = {"type": content_type,
                  "tag": tag,
                  "sort": "time",
                  "page_limit": 20,
                  "page_start": page_start}
        response = query_data_by_api(query_url, params)
        if response and response.text:
            subjects_json = json.loads(response.text)
            subjects = subjects_json.get('subjects')
            if subjects:
                for subject in subjects:
                    movie_url = subject.get('url')
                    if movie_url:
                        movie = dict(subject)
                        movie_list.append(movie)
        if len(movie_list) > 0:
            print("add %s detail page to parse detail" % len(movie_list))
            for movie in movie_list:
                movie_url = movie.get('url')
                title = movie.get('title')
                rate = movie.get('rate')

                record = '\t'.join((movie_url, title, rate))
                printToFile('', record)
                # addUrl(movie_url, payload=[LEVEL2, title, rate])
            page_number += 1
        else:
            has_reply_movie = False

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
                      default='https://movie.douban.com',
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
            topic='movies',
            lang='zho-CHN',
            name=DOMAIN,
            frequency='inc'
        )
        myScraper.inputDataBall(options.html)
    else:
        myScraper = WebScraper(
            scraperType='scrapers',
            topic='movies',
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
    header_list = ['#scraper01',
                   'title_movie',
                   'title_movie_original',
                   'URL_image',
                   'date_year_release',
                   'date_release_bycountry_csv',
                   'category',
                   'date_time_runtime_bycountry_csv',
                   'address_country_producing',
                   'title_movie_bycountry_csv',
                   'language_csv',
                   'name_actor_csv',
                   'name_director_csv',
                   'name_screenwriter_csv',
                   'number_rating',
                   'number_raters',
                   'URL_homepage',
                   'URL_imdb',
                   'description']
    HEADER = '\t'.join(header_list) + '\n'

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
    myScraper.addUrl(str(options.URL), payload=[LEVEL1])
    # start the scraping job
    try:
        # it need use sort to uniqt the row, so the header will be add later
        # if not options.restart:
        #     myScraper.printToFile('', HEADER)
        log.info('Starting the scrape \n')
        myScraper.run(processPage,
                      restart=options.restart,
                      delay=options.delay,
                      urlOpener=opener,
                      # badUrlsFile='/lm/data2/scrapers/zho-CHN/movies/movie.douban.com/log.inc/twse.com.tw.badUrls.lst'  # <-- please provide the full path
                      )

        output_file_tsv = output_file[:-3]
        command = 'gunzip -f ' + output_file
        print('command = ', command)
        os.system(command)

        with open(output_file_tsv, encoding='utf-8') as open_file:
            lines = open_file.readlines()

        line_set = set(lines)

        with open(output_file_tsv, 'w', encoding='utf-8') as open_file:
            open_file.write(HEADER)
            open_file.writelines(line_set)

        command = 'gzip -f ' + output_file_tsv
        print('command = ', command)
        os.system(command)

        # output_file_tsv = output_file[:-3]
        # command = 'gunzip -f ' + output_file
        # print('command = ', command)
        # os.system(command)
        #
        # command = 'sort -nr ' + output_file_tsv + ' > temp.tsv'
        # print('command = ', command)
        # os.system(command)
        #
        # # Write header to oname
        # with open(output_file_tsv, 'w') as out_file:
        #     out_file.write(HEADER)
        #
        # # Append data to oname
        # command = 'cat temp.tsv >> ' + output_file_tsv
        # print('command = ', command)
        # os.system(command)
        #
        # # Pack oname
        # command = 'gzip -f ' + output_file_tsv
        # print('command = ', command)
        # os.system(command)

        log.info('Finished the scrape \n')
    except Exception as error:
        traceback.print_exc()
        log.error(error)
        if options.debug: raise
        sys.exit(2)
