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
# lmwebapilib_v3.py
#
# Purpose : Repository for Scraper data from web api
#
# Jeff Jia, for Nuance Corporation, Chengdu,China
#
# Date Started: 2018-03-01
#
# Modules:
#
# Revision History:
# Jeff: rewrite it to py3 and base on py3 lmscraperkit
#
#####################################################################
from time import sleep

import sys

import json
import requests
import logging

from lmscraperkit_v3_1 import *
from lmscraperkit_v3_1 import _OPEN, _CLOSED
# from FileUtils import *

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt=' %Y/%m/%d %H:%M:%S')

LOG = logging.getLogger(__name__)

DEFAULT_USER_AGENT = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'


class BaseClient(object):
    def __init__(self, header_name=None, header_value=None, timeout=30):
        self._default_headers = {header_name: header_value} if header_name else {}
        self._timeout = timeout

    def set_default_header(self, header_name, header_value):
        self._default_headers[header_name] = header_value

    def call_api(self, url, method, headers={}, params=None, body=None, delay=10,
                 attempts=1):
        """
        use this requests to perform the REST API call
        :param attempts:
        :param delay:
        :param params:
        :param headers:
        :param url:
        :param method:
        :param body:
        :return:
        """
        LOG.debug("Call Web API" + format(method) + " " + format(url) + ' ' + format(body))

        headers.update(self._default_headers)

        timeout = self._timeout
        b_failed = True

        for i in range(attempts):
            sleep(delay)
            try:
                response = None
                if method in ['GET']:
                    response = requests.request(method, url, headers=headers, params=params, timeout=timeout)
                elif method in ['POST']:
                    response = requests.request(method, url, headers=headers, json=body, timeout=timeout)
                else:
                    LOG.error("Unrecognized HTTP Verb.")
            except Exception as err:
                LOG.error("error during call Web API api:{}".format(err))
                b_failed = True
            else:
                if response.status_code == 200:
                    return response
                else:
                    LOG.error("error during call Web API api:{}".format(url))
                    b_failed = True
        if b_failed:
            LOG.error("ERROR: Could not get page after trying %s times." % attempts)
            raise Exception("ERROR: Could not get page after trying %s times." % attempts)


class ValidationError(Exception):
    """
    Error to be raised when a validation test fails
    """

    def __init__(self, message):
        self.value = message

    def __str__(self):
        return self.value


class RequestScraper(object):
    def __init__(self, **kwargs):
        """
        Create a Scraper object
        """
        required_params = [
            'scraperType',
            'topic',
            'name',
            'lang',
            'frequency'
        ]
        for param in required_params:
            if param not in kwargs:
                LOG.error('"{0}" is a required parameter; please specify in scraper initialization'.format(param[1]))
                sys.exit(1)

        self.scraperType = kwargs['scraperType']
        self.topic = kwargs['topic']
        self.name = kwargs['name']
        self.lang = kwargs['lang']
        self.frequency = kwargs['frequency']
        try:
            self.langFirst = kwargs['langFirst']
        except KeyError:
            self.langFirst = False

        self.outputFiles = {}  # output file names and their paths
        self.basePath = '/lm/data2/'  # path onto which filepaths will be appended
        self.rawFileSubDomain = None
        self.rawDir = None

        # validate the inputs provided by the user
        errors = []
        if self.topic not in validTopics and self.topic.split('-')[0] not in validTopics:
            errors.append('ERROR: "{0}" is not a valid "topic" parameter; please select '
                          'from the following list:\n\t{1}'.format(self.topic, '\n\t'.join(validTopics)))
        if self.scraperType not in validScraperTypes:
            errors.append('ERROR: "{0}" is not a valid scraper type parameter; please select '
                          'from the following list:\n\t{1}'.format(self.scraperType, '\n\t'.join(validScraperTypes)))
        if not validLangID(self.lang):
            errors.append('ERROR: "{0}" is not a valid language code; code must be of xxx-YYY\
                format, where xxx is the language and YYY is the region/country'.format(self.lang))
        if self.frequency not in validFrequencies:
            errors.append('ERROR: "{0}" is not a valid frequency; please select a frequency from '
                          'the following list:\n\t{1}'.format(self.frequency, '\n\t'.join(validFrequencies)))

        if len(errors) > 0:
            raise ValidationError(
                'Found the following errors while validating input parameters:\n\n{0}'.format('\n'.join(errors)))
        else:
            # validation checks passed, continue initializing
            self.tempUrlList = deque()
            self.prManager = None
            self.delay = 1

    def _clean_up(self, **kwargs):
        """
        Clean up any temporary files and close all open file handles.
        """
        self.prManager.clean()
        os.remove(self.shelfPointer)

        for name, outputObj in self.outputFiles.items():
            outputObj.cleanUp()

    def generatePath(self, **kwargs):
        """
        Generate a path according to the /lm/data2 file system structure.
        """
        try:
            lang = kwargs['lang']
        except KeyError:
            lang = self.lang

        try:
            cleanState = kwargs['cleanState']
        except KeyError:
            cleanState = 'records'

        today = tuple(yearMonthDay())
        try:
            year = kwargs['year']
        except KeyError:
            year = today[0]
        try:
            month = kwargs['month']
        except KeyError:
            month = today[1]
        try:
            day = kwargs['day']
        except KeyError:
            day = today[2]

        topic = kwargs.get('topic', self.topic)

        path = lmData2Path(self.scraperType, lang, topic,
                           self.name, cleanState, self.frequency,
                           year, month, day, self.langFirst, base=self.basePath)
        return path

    def generateFileName(self, fileType='txt', subdomain='', encoding='utf8', **kwargs):
        if subdomain:
            dot_subdomain = '.{0}'.format(subdomain)
        else:
            dot_subdomain = ''
        if encoding:
            dot_encoding = '.{0}'.format(encoding.upper())
        else:
            dot_encoding = ''

        return '{dom}{subdom}.{language}{enc}.{ft}.gz'.format(
            dom=self.name,
            subdom=dot_subdomain,
            language=kwargs.get('lang', self.lang),
            enc=dot_encoding,
            ft=fileType
        )

    def setBasePath(self, path, **kwargs):
        """
        Change the Scraper's base path for file path generation.
        """
        self.basePath = path

    def setRawDirectory(self, path, **kwargs):
        """
        Set the output directory for the raw data.
        """
        abs_path = os.path.abspath(path)
        if not validateLmData2Path(abs_path):
            LOG.warning('WARNING: Provided path does not conform to /lm/data2 standards: {0}'.format(abs_path))
            self.rawDir = abs_path
        self.rawDir = abs_path

    def addOutputFile(self, label, filename, encoding='utf8', noTemp=False, **kwargs):
        """
        Add an output file that the Scraper can write to via printToFile.
        """
        # compute the absolute path to the specified file, and
        # create any necessary directories.
        abs_path_filename = os.path.abspath(filename)
        absolute_directory = os.path.dirname(abs_path_filename)
        log_path = ''

        # parse filename to extract date and reconstruct rawPath to put the symlink
        m = re.search(r'.+/(\d{4})[/-](\d{2})[/-](\d{2})', absolute_directory)
        if m:
            y, m, d = m.groups()
        else:
            sys.exit('No date could be extracted from path. Required to generate symlink from: {0}'.format(filename))

        log_path = self.rawDir if self.rawDir else self.generatePath(year=y, month=m, day=d, cleanState='raw')

        if not os.path.exists(log_path):
            os.makedirs(log_path)
        log_path += '/symLink-'
        symlink_path = log_path + filename.split('/')[-1]

        # check to see if the file exists.  If it does, check to see if it's a symlink
        # from a previously failed run of the script.  If it is, the linked file is
        # assumed to be a temporary file.  Resume writing to this file.
        if os.path.islink(symlink_path):
            temp_file_name = os.path.realpath(symlink_path)
        elif abs_path_filename.endswith('.gz'):
            fd, temp_file_name = tempfile.mkstemp(
                suffix='.gz',
                prefix='{0}.'.format(self.name),
                dir=MainTempDirectory(directory='/work')
            )
            os.close(fd)
        else:
            fd, temp_file_name = tempfile.mkstemp(
                prefix='{0}.'.format(self.name),
                dir=MainTempDirectory(directory='/work')
            )
            os.close(fd)
            # tempFileName = CreateNewTempFile(directory=MainTempDirectory())

        if not validateLmData2Path(abs_path_filename):
            LOG.warning(
                'WARNING: Provided path does not conform to /lm/data2 standards: {0}'.format(abs_path_filename))

        if not os.path.exists(absolute_directory):
            os.makedirs(absolute_directory)

        # store the association of the full path and the
        # provided label
        self.outputFiles[label] = OutputManager(abs_path_filename, temp_file_name, enc=encoding, noTempFile=noTemp,
                                                logPathName=log_path)

    def addUrl(self, url, base=None, payload=None, method="GET", params=None, body=None, priority=False):
        """
        Add 'newUrl' to the list of URLs to scrape.  If a payload is
        provided, attach it to the new URL.  If 'baseUrl' is provided,
        use it to figure out what the absolute url should be.  If 'priority'
        is True, then the URL will be inserted into the list of URLs to
        scrape in such a position that it will be the next URL visited.
        """
        if base:
            url_to_add = urllib.parse.urljoin(base, url)
        else:
            url_to_add = url
        try:
            self.prManager.registerNewRequest(url_to_add, payload, method, params, body, priority)
        except AttributeError:
            if priority:
                self.tempUrlList.appendleft((url_to_add, payload, method, params, body))
            else:
                self.tempUrlList.append((url_to_add, payload, method, params, body))

    def printToFile(self, filename, text, noNewLine=False, **kwargs):
        """
        Print text to a file.
        """
        newline = noNewLine
        self.outputFiles[filename].printString(text, noNewLine=newline)

    def parseRunKwargs(self, **kwargs):
        """
        This method makes it so that different instantiations of the Scraper
        class object can parse kwargs in a consistent manner despite different
        run() method implementations.  This is intended to make it so that
        the user can use virtually the same code on the surface to execute
        different Scraper objects.
        """
        mule = SettingsMule()

        try:
            mule.restartFromShelf = bool(kwargs['restart'])
        except KeyError:
            mule.restartFromShelf = False

        # get the delay between page requests
        try:
            mule.delay = max(int(kwargs['delay']), self.delay)
        except KeyError:
            mule.delay = 2

        # get the number of attempts to make before giving up on a page
        try:
            mule.attempts = kwargs['attempts']
        except KeyError:
            mule.attempts = 2

        try:
            mule.saveBadUrls = kwargs['badUrlsFile']
        except KeyError:
            mule.saveBadUrls = None

        # check for an HTML preprocessing function
        try:
            mule.preprocessHTML = kwargs['HTMLpreprocessor']
            arg_list = inspect.getargspec(mule.preprocessHTML)
            if len(arg_list.args) != 1:
                raise KeyError
        except KeyError:
            # no preprocessing function provided; create a dummy function
            # that does nothing
            mule.preprocessHTML = lambda x: x
        # Check for explicit setting of website encoding
        try:
            # if the user tells us what the site's encoding is,
            # We tell BeautifulSoup it should interpret the
            # HTML as that encoding.
            mule.websiteEncoding = kwargs['siteEncoding']

        except KeyError:
            # if the user does not tell us what the site's encoding is,
            # we set websiteEncoding to None, which triggers the default
            # behavior in htmlSaveSoupRemove().
            mule.websiteEncoding = None

        # check for explicit setting of URL encoding
        try:
            mule.urlEncoding = kwargs['urlEncoding']
        except KeyError:
            mule.urlEncoding = 'utf8'

        # set the time interval at which backups will be made of the shelf
        try:
            mule.backupDelta = kwargs['shelfBackupInterval']
        except KeyError:
            mule.backupDelta = 12.0  # default is 12 hours

        try:
            mule.reuseUrls = bool(kwargs['reuseUrls'])
        except KeyError:
            mule.reuseUrls = False

        # get the timeout in request
        try:
            mule.timeout = int(kwargs['timeout'])
        except KeyError:
            mule.timeout = 30

        return mule

    def _setup(self, **kwargs):
        try:
            restart_from_shelf = bool(kwargs['restart'])
        except KeyError:
            restart_from_shelf = False

        if not self.rawDir:
            # if the user has not specified the raw directory,
            # generate that now.
            self.setRawDirectory(path=self.generatePath(cleanState='raw'))
        if not os.path.exists(self.rawDir):
            # create the directories if they do not already exist
            os.makedirs(self.rawDir)

        self.shelfPointer = os.path.join(
            self.rawDir,
            self.generateFileName(fileType='shelf', subdomain=self.rawFileSubDomain)
        )

        if os.path.exists(self.shelfPointer):
            # attempt to read from the shelf pointer.
            # Expectation is that a file pointing to the shelf
            # is located next to the html.
            with open(self.shelfPointer, 'r') as shelfPtrFile:
                self.shelfDirectory = str(shelfPtrFile.read().strip())
                preexistingShelf = True
        else:
            # shelf pointer file does not exist
            preexistingShelf = False
            self.shelfDirectory = tempfile.mkdtemp(
                prefix='{0}.shelf.'.format(self.name),
                dir=MainTempDirectory(directory='/work')
            )
            with open(self.shelfPointer, "wt") as shelfPtrFile:
                shelfPtrFile.write(self.shelfDirectory)

        self.prManager = RequestManager(directory=self.shelfDirectory)

        if restart_from_shelf and preexistingShelf:
            self.prManager.load()
        else:
            if len(self.tempUrlList) == 0:
                LOG.error('ERROR: no URLs provided')
                sys.exit(1)
            while self.tempUrlList:
                url, payload, method, params, body = self.tempUrlList.popleft()
                self.prManager.registerNewRequest(url, payload, method, params, body)

        timeout = int(kwargs['timeout'])
        self._api_client = BaseClient(timeout=timeout)
        self._api_client.set_default_header("User-agent", DEFAULT_USER_AGENT)

    def run(self, process_response, **kwargs):
        mule = self.parseRunKwargs(**kwargs)
        bad_urls_list = []
        self._setup(restart=mule.restartFromShelf, timeout=mule.timeout)
        while self.prManager.hasOpenRequests():
            pr = self.prManager.next()
            current_url = pr.get('url')
            method = pr.get('method')
            params = pr.get('params')
            body = pr.get('body')

            if method == 'GET':
                LOG.info('skv3_1---+{0}?{1}'.format(current_url, json.dumps(params)))
            else:
                LOG.info('skv3_1---+{0}:{1}'.format(current_url, json.dumps(body)))

            if mule.urlEncoding:
                formatted_url = reformatURL(current_url, encoding=mule.urlEncoding)
                if formatted_url != current_url:
                    LOG.error('New Formatted url: %s' % formatted_url)
            else:
                formatted_url = current_url
            payload = pr.get('payload')

            # headers = pr.get('headers') todo, let user can define the header for each request

            try:
                response = self._api_client.call_api(formatted_url,
                                                     method=method,
                                                     params=params,
                                                     body=body,
                                                     delay=mule.delay,
                                                     attempts=mule.attempts)
            except Exception as ex:
                LOG.error('Failed to get page: ', formatted_url)
            else:
                try:
                    process_response(response, current_url, payload,
                                     self.addUrl,
                                     self.printToFile)
                except Exception as ex:
                    # catch other errors
                    if mule.saveBadUrls:
                        # print to report
                        LOG.error('Error processing page with error %s; adding to list of bad URLs' % ex)
                        bad_urls_list.append((current_url, payload))
                        break
                    else:
                        traceback.print_exc()
                        sys.exit(1)

            # set the status of the Request to complete
            pr.set('status', _CLOSED)

        if len(bad_urls_list) > 0:
            # print the bad URLs to a file
            LOG.error('''
            Some problems occurred while processing certain URLs.  Those URLs and their payloads have been printed
            to the following file:
            {0}
            '''.format(mule.saveBadUrls))
            bad_url_folder = os.path.dirname(mule.saveBadUrls)
            if not os.path.exists(bad_url_folder):
                os.makedirs(bad_url_folder)
            with open(mule.saveBadUrls, 'w') as badUrlsHandle:
                for item in bad_urls_list:
                    badUrlsHandle.write('{0}\t{1}'.format(item[0], repr(item[1])))

        self._clean_up()


class RequestManager:
    """
    Object used by the Scraper class to perform the actual page request
    management, including data persistency.
    """

    def __init__(self, directory, **kwargs):
        self.homeDirectory = os.path.abspath(directory)
        if not os.path.exists(self.homeDirectory):
            os.makedirs(self.homeDirectory)
        self.openPageRequests = deque()
        self.firstInLine = 0
        self.lastInLine = 0

    def next(self, **kwargs):
        """
        Returns the next page request in the queue.
        """

        while True:
            result = self.openPageRequests.popleft()
            return result

    def hasOpenRequests(self):
        """
        Return True if there are open requests pending.
        """
        if self.openPageRequests:
            return True
        else:
            return False

    def __getNextID(self, front):
        if front:
            self.firstInLine -= 1
            return self.firstInLine
        else:
            self.lastInLine += 1
            return self.lastInLine

    def registerNewRequest(self, url, payload, method="GET", params=None, body=None, priority=False, **kwargs):
        self.process_new_request(url, payload, method, params, body, priority, **kwargs)

    def process_new_request(self, url, payload, method="GET", params=None, body=None, priority=False, **kwargs):
        filename = CreateNewTempFile(suffix='.shelf.gz', directory=self.homeDirectory)
        os.remove(filename)  # delete the text file so that shelve works
        pr = APIRequest(
            self.__getNextID(priority),
            url,
            payload,
            method,
            params,
            body,
            filename,
        )
        if priority:
            self.openPageRequests.appendleft(pr)
        else:
            self.openPageRequests.append(pr)

    def clean(self, **kwargs):
        """
        delete the custom shelf files along with the directory that
        contains them.
        """
        if validHomeDir.search(self.homeDirectory):
            self.homeDirectory = re.sub('^\/home\/', '/work/', self.homeDirectory)
            LOG.info("""Warning::: the home directory of shelf file is {0}""".format(self.homeDirectory))
        for file_temp in os.listdir(self.homeDirectory):
            if not validShelfFile.search(file_temp):
                LOG.info("""Warning::: Skipping non shelf file {0}""".format(file_temp))
                continue
            os.remove(os.path.join(self.homeDirectory, file_temp))
        try:
            remove_only_files = kwargs['removeOnlyFiles']
        except KeyError:
            remove_only_files = False

        if not remove_only_files and not validHomeDir.search(self.homeDirectory):
            os.rmdir(self.homeDirectory)

    def load(self, **kwargs):
        """
        Load the custom shelf files from the specified directory.
        """
        file_listing = os.listdir(self.homeDirectory)
        open_p_rs = {}
        for file_rp in file_listing:
            pr = APIRequest(shelf=os.path.join(self.homeDirectory, file_rp))
            if pr.isOpen():
                open_p_rs[pr.get('id')] = pr
        self.openPageRequests = deque(
            request for (id, request) in sorted(open_p_rs.items(), key=lambda x: x[0])
        )
        if len(open_p_rs.keys()) > 0:
            self.firstInLine = min(open_p_rs.keys())
            self.lastInLine = max(open_p_rs.keys())
        else:
            LOG.error(
                """ERROR: No open page requests remaining.  Please delete temporary shelf directory: {0}""".format(
                    self.homeDirectory))
            sys.exit(1)


class APIRequest:
    """
    Object carrying the information for a page to be scraped.
    """

    def __init__(self, id=None, url=None, payload=None, method="GET", params=None, body=None, shelf=None):
        self.pathToShelf = shelf

        # if the file exists already, we're creating the
        # PageRequest from an existing object.
        rebuild = os.path.exists(self.pathToShelf)

        self.actualShelf = shelve.open(self.pathToShelf)
        self.attrs = {}

        if rebuild:
            for item in ['id', 'url', 'payload', 'method', 'params', 'body', 'status']:
                self.attrs[item] = self.actualShelf[item]
        else:
            self.attrs['id'] = id
            self.attrs['url'] = url
            self.attrs['payload'] = payload
            self.attrs['method'] = method
            self.attrs['params'] = params
            self.attrs['body'] = body
            self.attrs['status'] = _OPEN

            for key, val in self.attrs.items():
                self.actualShelf[key] = val

        self.actualShelf.close()

    def set(self, attr, val):
        if self.attrs[attr] != val:
            self.actualShelf = shelve.open(self.pathToShelf)
            self.actualShelf[attr] = val
            self.attrs[attr] = val
            self.actualShelf.close()

    def get(self, attr):
        return self.attrs[attr]

    def isOpen(self):
        return self.attrs['status'] == _OPEN