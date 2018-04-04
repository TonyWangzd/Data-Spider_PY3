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
# autonaviParser.py
#
# Purpose : Release autonavi database.
#
# Ticket Link  : https://bn-fbdb01.nuance.com/default.asp?48742
#
# Jeff Jia , for Nuance Corporation, Chengdu, China
#
# Date Started: 2018/03/26
#
#####################################################################

import zipfile
import gzip
from time import localtime, strftime
import time
from optparse import OptionParser

import os
import sys
import csv
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt=' %Y/%m/%d %H:%M:%S')
LOG = logging.getLogger(__name__)

csv.field_size_limit(sys.maxsize)

startTimer = time.time()

POI_EXT = '00.txt'
POI_EXT_CHN = '00.res'
EXT = '.txt'
EXT_CHN = '.res'
ENCODING = 'utf-8'

ROAD_FILENAME = 'NaviDataRoad'  # --> to generate autonavi.address.street.*-CHN.UTF8.tsv.gz
HOUSE_FILENAME = 'NaviDataHouseNo'  # --> to generate autonavi.address.other.*-CHN.UTF8.tsv.gz
ADMIN_FILENAME = 'NaviDataAdmin'
POI_TYPE_FILENAME = 'NaviDataOther'
POI_CATEGORY_FILENAME = 'NaviPoiCategory'

IGNORED = ['110100', '110200', '120100', '120200', '310100', '310200', '500100', '500200']

LANGUAGE_LIST = {'CHN': 'zho-CHN', 'ENG': 'eng-CHN'}
IGNORE_DISTRICT = ["海域", "Maritime Space"]

HEADER_COLUMN_ADDRESS = ["address_state",
                         "address_state_pronounce_csv",
                         "address_city",
                         "address_city_pronounce_csv",
                         "address_district",
                         "address_district_pronounce_csv",
                         "address_street",
                         "address_street_pronounce_csv",
                         "address_street_segmented",
                         "address_street_segmented_pronounce_csv"]

HEADER_COLUMN_POI = ["address_state",
                     "address_city",
                     "address_city_pronounce",
                     "address_district",
                     "address_district_pronounce"
                     "poi_name",
                     "poi_name_pronounce",
                     "poi_name_segmented",
                     "poi_name_segmented_pronounce",
                     "poi_address",
                     "category",
                     "category_pronounce",
                     "category_segmented",
                     "category_segmented_pronounce",
                     "number_id",
                     "category_other",
                     "category_other_pronounce",
                     "category_other_segmented"
                     "category_other_segmented_pronounce"]

FILE_ADDRESS_NAME = "autonavi.address.%s.%s.UTF8.tsv.gz"
FILE_POI_NAME = "autonavi.poi.%s.UTF8.tsv.gz"
ADDRESS_RECORD = 'address-autonavi/records.versions'
POI_RECORD = 'poi-autonavi/records.versions'


############################################################################

# Extraction of files inside the ZIP file
def filename_extraction(file_obj, language=''):
    poi_file_list = []
    road_file = ''
    house_file = ''
    admin_file = ''
    poi_type_file = ''
    poi_category_file = ''
    for filename in file_obj.namelist():
        if language in filename:
            if ROAD_FILENAME and ROAD_FILENAME in filename:
                road_file = filename
            elif HOUSE_FILENAME and HOUSE_FILENAME in filename:
                house_file = filename
            elif ADMIN_FILENAME and ADMIN_FILENAME in filename:
                admin_file = filename
            elif POI_TYPE_FILENAME and POI_TYPE_FILENAME in filename:
                poi_type_file = filename
            elif POI_CATEGORY_FILENAME and POI_CATEGORY_FILENAME in filename:
                poi_category_file = filename
            else:
                if POI_EXT in filename or POI_EXT_CHN in filename:
                    poi_file_list.append(filename)
    LOG.info("get RoadFile %s, House File %s, adminFile %s and total %d data file" % (
        road_file, house_file, admin_file, len(poi_file_list)))
    return road_file, admin_file, house_file, poi_file_list, poi_type_file, poi_category_file


def print_string(output_file, input_string):
    """
    Print the input_string to the appropriate file.
    If noNewLine is True, then use the .write() function to suppress
    the newline that is automatically appended when using the
    normal print function.
    """
    with gzip.open(output_file, 'a') as handle:
        handle.write(bytes(input_string, 'utf-8'))
        handle.write(bytes('\n', 'utf-8'))
    handle.close()


def print_lines(output_file, input_lines):
    """
    Print the input_lines to the appropriate file.
    If noNewLine is True, then use the .write() function to suppress
    the newline that is automatically appended when using the
    normal print function.
    """
    with gzip.open(output_file, 'a') as handle:
        for inputString in input_lines:
            handle.write(bytes(inputString, 'utf-8'))
            handle.write(bytes('\n', 'utf-8'))
    handle.close()


def get_year_month_day_path():
    return (time.strftime("%Y/%m/%d", time.gmtime()))


def normalize_pronounce(pronounce):
    return pronounce.strip().replace(';', ',')


#############################################################################

def parse_four_columns_file(parse_file):
    """
    the file if has twelve data column will be parse by this function
        column 1 is NDA it should be index
        column 2 is name  *
        column 3 is name half
        column 4 is PronsNum
        column 5 is LH_prons
        column 6 is SampaProns
        column 7 is PY_Prons *
        column 8 PartName *
        column 9 LH_PartProns
        column 10 SampaPartProns
        column 11 PY_PartProns *
        column 12 AD_CODE(admin code or category code)
    or if has only four column
        column 1 is index *
        column 2 is name *
        column 3 is pinyin *
        column 4 is code(admin code or category code) *
    the columns which mark as * is which we needed
    parse_dict = {admin_id: list(name, name_pronounce, name_segmented, name_segmented_pronounce)}
    :param parse_file:
    :return:
    """
    parse_dict = dict()
    with zip_files.open(parse_file) as parse_file_handler:
        parse_file_handler.readline()  # skip header line
        for line in parse_file_handler.readlines():
            try:
                tmp_list = line.decode(encoding="GBK").strip().split('\t')
            except Exception as ex:
                LOG.error("error happened with message %s" % ex)
                continue
            if len(tmp_list) > 4:
                name = tmp_list[1]
                name_pronounce = normalize_pronounce(tmp_list[6])
                name_segmented = tmp_list[7]
                name_segmented_pronounce = normalize_pronounce(tmp_list[10])
                admin_id = tmp_list[11]
            else:
                name = tmp_list[1]
                name_pronounce = tmp_list[2]
                admin_id = tmp_list[3]
                name_segmented = ""
                name_segmented_pronounce = ""
            if name not in IGNORE_DISTRICT:
                if admin_id in parse_dict and name not in parse_dict[admin_id]:
                    parse_dict[admin_id].append((name, name_pronounce, name_segmented, name_segmented_pronounce))
                else:
                    parse_dict[admin_id] = [(name, name_pronounce, name_segmented, name_segmented_pronounce)]
            else:
                LOG.debug("add ignore district name %s" % name)
    return parse_dict


def parse_poi_category(parse_file):
    """
    parse poi category file, which contain three column
        column 1 category id *
        column 2 category name *
        column 3 Name_half
        column 4 PronsNum
        column 5 LH_prons
        column 6 SampaProns
        column 7 PY_Prons *
        column 8 PartName *
        column 9 LH_PartProns
        column 10 SampaPartProns
        column 11 PY_PartProns *
    OR if has only THREE column
        column 1 category id *
        column 2 category name *
        column 3 category name pinyin *
    the columns which mark as * is which we needed
    the category id should not have duplicated
    :param parse_file:
    :return:
    """
    parse_dict = dict()
    with zip_files.open(parse_file) as poi_file_handler:
        poi_file_handler.readline()  # skip first line
        for line in poi_file_handler.readlines():
            try:
                tmp_list = line.decode(encoding="GBK").split('\t')
            except Exception as ex:
                LOG.error("error happened with message %s" % ex)
                continue
            if len(tmp_list) > 4:
                cat_id = tmp_list[0].strip()
                name = tmp_list[1].strip()
                name_pronounce = normalize_pronounce(tmp_list[6])
                name_segmented = tmp_list[7].strip()
                name_segmented_pronounce = normalize_pronounce(tmp_list[10])
            else:
                cat_id = tmp_list[0].strip()
                name = tmp_list[1].strip()
                name_pronounce = tmp_list[2].strip()
                name_segmented = ""
                name_segmented_pronounce = ""
            if cat_id in parse_dict:
                LOG.error("ERROR !!!! Duplicated Category Other ID !!!!!!!! STOP and RECHECK parser")
            if len(tmp_list) > 3:
                LOG.error("ERROR !!!! More Category Other column !!!!!!!! STOP and RECHECK parser")
            parse_dict[cat_id] = (name, name_pronounce, name_segmented, name_segmented_pronounce)
    return parse_dict


def preprocessing_admin_dict(admin_dict_tem):
    """
    Remove the item which has mulit admin name and identify them as ignore list
    :param admin_dict_tem:
    :return:
    """
    admin_dict = dict()
    ignore_list = list()
    for admin_id in admin_dict_tem:
        admin_name_list = admin_dict_tem.get(admin_id)
        if len(admin_name_list) > 1:
            LOG.warning("admin id %s are same for %s" % (admin_id, str(admin_name_list)))
            ignore_list.append(admin_id)
        else:
            admin_dict[admin_id] = admin_name_list[0]
    return admin_dict, ignore_list


# def preprocessing_poi_category_dict(poi_type_dict_tem):
#     """
#     Remove the item which has mulit admin name and identify them as ignore list
#     :param poi_type_dict_tem:
#     :return:
#     """
#     poi_cat_dict = dict()
#     ignore_list = list()
#     for pot_cat_id in poi_type_dict_tem:
#         poi_cat_name_list = poi_type_dict_tem.get(pot_cat_id)
#         if len(poi_cat_name_list) > 1:
#             LOG.warning("admin id %s are same for %s" % (pot_cat_id, str(poi_cat_name_list)))
#             poi_cat_name_csv = ','.join([name_item[0] for name_item in poi_cat_name_list])
#             poi_cat_name_pinyin_csv = ','.join([name_item[1] for name_item in poi_cat_name_list])
#             poi_cat_name_pinyin_csv
#             ignore_list.append((pot_cat_id, (poi_cat_name_csv, poi_cat_name_pinyin_csv)))
#             poi_cat_dict[pot_cat_id] = (poi_cat_name_csv, poi_cat_name_pinyin_csv)
#         else:
#             poi_cat_dict[pot_cat_id] = poi_cat_name_list[0]
#     return poi_cat_dict, ignore_list


def preprocessing_poi_category_dict(poi_cat_dict_tem):
    """
    Remove the item which has multi admin name and identify them as ignore list
    :param poi_cat_dict_tem:
    :return:
    """
    poi_type_dict = dict()
    ignore_list = list()
    for admin_id in poi_cat_dict_tem:
        admin_name_list = poi_cat_dict_tem.get(admin_id)
        if len(admin_name_list) > 1:
            LOG.warning("admin id %s are same for %s" % (admin_id, str(admin_name_list)))
            poi_cat_name_csv = ','.join([name_item[0] for name_item in admin_name_list])
            poi_cat_name_pronounce_csv = ','.join([name_item[1] for name_item in admin_name_list if name_item[1]])
            poi_cat_segmented = ','.join([name_item[2] for name_item in admin_name_list if name_item[2]])
            poi_cat_segmented_pronounce_csv = ','.join([name_item[3] for name_item in admin_name_list if name_item[3]])
            ignore_list.append((admin_id, (poi_cat_name_csv, poi_cat_name_pronounce_csv)))
            poi_type_dict[admin_id] = (
                poi_cat_name_csv, poi_cat_name_pronounce_csv, poi_cat_segmented, poi_cat_segmented_pronounce_csv)
        else:
            poi_type_dict[admin_id] = admin_name_list[0]
    return poi_type_dict, ignore_list


def print_header(output_file, header):
    output_file_folder = os.path.dirname(output_file)
    if not os.path.exists(output_file_folder):
        os.makedirs(output_file_folder)
    print_string(output_file, "#scraper01 " + "\t".join(header))


def parse_files():
    if options.language == "CHN":
        (road_file, admin_file, house_file, poi_file_list, poi_type_file, poi_cat_file) = filename_extraction(zip_files)
        admin_dict_temp = parse_four_columns_file(admin_file)
        admin_dict, ignore_list = preprocessing_admin_dict(admin_dict_temp)
        IGNORED.extend(ignore_list)

        parse_address_CHN(admin_dict, road_file, "street")
        parse_address_CHN(admin_dict, house_file, "other")
        parse_poi_CHN(admin_dict, poi_cat_file, poi_type_file, poi_file_list)
    else:
        (road_file, admin_file, house_file, poi_file_list, poi_type_file, poi_cat_file) = filename_extraction(
            zip_files,
            options.language)
        admin_dict_temp = parse_four_columns_file(admin_file)
        admin_dict, ignore_list = preprocessing_admin_dict(admin_dict_temp)
        IGNORED.extend(ignore_list)
        parse_address_ENG(admin_dict, road_file, "street")
        parse_address_ENG(admin_dict, house_file, "other")
        parse_poi_ENG(admin_dict, poi_cat_file, poi_type_file, poi_file_list)


def parse_address_CHN(admin_dict, address_file, sub_domain):
    address_dict = parse_four_columns_file(address_file)
    output_file = os.path.join(options.dest, ADDRESS_RECORD, "%s", get_year_month_day_path(), FILE_ADDRESS_NAME)

    output_file_zho = output_file % ("zho-CHN", "zho-CHN", sub_domain)
    output_file_yuc = output_file % ("yuc-CHN", "yuc-CHN", sub_domain)
    print_header(output_file_zho, HEADER_COLUMN_ADDRESS)
    print_header(output_file_yuc, HEADER_COLUMN_ADDRESS)

    for adminID in admin_dict:
        if adminID[0:2] == '82' or adminID[0:2] == '81':
            address_parse_and_print(output_file_yuc, adminID, admin_dict, address_dict)
        else:
            address_parse_and_print(output_file_zho, adminID, admin_dict, address_dict)


def parse_address_ENG(admin_dict, address_file, sub_domain):
    address_dict = parse_four_columns_file(address_file)
    output_file = os.path.join(options.dest, ADDRESS_RECORD, "%s", get_year_month_day_path(), FILE_ADDRESS_NAME)

    output_file_eng = output_file % ("eng-CHN", "eng-CHN", sub_domain)
    print_header(output_file_eng, HEADER_COLUMN_ADDRESS)

    for adminID in admin_dict:
        address_parse_and_print(output_file_eng, adminID, admin_dict, address_dict)


def address_parse_and_print(print_file, admin_id, admin_dict, address_dict):
    if admin_id[-4:] == '0000':
        state, state_pronounce, _, _ = admin_dict.get(admin_id)
        address_list = address_dict.get(admin_id)
        if address_list:
            LOG.error("admin id %s with name %s mapping to a location" % (admin_id, state))
    elif admin_id[-2:] == '00':
        city, city_pronounce, _, _ = admin_dict.get(admin_id)
        if admin_id in IGNORED:
            LOG.warning('Check CITY ONLY, IGNORED admin id %s with name %s' % (admin_id, city))
            city = ''
        state_id = admin_id[:2] + '0000'
        state, state_pronounce, state_segmented, state_segmented_pronounce = admin_dict.get(state_id, ("", "", "", ""))
        address_list = address_dict.get(admin_id)
        if address_list:
            if not state:
                LOG.warning("state not exist for admin id %s" % admin_id)
            record_list = list()
            for address, address_pronounce, address_segmented, address_segmented_pronounce in address_list:
                record = "\t".join((state.strip(), state_pronounce.strip(), city.strip(), city_pronounce.strip(),
                                    "", "", address.strip(), address_pronounce.strip(), address_segmented.strip(),
                                    address_segmented_pronounce.strip()))
                record_list.append(record)
            print_lines(print_file, record_list)
            # LOG.error("admin id %s with name %s %s mapping to a location" % (admin_id, state, city))
    else:
        district, district_pronounce, _, _ = admin_dict.get(admin_id, ("", "", "", ""))
        address_list = address_dict.get(admin_id)
        city_id = admin_id[:4] + '00'
        city, city_pronounce, _, _ = admin_dict.get(city_id, ("", "", "", ""))
        if city:
            if city_id in IGNORED:
                LOG.info('IGNORED admin id %s with name %s' % (admin_id, city))
                city = ''
                city_pronounce = ''
        else:
            if city_id not in IGNORED:
                LOG.error("City not exist for %s" % admin_id)
        state_id = admin_id[:2] + '0000'
        state, state_pronounce, _, _ = admin_dict.get(state_id, ("", "", "", ""))

        if not city:
            if city_id not in IGNORED:
                LOG.warning("city not exist for admin id %s" % admin_id)
        if not state:
            LOG.warning("state not exist for admin id %s" % admin_id)
        if address_list:
            record_list = list()
            for address, address_pronounce, address_segmented, address_segmented_pronounce in address_list:
                record = "\t".join(
                    (state.strip(), state_pronounce.strip(), city.strip(), city_pronounce.strip(), district.strip(),
                     district_pronounce.strip(), address.strip(), address_pronounce.strip(), address_segmented.strip(),
                     address_segmented_pronounce.strip()))
                record_list.append(record)
            print_lines(print_file, record_list)
        else:
            record = "\t".join((state.strip(), state_pronounce.strip(), city.strip(), city_pronounce.strip(),
                                district.strip(), district_pronounce.strip(), "", "", "", ""))
            LOG.error("admin id %s with name %s not have address info" % (admin_id, district))
            print_string(print_file, record)


def poi_parse_and_print(print_file, poi_file, admin_dict, poi_cat_dict, poi_cat_other_dict, province, city_admin_id):
    """
    there are two typ POI file if it has 8 column then it didn't have the poi category column then ignore it
    :param print_file:
    :param poi_file:
    :param admin_dict:
    :param poi_cat_dict:
    :param poi_cat_other_dict:
    :param province:
    :param city_admin_id:
    :return:
    """
    with zip_files.open(poi_file) as poi_file_handler:
        poi_file_handler.readline()  # skip first line
        record_list = list()
        for index, line in enumerate(poi_file_handler.readlines()):
            try:
                item_list = line.decode(encoding="GBK").strip().split('\t')
            except Exception as ex:
                LOG.error("error happened with message %s for file %s in line %d" % (ex, poi_file, index))
                continue
            if index != 0 and index % 50000 == 0:
                print_lines(print_file, record_list)
                record_list.clear()
                LOG.info('%d entries processed' % index)
            poi_cat_other_id = 0
            poi_name_segmented = ""
            poi_name_segmented_pronounce=""
            if len(item_list) == 7:
                (_, _, poi_name, poi_name_pronounce, poi_address, district_admin_id, poi_cat_id) = item_list
            elif len(item_list) == 9:
                (_, _, poi_name, poi_name_pronounce, poi_address, district_admin_id, poi_cat_id, _,
                 poi_cat_other_id) = item_list
            elif len(item_list) == 15:
                (_, _, poi_name, _, _, _, _, poi_name_pronounce, poi_name_segmented, _, _, poi_name_segmented_pronounce,
                 poi_address, district_admin_id, poi_cat_id) = item_list
            else:
                LOG.error("ERROR !!! the POI %s didn't have the correct data %s" % (poi_file, str(item_list)))
                continue
            if poi_cat_other_id == 0:
                poi_cat_other_id_str = ""
            else:
                poi_cat_other_id_str = str(poi_cat_other_id)

            poi_cat, poi_cat_pronounce, poi_cat_segmented, poi_cat_segmented_pronounce = poi_cat_dict.get(poi_cat_id, (
                "", "", "", ""))
            poi_cat_other, poi_cat_other_pronounce, poi_cat_other_segmented, poi_cat_other_segmented_pronounce = \
                poi_cat_other_dict.get(poi_cat_other_id, ("", "", "", ""))
            city, city_pronounce, _, _ = admin_dict.get(city_admin_id, ("", "", "", ""))
            if city_admin_id in IGNORED:
                # LOG.warning('IGNORED admin id %s with name %s' % (city_admin_id, city))
                city = ''
                city_pronounce = ''
            district, district_pronounce, _, _ = admin_dict.get(district_admin_id, ("", "", "", ""))
            record = "\t".join((
                province.strip(),
                city.strip(),
                city_pronounce.strip(),
                district.strip(),
                district_pronounce.strip(),
                poi_name.strip(),
                poi_name_pronounce.strip(),
                poi_name_segmented.strip(),
                poi_name_segmented_pronounce.strip(),
                poi_address.strip(),
                poi_cat.strip(),
                poi_cat_pronounce.strip(),
                poi_cat_segmented.strip(),
                poi_cat_segmented_pronounce.strip(),
                poi_cat_id.strip(),
                poi_cat_other.strip(),
                poi_cat_other_pronounce.strip(),
                poi_cat_other_id_str.strip()
            ))
            record_list.append(record)
        if record_list:
            print_lines(print_file, record_list)


def extraction_from_filename(poi_file, language=None):
    """
    Extraction of region and city from filename
    Filename structure as NaviDataPOI/<province>/<city_code>.txt
    :param language:
    :param poi_file:
    :return:
    """
    if language == "ENG":
        clean_split_name = poi_file.replace(EXT, "").split('/')
    else:
        clean_split_name = poi_file.replace(EXT_CHN, "").split('/')
    if len(clean_split_name) > 2:
        item_tuple = (clean_split_name[-2], clean_split_name[-1])
        return item_tuple


def parse_poi_CHN(admin_dict, poi_cat_other_file, poi_cat_file, poi_file_list):
    poi_cat_other_dict = parse_poi_category(poi_cat_other_file)
    poi_cat_dict_temp = parse_four_columns_file(poi_cat_file)

    poi_cat_dict, poi_cat_ignore = preprocessing_poi_category_dict(poi_cat_dict_temp)
    if poi_cat_ignore:
        LOG.error("Warning !!!! POI TYPE HAS Duplicated !!!!!!!!")

    output_file = os.path.join(options.dest, POI_RECORD, "%s", get_year_month_day_path(), FILE_POI_NAME)
    output_file_zho = output_file % ("zho-CHN", "zho-CHN")
    output_file_yuc = output_file % ("yuc-CHN", "yuc-CHN")
    print_header(output_file_zho, HEADER_COLUMN_POI)
    print_header(output_file_yuc, HEADER_COLUMN_POI)

    for poi_file in poi_file_list:
        (province, cityCode) = extraction_from_filename(poi_file)
        if '820000' in poi_file or '810000' in poi_file:
            poi_parse_and_print(output_file_yuc, poi_file, admin_dict, poi_cat_dict, poi_cat_other_dict, province,
                                cityCode)
        else:
            poi_parse_and_print(output_file_zho, poi_file, admin_dict, poi_cat_dict, poi_cat_other_dict, province,
                                cityCode)


def parse_poi_ENG(admin_dict, poi_cat_file, poi_type_file, poi_file_list):
    poi_cat_dict = parse_poi_category(poi_cat_file)
    poi_type_dict_temp = parse_four_columns_file(poi_type_file)

    poi_type_dict, poi_type_ignore = preprocessing_poi_category_dict(poi_type_dict_temp)
    if poi_type_ignore:
        LOG.error("Warning !!!! POI TYPE HAS Duplicated !!!!!!!!")

    output_file = os.path.join(options.dest, POI_RECORD, "%s", get_year_month_day_path(), FILE_POI_NAME)
    output_file_eng = output_file % ("eng-CHN", "eng-CHN")
    print_header(output_file_eng, HEADER_COLUMN_POI)

    for poi_file in poi_file_list:
        (province, cityCode) = extraction_from_filename(poi_file, "ENG")
        poi_parse_and_print(output_file_eng, poi_file, admin_dict, poi_type_dict, poi_cat_dict, province, cityCode)


def main():
    parse_files()

    LOG.info('Program %s Complete at %s' % (__file__, strftime("%Y-%m-%d %H:%M:%S", localtime())))
    LOG.info('Elapsed time (sec) = %d' % (time.time() - startTimer))
    LOG.info('Program finished')


#############################################################################
usage = '''
 python3.3 %prog [--debug] 

 <<NOTE>> 
'''

#############################################################################

if __name__ == '__main__':
    # Give raw data file path (zipped) and output file name as runtime args
    parser = OptionParser(usage=usage)

    parser.add_option('--basepath', '-b', dest='basepath',
                      default='/lm/data2/purchased/address-autonavi/raw.versions/zho-CHN/2018/02/27')
    parser.add_option('--zip', '-z', dest='zip', default='Autonavi_2017_Q4.zip')
    parser.add_option('--lang', dest='language', default='CHN',
                      help='optional are CHN and ENG')
    parser.add_option('--dest', '-d', dest='dest', default='/lm/data2/purchased',
                      help='location of records.versions folder')
    parser.add_option('--small', action='store_true', dest='small', default=False,
                      help='generates small output for testing')
    options, args = parser.parse_args()
    if options.language in LANGUAGE_LIST:
        zip_files = zipfile.ZipFile(options.basepath + '/' + options.zip, 'r')
        main()
    else:
        LOG.error("need specify correct language, the option are CHN and ENG")
