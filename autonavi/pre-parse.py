import zipfile, gzip
import logging
from autonavi.autonaviParser import filename_extraction

LOG = logging.getLogger("extract_autonavi_address_CHN")

IGNORE_DISTRICT = ["海域"]

def show_duplic_admin_id(admin_id, admin_name_list):
    if len(admin_name_list) > 1:
        LOG.warning(admin_name_list)


adminDict = dict()
zipInputFile = "temp/2018/02/27/Autonavi_data_2017_Q4.zip"
zip_files = zipfile.ZipFile(zipInputFile, 'r')
road_file, adminFile, house_file, poiFileList = filename_extraction(zip_files, "CHN")
with zip_files.open(adminFile) as admin_file_handler:
    admin_file_handler.readline()  # skip first line
    for line in admin_file_handler.readlines():
        tmpList = line.decode(encoding="GBK").strip().split('\t')
        district_name = tmpList[1]
        district_admin_id = tmpList[3]
        if district_name not in IGNORE_DISTRICT:
            if district_admin_id in adminDict:
                adminDict[tmpList[3]].append(district_name)
            else:
                adminDict[tmpList[3]] = [district_name]

for admin_id in adminDict:
    if len(adminDict.get(admin_id)) > 1:
        LOG.warning("admin id %s are same for %s" % (admin_id, str(adminDict.get(admin_id))))


LOG.info("Done")
