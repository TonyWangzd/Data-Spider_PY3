import logging
import re
LOG = logging.getLogger(__name__)

LOG.info("add info log")
LOG.debug("add Debug log")


ss = "当前在第 1 页 共计 6660 个页面 共有 133193 条记录"
s = re.findall("\d+", ss)
print(s)
