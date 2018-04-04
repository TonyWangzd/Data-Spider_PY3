from urllib import request
import urllib

url = "https://movie.douban.com/subject/5202003/"
# proxy = {"http":"61.94.216.243:8080"}

try:
    proxy = request.ProxyHandler({'http': '46.101.86.225:8118'})  # 设置proxy
    opener = request.build_opener(proxy)  # 挂载opener
    request.install_opener(opener)  # 安装opener
    page = opener.open(url).read()
    print(page)
except Exception as e:
    print(e)