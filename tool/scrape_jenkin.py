import requests
from bs4 import BeautifulSoup
from lxml import html

session_requests = requests.session()
headers = {
    'User-Agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"
}

login_url = "https://app-dragon-jenkins.nrc1.us.grid.nuance.com:8443/j_acegi_security_check"
data = {
    "j_username": "lei_jia",
    "j_password": "Cmoto*817123",
}
result = session_requests.post(login_url, headers=headers, data=data, verify=False)


head = result.headers
cookie = result.cookies
result = session_requests.get("https://app-dragon-jenkins.nrc1.us.grid.nuance.com:8443/job/SANDBOX/job/Job1/", verify=False)


soup = BeautifulSoup(result.text, 'html.parser')
name_div = soup.find("div", {"class": "ownership-user-info"})
a_links = name_div.findAll('a')
email_a = a_links[-1:][0]
email = email_a.text[1:-1]

print(email)
