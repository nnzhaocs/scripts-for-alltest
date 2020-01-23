from datetime import datetime
from threading import Timer
from time import sleep
from selenium import webdriver

x=datetime.today()
y=x.replace(day=x.day+1, hour=1, minute=0, second=0, microsecond=0)
delta_t=y-x

secs=delta_t.seconds+1

def get_num_repos():
    #settings for using a chrome executable in a non-standard location
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    browser = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)

    browser.get('https://hub.docker.com/search?q=&page=1')

    page = browser.page_source
    #page.encode('utf-8')

    element = browser.find_element_by_xpath("//div[@class='styles__currentSearch___35kW_']")
    #print element.text 

    f = open("num_repos.txt","a")
    f.write(str(datetime.datetime.now()) +'\t' + element.text[10:19] + '\n')
    f.close()
    sleep(5)
    browser.quit()
    

t = Timer(secs, get_num_repos)
t.start()
