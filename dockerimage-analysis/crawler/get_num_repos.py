from time import sleep
from selenium import webdriver
import datetime

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
