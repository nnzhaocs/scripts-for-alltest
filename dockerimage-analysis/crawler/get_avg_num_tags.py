from time import sleep
from selenium import webdriver
import datetime
import csv

#settings for using a chrome executable in a non-standard location
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
browser = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)

f = open("tags.txt","a")

page_number = 0
url = 'https://hub.docker.com/search/?q=&type=image&page=%d'
while True:
    page_number = page_number + 1
    browser.get(url % page_number)

    #getting repos links from page
    repos = browser.find_elements_by_xpath("//div[@class='styles__resultsWrapper___38JCx']")
    list_of_repos = []
    for repo in repos:
        elements = repo.find_elements_by_tag_name("a")
        for element in elements:
            list_of_repos.append(element.get_attribute("href"))
    print list_of_repos

    #loop through list of repos to get tags
    links = []
    repo_tags_links = [[] for x in xrange(len(list_of_repos))]
    for repo in list_of_repos:
        #get the repo html page
        f.write(repo)
        f.write('\n\n')
        browser.get(repo)
        tags = browser.find_elements_by_xpath("//h1[@id='supported-tags-and-respective-dockerfile-links']")
        for t in tags:
            setoflinks = t.find_element_by_xpath("//ul")
            #print "printing web element as text: "
            #print setoflinks.text
            number_of_tags = setoflinks.text.count("\n") + 1
            #print number_of_tags
            with open('repos_tags.csv', 'a') as csvfile:
                tags_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                tags_writer.writerow([repo,number_of_tags])
            f.write(setoflinks.text)
            f.write('\n\n\n\n')

f.close()
sleep(5)
browser.quit()
