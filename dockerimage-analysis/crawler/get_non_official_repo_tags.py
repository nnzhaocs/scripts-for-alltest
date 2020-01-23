from time import sleep
from selenium import webdriver
import datetime
import json

#settings for using a chrome executable in a non-standard location
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
browser = webdriver.Chrome('/usr/local/bin/chromedriver',chrome_options=chrome_options)


data = {}
page_number = 0
list_of_repos = []


#url of docker hub's explore page
url = 'https://hub.docker.com/search/?q=&type=image&page=%d'
while page_number < 100: #maximum number of accessible pages is 100  
    page_number = page_number + 1
    print "Crawling page: " + str(page_number) + " -->   url: " + url % page_number
    browser.get(url % page_number)

    #getting repos links from explore pages
    print "     Getting links to repos..."
    repos = browser.find_elements_by_xpath("//div[@class='styles__resultsWrapper___38JCx']")
    for repo in repos:
        for element in repo.find_elements_by_tag_name('a'):
            #for every page, we are adding links to repos in list_of_repos
            if '/r/' in element.get_attribute('href'):
			    list_of_repos.append(element.get_attribute('href'))
				#print element.get_attribute('href')
    #print list_of_repos

    #loop through list of repos to get tags
    print "     Looping through the repos to get tags..."
    for repo in list_of_repos:
		#get the repo html page
		data[repo] = []
		non_official_tags_page = '/tags'
		browser.get(repo+non_official_tags_page)
		for elem in browser.find_elements_by_xpath("//span[@class='styles__tagName___bE6Eb']"):
			data[repo].append({'tag': elem.text})
			#print elem.text
			#print "\n"
			
print "=============================================="
print "saving results to non_official_repos_tags.json"
print "=============================================="
with open('non_official_repos_tags.json','w') as outfile:
    json.dump(data, outfile)
print "DONE"

sleep(5)
browser.quit()

		
