import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient
import re
import json
from pyvirtualdisplay import Display
import cPickle as Pickle

'''
Scrap 'TIPS' page from Foursquare website
'''

def mongo_instance(dbname, collection_name):
    '''
    Create mongodb collection instance 
    '''
    connection = MongoClient(waitQueueTimeoutMS=90)
    db = connection[dbname]
    collection = db[collection_name]
    return collection

def getVenueTips( url, dic, browser ):
    '''
    Query the tips page recursively 
    '''	
    browser.get(url)
    script = browser.find_element_by_xpath("//*[@class='contents']/following-sibling::script")
    
    js_0 = script.get_attribute('innerText')
    js = re.search('\{relatedVenuesResponse.+enablePhotoBoosters: \w+\}', js_0).group()
    data = browser.execute_script('return ' + js)

    dic['_id'] = data['venue']['id'] 
    #print '============', data['venue']['id']
    dic['tips'].extend( data['tips']['items'] )
    dic['count'] = data['tips']['count']

    try:
        elem_next = browser.find_element_by_css_selector('link[rel="next"]') 
	url = elem_next.get_attribute('href')
	return getVenueTips( url, dic, browser )
    except:
        return 

def run_getVenueTips( db='citi', coln_web='venue_web', coln_tip='venue_tips', platform = 'mac' ):
	if platform == 'mac':
		chrome_options = Options()
		#chrome_options.add_argument('--no-startup-window')
		#chrome_options.add_argument('--dns-prefetch-disable')
		mydriver = webdriver.Chrome('/Users/zuya/Documents/galvanize/project/pkgs/chromedriver',\
		                            chrome_options=chrome_options)

	elif platform == 'linux':
		# Create a webdriver on ec2
		display = Display(visible=0, size=(800, 600))
		display.start()
		chrome_options = Options()
		chrome_options.add_argument('--dns-prefetch-disable')
		mydriver = webdriver.Chrome(chrome_options=chrome_options)
	
	mydriver.set_page_load_timeout(300)

	venuesBox = Pickle.load(open('../data/inBound.p', 'r'))
	v_cur = mongo_instance(db, coln_web)  #35.91, 35.44, 138.92
	venues = v_cur.find( {'_id':{'$in':venuesBox}},  no_cursor_timeout=True)[1000:]
	
	tips_cur = mongo_instance(db, coln_tip)
	id_checkList = tips_cur.find({}, {'_id':1})
	checkList = [x['_id'] for x in id_checkList] + ['57061788498e72afd7df6fc6', '5518fd9a498e4a598e487516', '58292ba34c6d67719e3a666a', '4e9113b593adc15b6254815d']
	#print '4b243a7df964a520356424e3' in checkList
	#print checkList[0:5]
	for venue in venues:
            if venue['venue']['id'] not in checkList:
   	        url = venue['venue']['canonicalUrl'] + '?tipsSort=recent'
		#print url
		print 'Current venue id:{:>8}'.format(venue['venue']['id'])
		tmp = {'_id': venue['venue']['id'], 'tips':[], 'tipsCount':None}
		getVenueTips(url, tmp, mydriver)
		try:
                    tips_cur.insert_one( tmp )
		except:
	            pass
if __name__ == "__main__":
	run_getVenueTips()
