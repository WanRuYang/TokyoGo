from  __future__ import division 
import logging
import os 
import re
import json
import numpy as np
import pandas as pd
import geopy
from geopy.distance import vincenty
import requests
import simplekml
from bs4 import BeautifulSoup
from pymongo import MongoClient
import cPickle as Pickle
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import socket

"""
Retrieve data from Foursquare API and use the venues' info to search url for web scraping

"""

def mongo_instance(dbname, collection_name):
    '''
    Create mongodb collection instance 
    '''
    connection = MongoClient(waitQueueTimeoutMS=90)
    db = connection[dbname]
    collection = db[collection_name]
    return collection

def cate(url='https://api.foursquare.com/v2/venues/categories?oauth_token=test&v=20161001'):
    cateList = requests.get(url)
    mgcon = mongo_instance( 'citi', 'categories')
    mgcon.insert_one(json.loads(cateList.text))
    return json.loads(cateList.text) 

#Required: use YYYYMMDD to specify data version (by date)
def get_categories(lst_categoryId='https://developer.foursquare.com/categorytree'):
    '''
    Do not remove categories for the first dataset 
    Request the content of the webpage and use BeautifulSoup to parse the result 
    INPUT: url:string
    OUTPUT: name:id:dict
    '''
    category_get = requests.get(lst_categoryId)
    categoryId_html = BeautifulSoup(category_get.text, 'html.parser')
    
    class_name = categoryId_html.find_all("div", class_="name")
    class_id = categoryId_html.find_all("div", class_="id")
    assert(len(class_name) == len(class_id))
   
    strip_fun = lambda x: x.string.encode('utf-8', 'ignore').strip() 
    keys = map(strip_fun, class_name)
    vals = map(strip_fun, [x.find('tt') for x in class_id] )
    idx = range(len(vals))
    categories = zip(idx, keys, vals)
    for c in categories:
        print 'c', c
    filters = range(49,59)+range(64,102)+\
                range(113,497)+range(551,560)+range(567,581)+range(584,591)+\
                range(592,646)+range(658,663)+range(664,730)+range(731,879)+\
                range(545,549)+[0,16,17,30,61,105,111,519,512,582,654,883,884,885,505]

    categories = [x for x in categories if x[0] not in filters]
    output = [x[2] for x in categories]
    for c in categories:
        print 'f', c

    with open('../data/categories_list.p', 'wb') as f:
        Pickle.dump(output, f)
    return categories 

def set_search_centers(p0= geopy.Point(35.682851, 139.996418)):
    '''#, 36.186731, 139.966485
    INPUT: geopy.Point(Latitude, Longitude)
    OUPUT: (save to pickle and kml file)
    ----
    p1 in lat,lng
    # 0.2km for 50km to the west
    # 1 km from 50km ~ 180km to the west
    # 135 km to the south 
    '''
    # Create North-South reference points 
    d_ns = vincenty(kilometers = 1)
    ns = list()
    for i in xrange(int(128/1)):
        temp = d_ns.destination(point=p0, bearing=180)
        ns.append(temp)
        p0=temp

    d_ew_1 = vincenty(kilometers = 0.75)
    d_ew_2 = vincenty(kilometers = 2.5)
    centers = list()

    for pt in ns:
        # Append current E-W serach init pt to result list 
        centers.append((pt.latitude, pt.longitude))
        # Copy the init point
        pA = pt
        for _ in xrange(int(60/0.75)):                               
            # Find the next pt
            tempA = d_ew_1.destination(point=pA, bearing=270)
            # Append new pt
            centers.append((tempA.latitude, tempA.longitude))
            # Use the new pt to serach for the next one
            pA = tempA
            # After 60 km west from starting line, loose the center distance
        pB = tempA
        for _ in xrange(0, 40):
            tempB = d_ew_2.destination(point=pB, bearing=270)
            centers.append((tempB.latitude, tempB.longitude))
            pB = tempB
    # Save to Pickle 
    with open('../data/search_centers_sou.p', 'wb') as pfile:
        Pickle.dump(centers ,pfile)

    # Save to kml map
    kml = simplekml.Kml()
    for row in centers:
        kml.newpoint(coords=['longitude', 'latitude']+[(row[1],row[0])])
    kml.save('../data/search_centers_sou.kml')


def pt2kml(pts, filename):
    # Save to kml map
    kml = simplekml.Kml()
    for row in pts:
	   kml.newpoint(coords=['longitude', 'latitude']+[(row[1],row[0])])
    kml.save('../data/'+filename)

def venus2kml(dbname='citi', collection_name = 'venue_web'):
    coln = mongo_instance(dbname,collection_name)
    data = coln.find({"venue.location.lat":{"$gt": 35.438922, "$lt": 35.917717}})
    pts = [(pt['venue']['location']['lat'], pt['venue']['location']['lng'] ) for pt in data]
    filename = '../data/venue_' + ''.join([str(x) for x in time.gmtime()][0:3]) + '.kml'
    pt2kml(pts, filename)

## Create a list of Lat and Lng for searchig venues
def getVenue1_api(search_centers, db='citi', coln='venue_api', id_field='id', date_api = 20161001):
    '''
    Get Venues by location, extent defined by ne and sw coordinates of the area of interest 
    INPUT: lat:float; lng:float; search_centers: list
    OUTPUT: #TODO INSERT TO POSTGRESQL DATABASE 
    '''
    FS_ID = os.getenv('FOURSQUARE_CLIENT_ID')
    FS_PW = os.getenv('FOURSQUARE_SECRET')
    print FS_ID
    print FS_PW

    mg = mongo_instance(db, coln) 
    venue_ids = set(mg.distinct('_id'))

    for idx, center in enumerate(search_centers):
        print 'CURRENT', idx
        Lat = center[0]
        Lng = center[1]
        url = 'https://api.foursquare.com/v2/venues/search?ll={0},{1}&client_id={2}&client_secret={3}&v={4}'.\
                format(Lat, Lng, FS_ID, FS_PW, date_api)
        print 'url', url
        r = requests.get(url)
        data_json = json.loads( r.text )
        print data_json
        venues_received = data_json['response']['venues']
        
        new_data = [ r for r in venues_received if r['id'] not in venue_ids ]
        print '# of dup records removed:{}'.format(len(venues_received) - len(new_data))
         
        with open('../data/fs_venue_api.json', 'a') as f:
            json.dump(new_data, f)

        # Set _id field for each recored         
        if len(new_data) > 0:
            venue_ids.update( [x['id'] for x in new_data] )
            id_field = 'id'
            for record in new_data:
                record['_id'] = record[id_field] 
            mg.insert_many(new_data) 
    
    return data_json

def getVenue2_Web( db='citi', coln_api='venue_api', coln_web='venue_web', ec2=False):
    '''
    <script type="text/javascript">fourSq.queue.push(function() {fourSq.venue2.desktop.VenueDetailPage.init({relatedVenuesResponse:
    get user name from the photo page 
    ------------------------------place name-----------------------place id------------------------
    # https://foursquare.com/v/%E9%87%8C%E8%A6%8B%E5%85%AC%E5%9C%92/4b80a633f964a520888330e3/photos
    '''
    # Get the categories list to filter out unwanted record
    lst_cat=Pickle.load(open('../data/categories_list.p', 'r'))

    # Extract venue name and id from mongodb api table; filter by categories of interest
    coln_api_conn  = mongo_instance(db, coln_api)
    
   # Retrive venue name and id to construct web url; within categories of interest only 
    cur_api = coln_api_conn.find( {"categories.id": { "$in": lst_cat}} ,  { "name": 1, "id": 1 })   
    
    # # Create a webdriver on local mac
    if ec2==False:
        chrome_options = Options()
        chrome_options.add_argument('--dns-prefetch-disable')
        mydriver = webdriver.Chrome('/Users/zuya/Documents/galvanize/project/pkgs/chromedriver',\
                                chrome_options=chrome_options)
    else:
        # Create a webdriver on ec2
        display = Display(visible=0, size=(800, 600))
        display.start()
        chrome_options = Options()
        chrome_options.add_argument('--dns-prefetch-disable')
        mydriver = webdriver.Chrome(chrome_options=chrome_options)
    
    mydriver.set_page_load_timeout(3000)
    # Create a log file to write the exception of js execute 
    logging.basicConfig(level=logging.DEBUG, filename='photosErr.log')

    mg = mongo_instance(db, coln_web)  
    # Check unique id to decide if insert to database 
    existed_id = mg.find( {}, {"_id":1})
    id_checkLst = set([ele['_id'] for ele in existed_id])
    id_fieldA = 'venue'
    id_fieldB = 'id'

    print 'id check list', id_checkLst
    # Construct url with venue name and id retrieve from mongodb; insert into another db
    for rec in list(cur_api):
        url = '/'.join(['https://foursquare.com/v', rec['name'], rec['id'], 'photos'])  
        if rec['id'] not in id_checkLst:
            mydriver.get(url)

            try:
                mydriver.find_element_by_class_name('startAutoLoad').click()
            except Exception:
                logging.exception("No extra data to load:", url)
                pass

            mydriver.set_page_load_timeout(3000)
            elems = mydriver.find_elements_by_tag_name('script')

            for script in elems:
                if script.get_attribute('type') == 'text/javascript':
                    js_0 = mydriver.execute_script("return arguments[0].innerHTML", script)
                    
                    if 'fourSq.queue.push' in js_0:
                        js = re.search('\{relatedVenuesResponse.+enablePhotoBoosters: \w+\}', js_0).group()
                        data = mydriver.execute_script('return ' + js)
                                               
                        if data[id_fieldA][id_fieldB] not in id_checkLst:
                            data['_id'] = data[id_fieldA][id_fieldB]   
                            mg.insert_one(data)
                            # Update the id checkLst set
                            id_checkLst.update(data['_id'])
        else:
            continue 


def getUrl_venues(  db='citi', coln_api='venue_api', coln_web='venue_web' ):
    # Get the categories list to filter out unwanted record
    lst_cat=Pickle.load(open('../data/categories_list.p', 'r'))

    # Extract venue name and id from mongodb api table; filter by categories of interest
    coln_api_conn  = mongo_instance(db, coln_api)
    coln_web_conn  = mongo_instance(db, coln_web)
    
   # Retrive venue name and id to construct web url; within categories of interest only 
    cur_api = coln_api_conn.find( {"categories.id": { "$in": lst_cat}} ,  { "name": 1, "id": 1 })  
    cur_web = coln_web_conn.find( {"venue.categories.id": { "$in": lst_cat}} ,  { "venue.name": 1, "id": 1 })  
    print cur_api[0]
    print cur_web[0]

    newlist = [ item for item in cur_api if item['_id'] not in [x['_id'] for x in cur_web] ]
    return newlist

def getVenue_url(mdriver, url):
   
    browser.get(url)

    try:
        browser.find_element_by_class_name('startAutoLoad').click()
    except Exception:
        pass

    mg = mongo_instance('citi', 'venue_web')  
    elems = browser.find_elements_by_tag_name('script')

    for script in elems:
        if script.get_attribute('type') == 'text/javascript':
            js_0 = browser.execute_script("return arguments[0].innerHTML", script)
            
            if 'fourSq.queue.push' in js_0:
                js = re.search('\{relatedVenuesResponse.+enablePhotoBoosters: \w+\}', js_0).group()
                data = browser.execute_script('return ' + js)                       
                data['_id'] = data['venue']['id']   
                mg.insert_one(data)


def getVenue_byList(listUrl = 'https://foursquare.com/shloshahshalosh/list/%E6%9D%B1%E4%BA%AC%E3%81%AE%E5%85%AC%E5%9C%9250'):

    mg = mongo_instance('citi', 'venue_web')  
    idsearch = mg.find({},{'_id':1})
    idsfind = [i['_id'] for i in idsearch]

    chrome_options = Options()
    chrome_options.add_argument('--dns-prefetch-disable')
    mydriver = webdriver.Chrome('/Users/zuya/Documents/galvanize/project/pkgs/chromedriver',\
                                chrome_options=chrome_options)    
    mydriver.set_page_load_timeout(300)
    mydriver.get(listUrl)

    vclass = mydriver.find_elements_by_css_selector('.venueLink')
    vclass = list(set([v.get_attribute('href') for v in vclass]))

    for url in vclass:
        if re.sub('http.+/', '', url) not in idsfind:
            print url
            getVenue_url(mydriver, url)

def getVenue2_Web_url( db='citi', coln_web='venue_web', platform = 'mac'):
    
    # Create a webdriver on mac
    if platform=='mac':
        chrome_options = Options()
        chrome_options.add_argument('--dns-prefetch-disable')
        mydriver = webdriver.Chrome('/Users/zuya/Documents/galvanize/project/pkgs/chromedriver',\
                                chrome_options=chrome_options)
    else:
        # Create a webdriver on ec2
        display = Display(visible=0, size=(800, 600))
        display.start()
        chrome_options = Options()
        chrome_options.add_argument('--dns-prefetch-disable')
        mydriver = webdriver.Chrome(chrome_options=chrome_options)
        mydriver.set_page_load_timeout(3000)
        # Create a log file to write the exception of js execute 
        # logging.basicConfig(level=logging.DEBUG, filename='photosErr.log')

    id_fieldA = 'venue'
    id_fieldB = 'id'
    
    mg = mongo_instance(db, coln_web)  
    existed_id = mg.find( {}, {"_id":1})
    id_checkLst = set([ele['_id'] for ele in existed_id])
    # Construct url with venue name and id retrieve from mongodb; insert into another db
    for rec in curs:
        url = '/'.join(['https://foursquare.com/v', rec['name'], rec['id'], 'photos'])  
        if rec['id'] not in id_checkLst:
            mydriver.get(url)

            try:
                mydriver.find_element_by_class_name('startAutoLoad').click()
            except Exception:
                # logging.exception("No extra data to load:", url)
                pass

            mydriver.set_page_load_timeout(3000)
            elems = mydriver.find_elements_by_tag_name('script')

            for script in elems:
                if script.get_attribute('type') == 'text/javascript':
                    js_0 = mydriver.execute_script("return arguments[0].innerHTML", script)
                    print js_0
                    #if 'fourSq.queue.push' in js_0:
                        #js = re.search('\{relatedVenuesResponse.+enablePhotoBoosters: \w+\}', js_0).group()
                        #data = mydriver.execute_script('return ' + js)
                                              
                        #data['_id'] = data[id_fieldA][id_fieldB]   
                        # mg.insert_one(data)
                        # # Update the id checkLst set
                        # id_checkLst.update(data['_id'])


def getUser_Web( db='citi', coln_user='user_web', coln_web='venue_web', platform='mac'):
    # Extract venue name and id from mongodb api table; filter by categories of interest
    colni_web_conn  = mongo_instance(db, coln_web)
    cur_photos= coln_web_conn.find({}) 
    user_urls = list()


    if platform=='mac':
        # Create a webdriver on local mac
        chrome_options = Options()
        chrome_options.add_argument('--dns-prefetch-disable')
        mydriver = webdriver.Chrome('/Users/zuya/Documents/galvanize/project/pkgs/chromedriver',\
                                    chrome_options=chrome_options)

    else:
         #Create a webdriver on ec2
         display = Display(visible=0, size=(800, 600))
         display.start()
         chrome_options = Options()
         chrome_options.add_argument('--dns-prefetch-disable')
         mydriver = webdriver.Chrome(chrome_options=chrome_options)
   
    mydriver.set_page_load_timeout(3000)

    mg_user = mongo_instance(db, coln_user)
    for p in list(cur_photos):
        # print p['venue']['tips']#['count']
        # print p['venue']['tips']['groups']#['items']
        # print 'tips count', p['venue']['tips']['count']
        if len(p['venue']['tips']['groups'])>0:
            for record in p['venue']['tips']['groups']:
                # print p['venue']['tips']['count']#['count']
                # print record.keys()
                # print record['name']
                for item in record['items']:
                    # print 'tip items', item['user']['canonicalUrl']
                    url = item['user']['canonicalUrl']
                    # print url

                    mydriver.get(url)
                    elems = mydriver.find_elements_by_tag_name('script')
      
                    for script in elems:
                        existed_id = mg_user.find( {}, {"_id":1}) # todo add this 
                        id_checkLst = set([ele['_id'] for ele in existed_id])
                        if script.get_attribute('type') == 'text/javascript':
                            js_0 = mydriver.execute_script("return arguments[0].innerHTML", script) 
                            # js_0 = js_0.encode('utf-8', 'ignore').strip()
                            if 'fourSq.userprofile2.desktop.UserProfile2Page.init' in js_0:
                                js = re.search('\{user\: \{"lists":.+useTipUpvotes: \w+\}', js_0).group()
                                data = mydriver.execute_script('return ' + js)
                                if data['user']['id'] not in id_checkLst:
                                    print data['user']['id']
                                    data['_id'] = data['user']['id']
                                    mg_user.insert_one(data)
                                    print 'insert one',  data['user']['id']
    for p in list(cur_photos):
        if len(p['photos']['items'])>0:
            url = p['photos']['items'][0]['user']['canonicalUrl']
            mydriver.get(url)
            elems = mydriver.find_elements_by_tag_name('script')

            for script in elems:
                existed_id = mg_user.find( {}, {"_id":1}) # todo add this 
                id_checkLst = set([ele['_id'] for ele in existed_id])
                if script.get_attribute('type') == 'text/javascript':
                    js_0 = mydriver.execute_script("return arguments[0].innerHTML", script) 
                    # js_0 = js_0.encode('utf-8', 'ignore').strip()
                    if 'fourSq.userprofile2.desktop.UserProfile2Page.init' in js_0:
                        js = re.search('\{user\: \{"lists":.+useTipUpvotes: \w+\}', js_0).group()
                        data = mydriver.execute_script('return ' + js)
                        if data['user']['id'] not in id_checkLst:
                            print data['user']['id']
                            data['_id'] = data['user']['id']
                            mg_user.insert_one(data)
                            print 'insert one',  data['user']['id']
                                                         
    return 



if __name__ == "__main__":
    # Create a list of search starting points; each will return a (lat, lng) pair
    # Each api requests retruns 30 venues
    # set_search_centers()
    # lst = get_categories()
    # for i, x in enumerate(lst):
        # print i, x
    #with open('../data/search_centers_sou.p', 'r') as pfile:
    #    cent = Pickle.load(pfile)
    # print len(cent)
    # cng = [range(0,5000), range(5000,10000)]
    # print len(cent)
    # print cent[0:50] #5000-10000, 15000+4210:20000, 20000:
   
    #x = cate()
    #getVenue1_api(cent[4999:10000])
    # time.sleep(60)
    # getVenue1_api(cent[5000:9000])
    
    getVenue2_Web()
    # getUser_Web()
    # venus2kml()
    # getUrl_venues()
    # getVenue2_Web_url( )
    # getVenue_url()
    # getVenue_byList
    #get_categories()
