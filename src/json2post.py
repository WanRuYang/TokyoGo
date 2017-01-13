from __future__ import print_function, division 
import os
import numpy as np
import pandas as pd
import re
from pymongo import MongoClient
import psycopg2
import cPickle as Pickle
import time
from datetime import datetime

# Database connection
def mongo_instance(dbname='citi', collection_name='venue_api'):
    '''
    Create mongodb collection instance 
    '''
    connection = MongoClient('localhost', 27017)
    db = connection[dbname]
    collection = db[collection_name]
    return collection

def showTipContent():
    """
    Explorer tips conent and manually pick info to store in postgresql table
    """
    venTipsMG = mongo_instance(dbname='citi', collection_name='venue_tips')
    tipsfound = venTipsMG.find({'_id':{'$in':inBound}})[20:22]  # Get20: 22 entries as examples

    for tp in tipsfound:
        print ('===============', tp['_id'], '===============')
        if tp['count'] > 0:
            for i, tip in enumerate(tp['tips'][0:2]):
                print ('* Current tip order',i,'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                keys = tip.keys()
                print ('user: {:>15}'.format(tip['user']['id']))
                print ('createdAt: {:>12}'.format(tip['createdAt']))
                print ('logview: {:>5}'.format(tip['logView']))
                print ('Num likes: {:>3}'.format(tip['likes']['count']))
                print ('tip_id: {:>29}'.format(tip['id']))
                print ('tip_url: {:>56}'.format(tip['canonicalUrl']))
                print ('Num saves:   {:>16}'.format(tip['saves']))
                print ('tip_tpe: {:>8}'.format(tip['type']))
                print ('agreeCount: {:>2}'.format(tip['agreeCount']))
                print ('disagreeCnt: {:>}'.format(tip['disagreeCount']))
                print ('text:       ', (tip['text']))
                if 'lang' in keys:
                    print ('lang: {:>16}'.format(tip['lang']))
                else:
                    print ('lang: {:>11}'.format(None))
                if 'justification' in keys:
                    print ('justification:', tip['justification'])
                else:
                    print ('justification:', None)

                if 'authorInteractionType' in keys:
                    print ('authorInteractionType:{}'.format(tip['authorInteractionType']))
                else:
                    print ('liked', None)
                if 'entities' in keys:
                    print (display(tip['entities']))
                else:
                    print ('entities', None)
                if 'photo' in keys:
                    print (tip['photo'])

def getTipsContent():
    """
    Store tips content to postgresql table
    """
    venTipsMG = mongo_instance(dbname='citi', collection_name='venue_tips')
    tipsfouind = venTipsMG.find({'_id':{'$in':inBound}})

    users_tip_url = set()
    rows= list()
    for tp in tipsfound:
        vid = tp['_id']
        if 'count' not in tp.keys():
            rows.append((vid, None, None, None, None,None, None, None, None,None, None, None, None,None, None, None, None,))
    
        elif 'count' in tp.keys() and tp['count'] > 0:
            for tip in tp['tips']:
                users_tip_url.update([tip['user']['canonicalUrl']])
                keys = tip.keys()
                entities = list()
                if 'lang' in keys:
                    lang = tip['lang']
                else:
                    lang = "NA"
            
                if 'justification' in keys: 
                    justification = tip['justification']['message']
                else:
                    justification = 'NA'
                
                if 'authorInteractionType' in keys:
                    authorInteractionType = tip['authorInteractionType']
                else:
                    authorInteractionType = 'NA'
            
                if 'entities' in keys and len(tip['entities'])>0:
                    for ent in tip['entities']:
                        try:
                            entities.append(ent['text'])
                        except:
                            pass
                else:
                    entities = None
            
                if 'photo' in keys:
                    hasphoto = True
                else:
                    hasphoto = False 
                
                rows.append((
                        vid,
                        tip['user']['id'],
                        datetime.fromtimestamp(tip['createdAt']),
                        lang,
                        justification,
                        authorInteractionType,
                        tip['id'],
                        tip['type'],            
                        tip['logView'],
                        tip['likes']['count'],
                        tip['saves']['count'],
                        tip['canonicalUrl'],
                        tip['agreeCount'],
                        tip['disagreeCount'],
                        tip['text'],
                        hasphoto,
                        entities 
                        ))                
        # print (len(list(users_tip)))
        # with open('userList_urlNov26_tips.p', 'wb') as pf:
        #     Pickle.dump(list(users_tip), pf)
        print ("# of rows:{:>6}".format(len(rows)))

    pg_conn = psycopg2.connect(database='citi', user='postgres', host='localhost', port='5432')
    pg_cur = pg_conn.cursor()
    com_DropTb = """DROP TABLE IF EXISTS tips;"""
    com_CreateTb = """
                CREATE TABLE tips(
                vid char(24) NOT NULL, 
                sid int,
                time timestamp,
                lang text,
                justification text,
                authorInteractionType text,
                tid text,
                t_type text,            
                logView text,
                likes int,
                saves int,
                url text,
                agreeCount int,
                disagreeCount int,
                content text,
                hasphoto boolean,
                entities text
                );
            """
    pg_cur.execute( com_DropTb ) 
    pg_cur.execute( com_CreateTb )
    args_str = ','.join(pg_cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in rows)
    pg_cur.execute("INSERT INTO tips VALUES " + args_str + ";") 
    pg_cur.execute( 'CREATE INDEX vid_index ON tips (vid);')
    # with open('langProp.sql', 'r') as f:
    #     sq = f.read()
    # print (sq)
    # pg_cur.execute(sq)
    pg_conn.commit()
    pg_conn.close()


def main():
    getTipsContent()

if __name__ == "__main__":
    man()
