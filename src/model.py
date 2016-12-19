from __future__ import division, print_function 
import graphlab
import psycopg2
import pandas as pd
import numpy as np
from collections import Counter
from pprint import pprint

class GLmfRecommender(object):
    def __init__(self, pg_conn):
        """
        INPUT: PG connection instance
        Retrieve data from citi db 
        """

        sqlstr = """
                SELECT DISTINCT u.sid, u.vid, liked+pvalue 
                FROM user_rate u 
                INNER JOIN top200 t ON t.vid = u.vid; 
                """
        pg_cur = pg_conn.cursor()

        # Query venue table; skip venues with no likes, lists, tips, and checkins
        colns = ['sid', 'vid', 'rating']
        pg_cur.execute( sqlstr )
        pg_conn.commit()
        df = pd.DataFrame(pg_cur.fetchall(), columns=colns).sort_values('sid')
        df = df[~df.sid.isnull()]

        sqlstr = """SELECT * FROM user_homecity;"""
        colns = ['sid', 'homecity']
        pg_cur.execute( sqlstr )
        pg_conn.commit()
        user_info = pd.DataFrame(pg_cur.fetchall(), columns=colns).sort_values('sid')
        pg_cur.close()
        user_info = user_info[~user_info.sid.isnull()]
        user_info = user_info[user_info.sid.isin(df.sid)]

        # Create graphlab main dataframe and user info dataframe 
        self.sf = graphlab.SFrame(df)
        self.user_sf= graphlab.SFrame(user_info)
    
        # Load pretrained model, will be overwritten if fit() is called
        try:
            unpickler = graphlab._gl_pickle.GLUnpickler(filename = 'glmodel.p') 
            obj_ret = unpickler.load() 
            unpickler.close()
            self.model = obj_ret['model']
   
        except:
            self.model = None

        self.params = dict(user_id='sid',
                  item_id='vid',
                  target='rating',
                  solver='als', side_data_factorization=True)

      
    def parameter_search( self ) :
        """
        INPUT 
        Tune model to find the best parameters
        """
        
        kfolds = graphlab.cross_validation.KFold(self.sf, 5)
        paramsearch = graphlab.model_parameter_search.create(
                        kfolds,
                        graphlab.recommender.factorization_recommender.create,
                        params)

        # Print parameter search result
        print ("best params by recall 5:")
        pprint(paramsearch.get_best_params('mean_validation_recall@5'))
        print ()
        print ("best params by precision 5:")
        pprint(paramsearch.get_best_params('mean_validation_precision@5'))
        print ()
        print ("best params by rmse:")
        pprit(paramsearch.get_best_params('mean_validation_rmse'))
   
        search_summary= parameter_search.get_results()
        return search_summary['validation_rmse'].min()

    def fit( self ):
        """
        INPUT: None
        OUPUT: Model Object

        Matrix factorization recommender trained with top150 venues table 
        (total 442 veneus). 

        """
        # Best RMSE
        self.params.update(dict(user_data=self.user_sf,
                  linear_regularization=1e-05,
                  max_iterations=25,
                  num_factors=64,
                  regularization=1e-09))
         
        self.model = graphlab.recommender.factorization_recommender.create(self.sf, **self.params)
        
        # Setup the GLC pickler
        obj = {'model': self.model}
        pickler = graphlab._gl_pickle.GLPickler(filename = 'glmodel.p')
        pickler.dump(obj)
        pickler.close()

        return self.model

    def recommend( self, homecity, new_obs_data ):
        """
        INPUT: List, Str, Boolean
        OUPUT: DataFrame
        """
        # Add new user info
        new_user_info = graphlab.SFrame({'sid': [1],
                             'homecity': [homecity]})

        # Train model with side info -- new observation
        model_venue_sim = graphlab.item_similarity_recommender.create( self.sf,
                                                            user_id='sid',
                                                            item_id='vid',
                                                            target='rating')
        # Create sf new observation data (user picked images )
        new_obs_data = graphlab.SFrame({'sid' : [1 for x in range(len(new_obs_data))],
                                    'vid' : new_obs_data
                                    })

        # recommend
        recommendations = model_venue_sim.recommend([1], 
                                                new_user_data = new_user_info, 
                                                new_observation_data = new_obs_data,
                                                k=20)
        return recommendations

def weightfunc(x):
        try:
            return weight[x]
        except:
            return 0

def main(pids, homecity, pg_conn):
    """
    INPUT: List
    OUTPUT: List

    Take user input pids, convert to new_obs_data
    """
    # Query photos table 
    pids = map(str, pids)
    if len(pids)==1:
        pids = "('"+ pids[0] + "')"
    else:
        pids = str(tuple(pids))
    print (pids)
    pg_cur = pg_conn.cursor()
    source = {'foreign': 'foreigner', 'japan': 'japanese', 'tokyo': 'tokyoer'}
    homecity = homecity.lower()
    colns = ['vid', 'category', 'foreigner', 'japanese', 'tokyoer', 'cluster']
    sqlStr = """SELECT p.vid, t.category, t.foreigner, t.japanese, t.tokyoer, t.cluster \
                    FROM photos p 
                    INNER JOIN top200 t ON t.vid = p.vid
                    WHERE p.vid IN ( \
                            SELECT DISTINCT s.vid \
                            FROM venues s \
                                 WHERE s.lists !=0 AND s.likes != 0 AND s.checkins !=0 AND s.tips != 0 AND \
                                s.vid NOT IN ('4be4f932d4f7c9b61df92420', '4b0587a6f964a5203e9e22e3')) \
                    AND p.pid IN """ + pids + """ \
                    AND t.""" + source[homecity] + """ = 1;"""

    pg_cur.execute(sqlStr)
    pg_conn.commit()
    df = pd.DataFrame(pg_cur.fetchall(), columns=colns).drop_duplicates()

    colns_top200 = ['vid', 'category', 'foreigner', 'japanese', 'tokyoer', 'cluster']
    pg_cur.execute("""SELECT * FROM top200;""")
    top200 = pd.DataFrame(pg_cur.fetchall(), columns=colns_top200)

    weight_count = Counter(df.category)
    weight = dict(zip(weight_count.keys(), np.array(weight_count.values()) / sum(weight_count.values()) * 10))

    recommendations = GLmfRecommender(pg_conn).recommend( homecity=source[homecity], new_obs_data=df.category ).to_dataframe()
    recommendations = recommendations.merge(top200, on='vid', how='left')
    recommendations['weighted_score'] = recommendations['category'].map(weightfunc) * recommendations['score']
    recommendations.sort_values('weighted_score', inplace=True)

    return list(recommendations.vid)[0:15]

if __name__ == "__main__":
    # runPredict()
    # recommend( homecity, new_obs_data, pretrained=True)
    pg_conn = psycopg2.connect(database='citi', user='postgres', password=os.getenv('PGPW'), host='localhost', port='5432')
    pg_cur = pg_conn.cursor()
    pids = ['553c65b2498e2e281adde15a']
    homecity = 'japan'
    main(pids, homecity, pg_conn)
