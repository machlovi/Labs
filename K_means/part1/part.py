from __future__ import print_function
from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator
import sys



if __name__ == '__main__':


    spark = SparkSession.builder.appName('K_Mean').getOrCreate()
   
    df=spark.read.csv(sys.argv[1],header=True,inferSchema=True).select(['player_name','SHOT_CLOCK','CLOSE_DEF_DIST','SHOT_DIST','SHOT_RESULT'])

    
    players = ['stephen curry', 'james harden', 'chris paul','lebron james']
    df1=df.dropna().replace(['made', 'missed'],['1','0'],'SHOT_RESULT')

    assembler=VectorAssembler(inputCols=['SHOT_CLOCK','CLOSE_DEF_DIST','SHOT_DIST'],outputCol='features')

    data=assembler.transform(df1)
    K=4
    kmeans = KMeans(k=K, seed=1)  # 4 clusters here

    model = kmeans.fit(data.select('features'))

    centers = model.clusterCenters()

    most_fea=[]


    df_players=df.filter((df.player_name).isin (players)).dropna().replace(['made', 'missed'],['1','0'],'SHOT_RESULT')

    for player in players:

        df_player=df_players.where((df.player_name)==player)

        newpred=model.transform(assembler.transform(df_player))
        temp=np.zeros([K])

        for i in range(0,K):

            
            x=newpred.where(newpred.prediction==i)
            # x.show(4)
            x_count=x.where(x.prediction==i).count()

            y=x.where(newpred.SHOT_RESULT==1).count()

            # print(x_count/y)
            temp[i]=float(y/x_count)
            # temp.append(float(x_count/y))


        cluster=np.argmax(temp,axis=0)

        center=centers[cluster]
        
        most_fea.append("For " + player + " cluster : " + str(cluster)  + " is most feasble having fear score of "+ str(temp[cluster]) +" with centre: "+ str(center) )


    for i in most_fea:
        print("*"*(len(i)))
        print(i)
    


    spark.stop()