from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import sys

if __name__ == '__main__':

    

    spark=SparkSession.builder.appName('K_mean_part2').getOrCreate()
    df=spark.read.csv(sys.argv[1], inferSchema=True, header=True).select(['Street Code1','Street Code2','Street Code3','Vehicle Color'])

    assembler=VectorAssembler(inputCols=['Street Code1','Street Code2','Street Code3'],outputCol='features')

    data=assembler.transform(df)

    # Setup KMeans model and train
    model=KMeans(featuresCol='features',k=3).fit(data)
    prediction=model.transform(data)

    # out of sample data
    newdf=spark.createDataFrame([(34510, 10030, 34050,'BLK')],
                                  ('Street Code1','Street Code2','Street Code3','Vehicle Color'))

    newpred=model.transform(assembler.transform(newdf))

    # cluster of the out of sample data and its color
    cluster=newpred.select('prediction').collect()[0].prediction
    color=newpred.select('Vehicle Color').collect()[0]['Vehicle Color']

    colors=prediction.filter(prediction['prediction']==cluster).select('Vehicle Color').collect()

    prob=sum(list(map(lambda row: row['Vehicle Color']==color, colors)))/len(colors)

    result='K=' + str(sys.argv[2]) + ' ' +'clusterNo.=' + str(cluster) + ' ' +'ProbOfTicketed=' + str(round(prob, 4)) + '\n'
    print(result)


    spark.stop()

