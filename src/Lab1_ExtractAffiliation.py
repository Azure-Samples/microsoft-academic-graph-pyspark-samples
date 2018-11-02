from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Microsoft academic graph spark Labs")\
        .getOrCreate()
    sqlContext = SQLContext(spark)

    df = sqlContext.read.format('csv').\
         option("delimiter", "\t").\
         options(header='false', inferSchema='false').\
         load('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/Affiliations.txt')

    headers = ['AffiliationId', 'Rank', 'NormalizedName', \
               'DisplayName', 'GridId', 'OfficialPage', 'WikiPage', \
               'PaperCount', 'CitationCount', 'CreatedDate']
    df = df.toDF(*headers)

    # Have done: Start the Spark context, Load the Data and insert the schema as instructed
    # Extract the AffiliationId for Microsoft
    microsoft = df.where(df.NormalizedName == 'microsoft').select(df.AffiliationId, df.DisplayName)
    # Optional: peek the result
    microsoft.show()
    # Optional: Count how many rows in the result
    print("Number of rows in the dataframe: {}".format(microsoft.count()))
    microsoft.write.csv('/output/user01/pyspark/Affiliation.csv', \
                        mode='overwrite', header='true')
    # Stop the Spark context

    spark.stop()