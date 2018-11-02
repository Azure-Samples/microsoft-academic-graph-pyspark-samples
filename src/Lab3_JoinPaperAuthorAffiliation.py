from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("academic graph spart test")\
        .getOrCreate()
    sqlContext = SQLContext(spark)

    affiliations = sqlContext.read.format('csv').\
         option("delimiter", ",").\
         options(header='true', inferSchema='true').\
         load('/output/user01/pyspark/Affiliation.csv')

    paperAuthorAffiliations = sqlContext.read.format('csv').\
         option("delimiter", "\t").\
         options(header='false', inferSchema='false').\
         load('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/PaperAuthorAffiliations.txt')
    paperAuthorAffiliationHeaders = ['PaperId', 'AuthorId', 'AffiliationId', 'AuthorSequenceNumber', 'OriginalAffiliation']
    paperAuthorAffiliations = paperAuthorAffiliations.toDF(*paperAuthorAffiliationHeaders)

    orgPaperAuthorAffiliation = paperAuthorAffiliations\
                                .join(affiliations, paperAuthorAffiliations.AffiliationId == affiliations.AffiliationId, 'inner')\
                                .select(paperAuthorAffiliations.PaperId, paperAuthorAffiliations.AuthorId, \
                                        affiliations.AffiliationId, paperAuthorAffiliations.AuthorSequenceNumber)
    orgPaperAuthorAffiliation.show()
    print('Number of rows in PaperAuthorAffiliation: {}'.format(orgPaperAuthorAffiliation.count()))
    orgPaperAuthorAffiliation.write.csv('/output/user01/pyspark/paperAuthorAffiliationRelationship.csv', \
                                         mode='overwrite', \
                                         header='true')

    spark.stop()