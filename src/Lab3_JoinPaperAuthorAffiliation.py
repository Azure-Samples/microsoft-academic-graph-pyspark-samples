from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

from CreatePySparkFunctions import *

# Replace containerName and accountName
containerName = "myContainerName"
accountName = "myAccountName"

outputDir = "/output/user01/pyspark"

if __name__ == "__main__":

    # Start Spark context
    spark = SparkSession \
        .builder \
        .appName("Microsoft academic graph spark Labs") \
        .getOrCreate()
    sqlContext = SQLContext(spark)

    # Load Affiliation data
    affiliations = sqlContext.read.format('csv') \
        .option("delimiter", ",") \
        .options(header='true', inferSchema='true') \
        .load('%s/Affiliation.csv' % outputDir)

    # Load PaperAuthorAffiliations data
    paperAuthorAffiliations = getDataFrameForPaperAuthorAffiliations(sqlContext, containerName, accountName)
    # Optional: peek result
    paperAuthorAffiliations.show()

    orgPaperAuthorAffiliation = paperAuthorAffiliations \
        .join(affiliations, paperAuthorAffiliations.AffiliationId == affiliations.AffiliationId, 'inner') \
        .select(paperAuthorAffiliations.PaperId, paperAuthorAffiliations.AuthorId, \
                affiliations.AffiliationId, paperAuthorAffiliations.AuthorSequenceNumber)

    # Optional: peek result
    orgPaperAuthorAffiliation.show()

    # Optional: Count number of rows in result
    print('Number of rows in PaperAuthorAffiliation: {}'.format(orgPaperAuthorAffiliation.count()))

    # Output result
    orgPaperAuthorAffiliation.write.csv('%s/PaperAuthorAffiliationRelationship.csv' % outputDir, mode='overwrite', header='true')

    # Stop Spark context
    spark.stop()
