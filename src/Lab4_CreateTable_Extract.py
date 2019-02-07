from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import concat, lit, log, when
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

    #Load Authors data
    authors = getDataFrameForAuthors(sqlContext, containerName, accountName)

    #Load PaperAuthorAffiliationRelationship data
    paperAuthorAffiliation = sqlContext.read.format('csv') \
        .option("delimiter", ",") \
        .options(header='true', inferSchema='true') \
        .load('%s/PaperAuthorAffiliationRelationship.csv' % outputDir)

    orgAuthorIds = paperAuthorAffiliation.select(paperAuthorAffiliation.AuthorId).distinct()

    # Get all author details.
    orgAuthors = authors.join(orgAuthorIds, authors.AuthorId == orgAuthorIds.AuthorId, 'inner') \
        .select(orgAuthorIds.AuthorId, authors.DisplayName) \
        .selectExpr('AuthorId as AuthorId', 'DisplayName as AuthorName')

    # Optional: peek result
    orgAuthors.show()

    # Output result
    orgAuthors.write.csv('%s/Author.csv' % outputDir, mode='overwrite', header='true')

    #Load Papers data
    papers = getDataFrameForPapers(sqlContext, containerName, accountName)

    Paper = papers.withColumn('Prefix', lit('https://academic.microsoft.com/#/detail/'))

    # Get all paper details.
    orgPaperIds = paperAuthorAffiliation.select(paperAuthorAffiliation.PaperId).distinct()
    orgPapers = Paper.join(orgPaperIds, Paper.PaperId == orgPaperIds.PaperId) \
        .where(Paper.Year >= 1991) \
        .select(Paper.PaperId, Paper.PaperTitle.alias('Title'), Paper.EstimatedCitation.alias('CitationCount'), \
                Paper.Date, when(Paper.DocType.isNull(), 'Not available').otherwise(Paper.DocType).alias('PublicationType'), \
                log(Paper.Rank).alias('LogProb'), concat(Paper.Prefix, Paper.PaperId).alias('Url'), \
                when(Paper.ConferenceSeriesId.isNull(), Paper.JournalId).otherwise(Paper.ConferenceSeriesId).alias('VId'), \
                Paper.Year)

    # Optional: peek result
    orgPapers.show()

    # Optional: Count number of rows in result
    print('Number of rows in orgPapers: {}'.format(orgPapers.count()))

    # Output result
    orgPapers.write.csv('%s/Paper.csv' % outputDir, mode='overwrite', header='true')

    # Stop Spark context
    spark.stop()
