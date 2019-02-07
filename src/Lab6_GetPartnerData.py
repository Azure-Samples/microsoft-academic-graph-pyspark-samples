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
        .appName("academic graph spart test") \
        .getOrCreate()
    sqlContext = SQLContext(spark)

    # Get all paper-author-affiliation relationships
    paperAuthorAffiliations = getDataFrameForPaperAuthorAffiliations(sqlContext, containerName, accountName)

    # Get all affiliation details.
    affiliations = getDataFrameForAffiliations(sqlContext, containerName, accountName)

    # Get all author details.
    authors = getDataFrameForAuthors(sqlContext, containerName, accountName)

    # Get all paper details for the input organization.
    orgPapers = sqlContext.read.format('csv') \
        .option("delimiter", ",") \
        .options(header='true', inferSchema='true') \
        .load('%s/Paper.csv' % outputDir)

    # Get all Paper-Author-Affiliation relationships for the input organization
    orgpaperAuthorAffiliation = sqlContext.read.format('csv') \
        .option("delimiter", ",") \
        .options(header='true', inferSchema='true') \
        .load('%s/PaperAuthorAffiliationRelationship.csv' % outputDir)

    # Get all Paper-Author-Affiliation relationships for papers published by the input organization.
    orgAllPaperAuthorAffiliations = paperAuthorAffiliations \
        .join(orgPapers, paperAuthorAffiliations.PaperId == orgPapers.PaperId, 'inner') \
        .select(orgPapers.PaperId, paperAuthorAffiliations.AuthorId, \
                paperAuthorAffiliations.AffiliationId, paperAuthorAffiliations.AuthorSequenceNumber)

    # Get partner Paper-Author-Affiliation relationships by excluding those relationships of the input organization.
    orgPartnerPaperAuthorAffiliation = orgAllPaperAuthorAffiliations.subtract(orgpaperAuthorAffiliation)
    orgPartnerPaperAuthorAffiliation.show()
    orgPartnerPaperAuthorAffiliation.write.csv('%s/Partner_PaperAuthorAffiliationRelationship.csv' % outputDir, mode='overwrite', header='true')

    # Get all partner affiliation Ids.
    orgPartnerAffiliationIds = orgPartnerPaperAuthorAffiliation \
        .where(orgPartnerPaperAuthorAffiliation.AffiliationId.isNotNull()) \
        .select(orgPartnerPaperAuthorAffiliation.AffiliationId) \
        .distinct()

    # Get all partner affiliation details.
    orgPartnerAffiliations = affiliations \
        .join(orgPartnerAffiliationIds, affiliations.AffiliationId == orgPartnerAffiliationIds.AffiliationId, 'inner') \
        .select(orgPartnerAffiliationIds.AffiliationId, affiliations.DisplayName.alias('AffiliationName'))
    orgPartnerAffiliations.show()
    orgPartnerAffiliations.write.csv('%s/Partner_Affiliation.csv' % outputDir, mode='overwrite', header='true')

    # Get all partner author Ids.
    orgPartnerAuthorIds = orgPartnerPaperAuthorAffiliation.select(orgPartnerPaperAuthorAffiliation.AuthorId).distinct()
    orgPartnerAuthors = authors.join(orgPartnerAuthorIds, orgPartnerAuthorIds.AuthorId == authors.AuthorId) \
                            .select(orgPartnerAuthorIds.AuthorId, authors.DisplayName.alias('AuthorName'))
    orgPartnerAuthors.show()
    orgPartnerAuthors.write.csv('%s/Partner_Author.csv' % outputDir, mode='overwrite', header='true')

    # Stop Spark context
    spark.stop()
