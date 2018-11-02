from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("academic graph spart test")\
        .getOrCreate()
    sqlContext = SQLContext(spark)

    # Get all paper-author-affiliation relationships
    paperAuthorAffiliations = sqlContext.read.format('csv')\
                                .option("delimiter", "\t")\
                                .options(header='false', inferSchema='false')\
                                .load('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/PaperAuthorAffiliations.txt')
    paperAuthorAffiliationHeaders = ['PaperId', 'AuthorId', 'AffiliationId', 'AuthorSequenceNumber']
    paperAuthorAffiliations = paperAuthorAffiliations.toDF(*paperAuthorAffiliationHeaders)

    # Get all affiliation details.
    affiliations = sqlContext.read.format('csv')\
                    .option("delimiter", "\t")\
                    .options(header='false', inferSchema='false')\
                    .load('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/Affiliations.txt')
    affiliationHeaders = ['AffiliationId', 'Rank', 'NormalizedName', \
                          'DisplayName', 'GridId', 'OfficialPage', 'WikiPage', \
                          'PaperCount', 'CitationCount', 'CreatedDate']
    affiliations = affiliations.toDF(*affiliationHeaders)

    # Get all author details.
    authors = sqlContext.read.format('csv')\
                .option("delimiter", "\t")\
                .options(header='false', inferSchema='false')\
                .load('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/Authors.txt')
    authorHeaders = ['AuthorId', 'Rank', 'NormalizedName', 'DisplayName', \
                     'LastKnownAffiliationId', 'PaperCount', 'CitationCount', 'CreatedDate']
    authors = authors.toDF(*authorHeaders)

    # Get all paper details for the input organization.
    orgPapers = sqlContext.read.format('csv')\
                .option("delimiter", ",")\
                .options(header='true', inferSchema='true')\
                .load('/output/user01/pyspark/Paper.csv')

    # Get all Paper-Author-Affiliation relationships for the input organization
    orgpaperAuthorAffiliation = sqlContext.read.format('csv')\
                            .option("delimiter", ",")\
                            .options(header='true', inferSchema='true')\
                            .load('/output/user01/pyspark/paperAuthorAffiliationRelationship.csv')

    # Get all Paper-Author-Affiliation relationships for papers published by the input organization.
    orgAllPaperAuthorAffiliations = paperAuthorAffiliations.join(orgPapers, paperAuthorAffiliations.PaperId == orgPapers.PaperId, 'inner')\
                                        .select(orgPapers.PaperId, paperAuthorAffiliations.AuthorId, \
                                                paperAuthorAffiliations.AffiliationId, paperAuthorAffiliations.AuthorSequenceNumber)

    # Get partner Paper-Author-Affiliation relationships by excluding those relationships of the input organization.
    orgPartnerPaperAuthorAffiliation = orgAllPaperAuthorAffiliations.subtract(orgpaperAuthorAffiliation)
    orgPartnerPaperAuthorAffiliation.show()
    orgPartnerPaperAuthorAffiliation.write.csv('/output/user01/pyspark/Partner_PaperAuthorAffiliationRelationship.csv', \
                                               mode='overwrite', \
                                               header='true')

    # Get all partner affiliation Ids.
    orgPartnerAffiliationIds = orgPartnerPaperAuthorAffiliation.where(orgPartnerPaperAuthorAffiliation.AffiliationId.isNotNull())\
                                    .select(orgPartnerPaperAuthorAffiliation.AffiliationId).distinct()

    # Get all partner affiliation details.
    orgPartnerAffiliations = affiliations.join(orgPartnerAffiliationIds, affiliations.AffiliationId == orgPartnerAffiliationIds.AffiliationId, 'inner')\
                                         .select(orgPartnerAffiliationIds.AffiliationId, affiliations.DisplayName.alias('AffiliationName'))
    orgPartnerAffiliations.show()
    orgPartnerAffiliations.write.csv('/output/user01/pyspark/Partner_Affiliation.csv', \
                                     mode='overwrite', \
                                     header='true')

    # Get all partner author Ids.
    orgPartnerAuthorIds = orgPartnerPaperAuthorAffiliation.select(orgPartnerPaperAuthorAffiliation.AuthorId).distinct()
    orgPartnerAuthors = authors.join(orgPartnerAuthorIds, orgPartnerAuthorIds.AuthorId == authors.AuthorId)\
                            .select(orgPartnerAuthorIds.AuthorId, authors.DisplayName.alias('AuthorName'))
    orgPartnerAuthors.show()
    orgPartnerAuthors.write.csv('/output/user01/pyspark/Partner_Author.csv', \
                                 mode='overwrite', \
                                 header='true')

    spark.stop()