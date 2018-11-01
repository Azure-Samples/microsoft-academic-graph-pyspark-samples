from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import concat, lit, log, when
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("academic graph spart test")\
        .getOrCreate()
    sqlContext = SQLContext(spark)

    authors = sqlContext.read.format('csv').\
            option("delimiter", "\t").\
            options(header='false', inferSchema='false').\
            load('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/Authors.txt')
    authorHeaders = ['AuthorId', 'Rank', 'NormalizedName', 'DisplayName', \
                        'LastKnownAffiliationId', 'PaperCount', 'CitationCount', 'CreatedDate']
    authors = authors.toDF(*authorHeaders)
    Author = authors

    paperAuthorAffiliation = sqlContext.read.format('csv').\
            option("delimiter", ",").\
            options(header='true', inferSchema='true').\
            load('/output/user01/pyspark/paperAuthorAffiliationRelationship.csv')

    orgAuthorIds = paperAuthorAffiliation.select(paperAuthorAffiliation.AuthorId).distinct()

    # Get all author details.
    orgAuthors = Author.join(orgAuthorIds, Author.AuthorId == orgAuthorIds.AuthorId, 'inner')\
                        .select(orgAuthorIds.AuthorId, Author.DisplayName)\
                        .selectExpr('AuthorId as AuthorId', 'DisplayName as AuthorName')
    orgAuthors.show()
    orgAuthors.write.csv('/output/user01/pyspark/Author.csv', \
                            mode='overwrite', \
                            header='true')

    papers = sqlContext.read.format('csv').\
            option("delimiter", "\t").\
            options(header='false', inferSchema='false').\
            load('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/Papers.txt')
    paperHeaders = ['PaperId', 'Rank', 'Doi', 'DocType', 'PaperTitle', 'OriginalTitle', \
                    'BookTitle', 'Year', 'Date', 'Publisher', 'JournalId', 'ConferenceSeriesId', \
                    'ConferenceInstanceId', 'Volume', 'Issue', 'FirstPage', 'LastPage', \
                    'ReferenceCount', 'CitationCount', 'EstimatedCitation', 'CreatedDate']
    papers = papers.toDF(*paperHeaders)
    Paper = papers.withColumn('Prefix', lit('https://academic.microsoft.com/#/detail/'))

    # Get all paper details.
    orgPaperIds = paperAuthorAffiliation.select(paperAuthorAffiliation.PaperId).distinct()
    orgPapers = Paper.join(orgPaperIds, Paper.PaperId == orgPaperIds.PaperId)\
                        .where(Paper.Year >= 1991)\
                        .select(Paper.PaperId, Paper.PaperTitle.alias('Title'), Paper.EstimatedCitation.alias('CitationCount'), \
                                Paper.Date, when(Paper.DocType.isNull(), 'Not available').otherwise(Paper.DocType).alias('PublicationType'), \
                                log(Paper.Rank).alias('LogProb'), concat(Paper.Prefix, Paper.PaperId).alias('Url'), \
                                when(Paper.ConferenceSeriesId.isNull(), Paper.JournalId).otherwise(Paper.ConferenceSeriesId).alias('VId'), \
                                Paper.Year)
    orgPapers.show()
    print('Number of rows in orgPapers: {}'.format(orgPapers.count()))
    orgPapers.write.csv('/output/user01/pyspark/Paper.csv', \
                        mode='overwrite', \
                        header='true')

    spark.stop()