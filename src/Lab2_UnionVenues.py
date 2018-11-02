from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Microsoft academic graph spark Labs")\
        .getOrCreate()
    sqlContext = SQLContext(spark)

    conferences = sqlContext.read.format('csv').\
            option("delimiter", "\t").\
            options(header='false', inferSchema='false').\
            load('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/ConferenceSeries.txt')
    conferenceHeaders = ['ConferenceSeriesId', 'Rank', 'NormalizedName', \
                            'DisplayName', 'PaperCount', 'CitationCount', 'CreatedDate']
    conferences = conferences.toDF(*conferenceHeaders)

    journals = sqlContext.read.format('csv').\
            option("delimiter", "\t").\
            options(header='false', inferSchema='false').\
            load('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/Journals.txt')
    journalHeaders = ['JournalId', 'Rank', 'NormalizedName', \
                            'DisplayName', 'Issn', 'Publisher', 'Webpage', \
                            'PaperCount', 'CitationCount', 'CreatedDate']
    journals = journals.toDF(*journalHeaders)

    conferences = conferences\
                    .select(conferences.ConferenceSeriesId, conferences.DisplayName, conferences.NormalizedName)\
                    .selectExpr('ConferenceSeriesId as VId', 'DisplayName as VenueName', 'NormalizedName as VenueShortName')
    journals = journals\
                .select(journals.JournalId, journals.DisplayName, journals.NormalizedName)\
                .selectExpr('JournalId as VId', 'DisplayName as VenueName', 'NormalizedName as VenueShortName')
    venue = conferences.union(journals)
    venue.show()
    print('Number of rows in venue: {}'.format(venue.count()))
    venue.write.csv('/output/user01/pyspark/Venue.csv')

    spark.stop()