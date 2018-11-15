from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

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

    #Load ConferenceSeries data
    conferences = sqlContext.read.format('csv') \
        .option("delimiter", "\t") \
        .options(header='false', inferSchema='false') \
        .load('wasbs://%s@%s.blob.core.windows.net/mag/ConferenceSeries.txt' % (containerName, accountName))

    # Insert headers
    conferenceHeaders = ['ConferenceSeriesId', 'Rank', 'NormalizedName', \
                         'DisplayName', 'PaperCount', 'CitationCount', 'CreatedDate']
    conferences = conferences.toDF(*conferenceHeaders)

    # Optional: peek result
    conferences.show()

    #Load Journals data
    journals = sqlContext.read.format('csv') \
        .option("delimiter", "\t") \
        .options(header='false', inferSchema='false') \
        .load('wasbs://%s@%s.blob.core.windows.net/mag/Journals.txt' % (containerName, accountName))

    # Insert headers
    journalHeaders = ['JournalId', 'Rank', 'NormalizedName', \
                      'DisplayName', 'Issn', 'Publisher', 'Webpage', \
                      'PaperCount', 'CitationCount', 'CreatedDate']
    journals = journals.toDF(*journalHeaders)

    # Optional: peek result
    journals.show()

    conferences = conferences \
        .select(conferences.ConferenceSeriesId, conferences.DisplayName, conferences.NormalizedName) \
        .selectExpr('ConferenceSeriesId as VId', 'DisplayName as VenueName', 'NormalizedName as VenueShortName')

    journals = journals \
        .select(journals.JournalId, journals.DisplayName, journals.NormalizedName) \
        .selectExpr('JournalId as VId', 'DisplayName as VenueName', 'NormalizedName as VenueShortName')

    venue = conferences.union(journals)

    # Optional: peek result
    venue.show()

    # Optional: Count number of rows in result
    print('Number of rows in venue: {}'.format(venue.count()))

    # Output result
    venue.write.csv('%s/Venue.csv' % outputDir, mode='overwrite', header='true')

    # Stop Spark context
    spark.stop()
