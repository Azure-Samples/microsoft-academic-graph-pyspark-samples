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

    #Load ConferenceSeries data
    conferences = getDataFrameForConferenceSeries(sqlContext, containerName, accountName)
    # Optional: peek result
    conferences.show()

    #Load Journals data
    journals = getDataFrameForJournals(sqlContext, containerName, accountName)
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
