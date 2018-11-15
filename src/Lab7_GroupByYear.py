from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import count, sum
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

    # Get all paper details for the input organization.
    orgPapers = sqlContext.read.format('csv') \
        .option("delimiter", ",") \
        .options(header='true', inferSchema='true') \
        .load('%s/Paper.csv' % outputDir)

    # Get paper count and citation sum for each year.
    orgPaperGroupByYear = orgPapers \
        .groupBy(orgPapers.Year) \
        .agg(count(orgPapers.PaperId).alias('PaperCount'), sum(orgPapers.CitationCount).alias('CitationSum'))

    # Optional: peek result
    orgPaperGroupByYear.show()

    # Output result
    orgPaperGroupByYear.write.csv('%s/OrgPaperGroupByYear.csv' % outputDir, mode='overwrite', header='true')

    # Stop Spark context
    spark.stop()
