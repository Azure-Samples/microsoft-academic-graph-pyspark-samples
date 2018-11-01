from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import count, sum
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Microsoft academic graph spark Labs")\
        .getOrCreate()
    sqlContext = SQLContext(spark)

    # Get all paper details for the input organization.
    orgPapers = sqlContext.read.format('csv')\
                .option("delimiter", ",")\
                .options(header='true', inferSchema='true')\
                .load('/output/user01/pyspark/Paper.csv')

    # Get paper count and citation sum for each year.
    #orgPaperGroupByYear = orgPapers.select(orgPapers.Year, count(orgPapers.PaperId).alias('PaperCount'), sum(orgPapers.CitationCount).alias('CitationSum'))\
    #                        .groupBy(orgPapers.Year)
    orgPaperGroupByYear = orgPapers.groupBy(orgPapers.Year).agg(count(orgPapers.PaperId).alias('PaperCount'), sum(orgPapers.CitationCount).alias('CitationSum'))
    orgPaperGroupByYear.show()
    orgPaperGroupByYear.write.csv('/output/user01/pyspark/OrgPaperGroupByYear.csv', \
                                  mode='overwrite', \
                                  header='true')

    spark.stop()