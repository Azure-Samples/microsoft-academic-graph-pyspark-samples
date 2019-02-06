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

    # Load Affiliations data
    df = getDataFrameForAffiliations(sqlContext, containerName, accountName)

    # Optional: peek the result
    df.show()

    # Extract the AffiliationId for Microsoft
    microsoft = df.where(df.NormalizedName == 'microsoft').select(df.AffiliationId, df.DisplayName)

    # Optional: peek the result
    microsoft.show()

    # Optional: Count number of rows in result
    print("Number of rows in the dataframe: {}".format(microsoft.count()))

    # Output result
    microsoft.write.csv('%s/Affiliation.csv' % outputDir, mode='overwrite', header='true')

    # Stop Spark context
    spark.stop()
