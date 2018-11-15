from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

# Replace containerName and accountName
containerName = "myContainerName"
accountName = "myAccountName"

outputDir = "/output/user01/pyspark"

def FieldsOfStudy(baseDir):
    _fieldsOfStudy = sqlContext.read.format('csv') \
        .option("delimiter", "\t") \
        .options(header='false', inferSchema='false') \
        .load(baseDir + 'FieldsOfStudy.txt')

    headers = ['FieldOfStudyId', 'Rank', 'NormalizedName', 'DisplayName', \
               'MainType', 'Level', 'PaperCount', 'CitationCount', 'CreatedDate']
    _fieldsOfStudy = _fieldsOfStudy.toDF(*headers)
    return _fieldsOfStudy

def PaperFieldsOfStudy(baseDir):
    _paperFiledsOfStudy = sqlContext.read.format('csv') \
        .option("delimiter", "\t") \
        .options(header='false', inferSchema='false') \
        .load(baseDir + 'PaperFieldsOfStudy.txt')

    headers = ['PaperId', 'FieldOfStudyId', 'Similarity']
    return _paperFiledsOfStudy.toDF(*headers)

if __name__ == "__main__":

    # Start Spark context
    spark = SparkSession \
        .builder \
        .appName("Microsoft academic graph spark Labs") \
        .getOrCreate()
    sqlContext = SQLContext(spark)

    # Load FieldsOfStudy data
    fieldOfStudy = FieldsOfStudy('wasbs://%s@%s.blob.core.windows.net/mag/' % (containerName, accountName))

    # Load PaperFieldsOfStudy data
    paperFieldsOfStudy = PaperFieldsOfStudy('wasbs://%s@%s.blob.core.windows.net/mag/' % (containerName, accountName))

    # Get all paper details for the input organization.
    orgPapers = sqlContext.read.format('csv') \
        .option("delimiter", ",") \
        .options(header='true', inferSchema='true') \
        .load('%s/Paper.csv' % outputDir) 

    # Get all Paper-Field-of-Study relationships for the input organization.
    orgPaperFieldOfStudy = paperFieldsOfStudy.join(orgPapers, paperFieldsOfStudy.PaperId == orgPapers.PaperId, 'inner') \
        .select(orgPapers.PaperId, paperFieldsOfStudy.FieldOfStudyId)

    # Optional: peek result
    orgPaperFieldOfStudy.show()

    # Output result
    orgPaperFieldOfStudy.write.csv('%s/PaperFieldOfStudyRelationship.csv' % outputDir, mode='overwrite', header='true')

    # Get all field-of-study Ids for the input organization.
    orgFieldOfStudyIds = orgPaperFieldOfStudy.select(orgPaperFieldOfStudy.FieldOfStudyId).distinct()

    # Get all field-of-study details for the input organization
    out_filedOfStudy = fieldOfStudy.join(orgFieldOfStudyIds, fieldOfStudy.FieldOfStudyId == orgFieldOfStudyIds.FieldOfStudyId, 'inner') \
        .select(orgFieldOfStudyIds.FieldOfStudyId, fieldOfStudy.Level.alias('FieldLevel'), fieldOfStudy.DisplayName.alias('FieldName'))

    # Optional: peek result
    out_filedOfStudy.show()

    # Output result
    out_filedOfStudy.write.csv('%s/FieldOfStudy.csv' % outputDir, mode='overwrite', header='true')

    # Stop Spark context
    spark.stop()
