from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

def FieldsOfStudy(baseDir):
    _fieldsOfStudy = sqlContext.read.format('csv')\
                    .option("delimiter", "\t")\
                    .options(header='false', inferSchema='false')\
                    .load(baseDir + 'FieldsOfStudy.txt')
    headers = ['FieldOfStudyId', 'Rank', 'NormalizedName', 'DisplayName', \
               'MainType', 'Level', 'PaperCount', 'CitationCount', 'CreatedDate']
    _fieldsOfStudy = _fieldsOfStudy.toDF(*headers)
    return _fieldsOfStudy

def PaperFieldsOfStudy(baseDir):
    _paperFiledsOfStudy = sqlContext.read.format('csv')\
                         .option("delimiter", "\t")\
                         .options(header='false', inferSchema='false')\
                         .load(baseDir + 'PaperFieldsOfStudy.txt')
    headers = ['PaperId', 'FieldOfStudyId', 'Similarity']
    return _paperFiledsOfStudy.toDF(*headers)

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("academic graph spart test")\
        .getOrCreate()
    sqlContext = SQLContext(spark)

    fieldOfStudy = FieldsOfStudy('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/')
    paperFieldsOfStudy = PaperFieldsOfStudy('wasbs://mag-2018-09-27@magtrainingsource.blob.core.windows.net/mag/')

    # Get all paper details for the input organization.
    orgPapers = sqlContext.read.format('csv')\
                .option("delimiter", ",")\
                .options(header='true', inferSchema='true')\
                .load('/output/user01/pyspark/Paper.csv')

    # Get all Paper-Field-of-Study relationships for the input organization.
    orgPaperFieldOfStudy = paperFieldsOfStudy.join(orgPapers, paperFieldsOfStudy.PaperId == orgPapers.PaperId, 'inner')\
                            .select(orgPapers.PaperId, paperFieldsOfStudy.FieldOfStudyId)
    orgPaperFieldOfStudy.show()
    orgPaperFieldOfStudy.write.csv('/output/user01/pyspark/PaperFieldOfStudyRelationship.csv', \
                                    mode='overwrite', \
                                    header='true')

    # Get all field-of-study Ids for the input organization.
    orgFieldOfStudyIds = orgPaperFieldOfStudy.select(orgPaperFieldOfStudy.FieldOfStudyId).distinct()

    # Get all field-of-study details for the input organization
    out_filedOfStudy = fieldOfStudy.join(orgFieldOfStudyIds, fieldOfStudy.FieldOfStudyId == orgFieldOfStudyIds.FieldOfStudyId, 'inner')\
                        .select(orgFieldOfStudyIds.FieldOfStudyId, fieldOfStudy.Level.alias('FieldLevel'), fieldOfStudy.DisplayName.alias('FieldName'))
    out_filedOfStudy.show()
    out_filedOfStudy.write.csv('/output/user01/pyspark/FieldOfStudy.csv', \
                                    mode='overwrite', \
                                    header='true')

    spark.stop()