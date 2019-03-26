# Get paper details for the input organization from previous output
header = ['PaperId', 'Title', 'CitationCount', 'Date', 'PublicationType', 'LogProb', 'Url', 'VId', 'Year']
orgPapers = spark.read.format('csv').options(header='true', inferSchema='true').load('%s/%s' % (OutputDir, 'Paper.csv')).toDF(*header)

# Load FieldsOfStudy data
fieldOfStudy = getFieldsOfStudyDataFrame(MagDir)

# Load PaperFieldsOfStudy data
paperFieldsOfStudy = getPaperFieldsOfStudyDataFrame(MagDir)

# Get Paper-Field-of-Study relationships for the input organization
orgPaperFieldOfStudy = paperFieldsOfStudy.join(orgPapers, paperFieldsOfStudy.PaperId == orgPapers.PaperId, 'inner') \
    .select(orgPapers.PaperId, paperFieldsOfStudy.FieldOfStudyId)

# Optional: peek result
orgPaperFieldOfStudy.show(10)

# Output result
orgPaperFieldOfStudy.write.mode('overwrite').format('csv').option('header','true').save('%s/PaperFieldOfStudyRelationship.csv' % OutputDir)

# Get all field-of-study Ids for the input organization
orgFieldOfStudyIds = orgPaperFieldOfStudy.select(orgPaperFieldOfStudy.FieldOfStudyId).distinct()

# Get all field-of-study details for the input organization
orgFiledOfStudy = fieldOfStudy.join(orgFieldOfStudyIds, fieldOfStudy.FieldOfStudyId == orgFieldOfStudyIds.FieldOfStudyId, 'inner') \
    .select(orgFieldOfStudyIds.FieldOfStudyId, fieldOfStudy.Level.alias('FieldLevel'), fieldOfStudy.DisplayName.alias('FieldName'))

# Optional: peek result
orgFiledOfStudy.show(10)

# Output result
orgFiledOfStudy.write.mode('overwrite').format('csv').option('header','true').save('%s/FieldOfStudy.csv' % OutputDir)
