# Load FieldsOfStudy data
fieldOfStudy = getFieldsOfStudyDataFrame(MagDir)

# Load PaperFieldsOfStudy data
paperFieldsOfStudy = getPaperFieldsOfStudyDataFrame(MagDir)

# Get all paper details for the input organization.
header = ['PaperId', 'AuthorId', 'AffiliationId', 'AuthorSequenceNumber']
orgPapers = spark.read.format('csv').options(header='true', inferSchema='true').load('%s/%s' % (OutputDir, 'Paper.csv')).toDF(*header)

# Get all Paper-Field-of-Study relationships for the input organization.
orgPaperFieldOfStudy = paperFieldsOfStudy.join(orgPapers, paperFieldsOfStudy.PaperId == orgPapers.PaperId, 'inner') \
    .select(orgPapers.PaperId, paperFieldsOfStudy.FieldOfStudyId)

# Optional: peek result
orgPaperFieldOfStudy.show()

# Output result
orgPaperFieldOfStudy.coalesce(1).write.mode('overwrite').format('csv').option('header','true').save('%s/PaperFieldOfStudyRelationship.csv' % OutputDir)

# Get all field-of-study Ids for the input organization.
orgFieldOfStudyIds = orgPaperFieldOfStudy.select(orgPaperFieldOfStudy.FieldOfStudyId).distinct()

# Get all field-of-study details for the input organization
out_filedOfStudy = fieldOfStudy.join(orgFieldOfStudyIds, fieldOfStudy.FieldOfStudyId == orgFieldOfStudyIds.FieldOfStudyId, 'inner') \
    .select(orgFieldOfStudyIds.FieldOfStudyId, fieldOfStudy.Level.alias('FieldLevel'), fieldOfStudy.DisplayName.alias('FieldName'))

# Optional: peek result
out_filedOfStudy.show()

# Output result
out_filedOfStudy.coalesce(1).write.mode('overwrite').format('csv').option('header','true').save('%s/FieldOfStudy.csv' % OutputDir)
