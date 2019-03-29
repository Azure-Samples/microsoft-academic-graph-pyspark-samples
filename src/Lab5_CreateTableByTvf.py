# Get paper details for the input organization from previous output
orgPapers = asu.load('Paper.csv')

# Load FieldsOfStudy data
fieldOfStudy = mag.getDataframe('FieldsOfStudy')

# Load PaperFieldsOfStudy data
paperFieldsOfStudy = mag.getDataframe('PaperFieldsOfStudy')

# Get Paper-Field-of-Study relationships for the input organization
orgPaperFieldOfStudy = paperFieldsOfStudy \
    .join(orgPapers, paperFieldsOfStudy.PaperId == orgPapers.PaperId, 'inner') \
    .select(orgPapers.PaperId, paperFieldsOfStudy.FieldOfStudyId)

# Optional: peek result
orgPaperFieldOfStudy.show(10)

# Output result
asu.save(orgPaperFieldOfStudy, 'PaperFieldOfStudyRelationship.csv')

# Get all field-of-study Ids for the input organization
orgFieldOfStudyIds = orgPaperFieldOfStudy.select(orgPaperFieldOfStudy.FieldOfStudyId).distinct()

# Get all field-of-study details for the input organization
orgFiledOfStudy = fieldOfStudy \
    .join(orgFieldOfStudyIds, fieldOfStudy.FieldOfStudyId == orgFieldOfStudyIds.FieldOfStudyId, 'inner') \
    .select(orgFieldOfStudyIds.FieldOfStudyId, fieldOfStudy.Level.alias('FieldLevel'), fieldOfStudy.DisplayName.alias('FieldName'))

# Optional: peek result
orgFiledOfStudy.show(10)

# Output result
asu.save(orgFiledOfStudy, 'FieldOfStudy.csv')
