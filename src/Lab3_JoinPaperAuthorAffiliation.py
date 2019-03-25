# Load Affiliation data
header = ['AffiliationId', 'DisplayName']
affiliations = spark.read.format('csv').options(header='true', inferSchema='true').load('%s/%s' % (OutputDir, 'Affiliation.csv')).toDF(*header)

# Load PaperAuthorAffiliations data
paperAuthorAffiliations = getPaperAuthorAffiliationsDataFrame(MagDir)
# Optional: peek result
paperAuthorAffiliations.show()

orgPaperAuthorAffiliation = paperAuthorAffiliations \
    .join(affiliations, paperAuthorAffiliations.AffiliationId == affiliations.AffiliationId, 'inner') \
    .select(paperAuthorAffiliations.PaperId, paperAuthorAffiliations.AuthorId, \
            affiliations.AffiliationId, paperAuthorAffiliations.AuthorSequenceNumber)

# Optional: peek result
orgPaperAuthorAffiliation.show()

# Optional: Count number of rows in result
print('Number of rows in PaperAuthorAffiliation: {}'.format(orgPaperAuthorAffiliation.count()))

# Output result
orgPaperAuthorAffiliation.coalesce(1).write.mode('overwrite').format('csv').option('header','true').save('%s/PaperAuthorAffiliationRelationship.csv' % OutputDir)
