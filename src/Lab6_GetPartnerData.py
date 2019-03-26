# Get all paper details for the input organization from previous output
header = ['PaperId', 'Title', 'CitationCount', 'Date', 'PublicationType', 'LogProb', 'Url', 'VId', 'Year']
orgPapers = spark.read.format('csv').options(header='true', inferSchema='true').load('%s/%s' % (OutputDir, 'Paper.csv')).toDF(*header)

# Get all Paper-Author-Affiliation relationships for the input organization from previous output
header = ['PaperId', 'AuthorId', 'AffiliationId', 'AuthorSequenceNumber']
orgPaperAuthorAffiliation = spark.read.format('csv').options(header='true', inferSchema='true').load('%s/%s' % (OutputDir, 'PaperAuthorAffiliationRelationship.csv')).toDF(*header)

# Get all paper-author-affiliation relationships
paperAuthorAffiliations = getPaperAuthorAffiliationsDataFrame(MagDir)

# Get all affiliation details
affiliations = getAffiliationsDataFrame(MagDir)

# Get all author details
authors = getAuthorsDataFrame(MagDir)

# Get all Paper-Author-Affiliation relationships for papers published by the input organization
orgAllPaperAuthorAffiliations = paperAuthorAffiliations \
    .join(orgPapers, paperAuthorAffiliations.PaperId == orgPapers.PaperId, 'inner') \
    .select(orgPapers.PaperId, paperAuthorAffiliations.AuthorId, \
            paperAuthorAffiliations.AffiliationId, paperAuthorAffiliations.AuthorSequenceNumber)

# Get partner Paper-Author-Affiliation relationships by excluding those relationships of the input organization
partnerPaperAuthorAffiliation = orgAllPaperAuthorAffiliations.subtract(orgPaperAuthorAffiliation)
partnerPaperAuthorAffiliation.show(10)
partnerPaperAuthorAffiliation.write.mode('overwrite').format('csv').option('header','true').save('%s/PartnerPaperAuthorAffiliationRelationship.csv' % OutputDir)

# Get all partner affiliation Ids
partnerAffiliationIds = partnerPaperAuthorAffiliation \
    .where(partnerPaperAuthorAffiliation.AffiliationId.isNotNull()) \
    .select(partnerPaperAuthorAffiliation.AffiliationId) \
    .distinct()

# Get all partner affiliation details
partnerAffiliations = affiliations \
    .join(partnerAffiliationIds, affiliations.AffiliationId == partnerAffiliationIds.AffiliationId, 'inner') \
    .select(partnerAffiliationIds.AffiliationId, affiliations.DisplayName.alias('AffiliationName'))
partnerAffiliations.show(10)
partnerAffiliations.write.mode('overwrite').format('csv').option('header','true').save('%s/PartnerAffiliation.csv' % OutputDir)

# Get all partner author Ids
partnerAuthorIds = partnerPaperAuthorAffiliation.select(partnerPaperAuthorAffiliation.AuthorId).distinct()
partnerAuthors = authors.join(partnerAuthorIds, partnerAuthorIds.AuthorId == authors.AuthorId) \
                        .select(partnerAuthorIds.AuthorId, authors.DisplayName.alias('AuthorName'))
partnerAuthors.show(10)
partnerAuthors.write.mode('overwrite').format('csv').option('header','true').save('%s/PartnerAuthor.csv' % OutputDir)
