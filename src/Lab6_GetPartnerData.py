# Get all paper details for the input organization from previous output
orgPapers = asu.load('Paper.csv')

# Get all Paper-Author-Affiliation relationships for the input organization from previous output
orgPaperAuthorAffiliation = asu.load('PaperAuthorAffiliationRelationship.csv')

# Get all paper-author-affiliation relationships
paperAuthorAffiliations = mag.getDataframe('PaperAuthorAffiliations')

# Get all affiliation details
affiliations = mag.getDataframe('Affiliations')

# Get all author details
authors = mag.getDataframe('Authors')

# Get all Paper-Author-Affiliation relationships for papers published by the input organization
orgAllPaperAuthorAffiliations = paperAuthorAffiliations \
    .join(orgPapers, paperAuthorAffiliations.PaperId == orgPapers.PaperId, 'inner') \
    .select(orgPapers.PaperId, paperAuthorAffiliations.AuthorId, \
            paperAuthorAffiliations.AffiliationId, paperAuthorAffiliations.AuthorSequenceNumber)

# Get partner Paper-Author-Affiliation relationships by excluding those relationships of the input organization
partnerPaperAuthorAffiliation = orgAllPaperAuthorAffiliations.subtract(orgPaperAuthorAffiliation)
partnerPaperAuthorAffiliation.show(10)
asu.save(partnerPaperAuthorAffiliation, 'PartnerPaperAuthorAffiliationRelationship.csv')

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
asu.save(partnerAffiliations, 'PartnerAffiliation.csv')

# Get all partner author Ids
partnerAuthorIds = partnerPaperAuthorAffiliation.select(partnerPaperAuthorAffiliation.AuthorId).distinct()
partnerAuthors = authors.join(partnerAuthorIds, partnerAuthorIds.AuthorId == authors.AuthorId) \
                        .select(partnerAuthorIds.AuthorId, authors.DisplayName.alias('AuthorName'))
partnerAuthors.show(10)
asu.save(partnerAuthors, 'PartnerAuthor.csv')
