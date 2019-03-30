# Load Affiliation data for the input organization from previous output
affiliations = asu.load('Affiliation.csv')

# Load PaperAuthorAffiliations data
paperAuthorAffiliations = mag.getDataframe('PaperAuthorAffiliations')
# Optional: peek result
paperAuthorAffiliations.show(10)

# Filter PaperAuthorAffiliations by AffiliationId
orgPaperAuthorAffiliation = paperAuthorAffiliations \
    .join(affiliations, paperAuthorAffiliations.AffiliationId == affiliations.AffiliationId, 'inner') \
    .select(paperAuthorAffiliations.PaperId, paperAuthorAffiliations.AuthorId, \
            affiliations.AffiliationId, paperAuthorAffiliations.AuthorSequenceNumber)

# Optional: peek result
orgPaperAuthorAffiliation.show(10)

# Optional: Count number of rows in result
print('Number of rows in PaperAuthorAffiliation: {}'.format(orgPaperAuthorAffiliation.count()))

# Output result
asu.save(orgPaperAuthorAffiliation, 'PaperAuthorAffiliationRelationship.csv')
