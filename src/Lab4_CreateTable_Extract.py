from pyspark.sql.functions import concat, lit, log, when

#Load PaperAuthorAffiliationRelationship data from previous output
paperAuthorAffiliation = spark.read.format('csv').options(header='true', inferSchema='true').load('%s/%s' % (OutputDir, 'PaperAuthorAffiliationRelationship.csv'))
paperAuthorAffiliation.show(10)

orgAuthorIds = paperAuthorAffiliation.select(paperAuthorAffiliation.AuthorId).distinct()

#Load Authors data
authors = getAuthorsDataFrame(MagDir)

# Get all author details
orgAuthors = authors \
    .join(orgAuthorIds, authors.AuthorId == orgAuthorIds.AuthorId, 'inner') \
    .select(orgAuthorIds.AuthorId, authors.DisplayName.alias('AuthorName'))

# Optional: peek result
orgAuthors.show(10)

# Output result
orgAuthors.write.mode('overwrite').format('csv').option('header','true').save('%s/Author.csv' % OutputDir)

#Load Papers data
papers = getPapersDataFrame(MagDir)

papers = papers.withColumn('Prefix', lit('https://academic.microsoft.com/#/detail/'))

# Get all paper details
orgPaperIds = paperAuthorAffiliation.select(paperAuthorAffiliation.PaperId).distinct()

orgPapers = papers \
    .join(orgPaperIds, papers.PaperId == orgPaperIds.PaperId) \
    .where(papers.Year >= 1991) \
    .select(papers.PaperId, papers.PaperTitle.alias('Title'), papers.EstimatedCitation.alias('CitationCount'), \
            papers.Date, when(papers.DocType.isNull(), 'Not available').otherwise(papers.DocType).alias('PublicationType'), \
            log(papers.Rank).alias('LogProb'), concat(papers.Prefix, papers.PaperId).alias('Url'), \
            when(papers.ConferenceSeriesId.isNull(), papers.JournalId).otherwise(papers.ConferenceSeriesId).alias('VId'), \
            papers.Year)

# Optional: peek result
orgPapers.show(10)

# Optional: Count number of rows in result
print('Number of rows in orgPapers: {}'.format(orgPapers.count()))

# Output result
orgPapers.write.mode('overwrite').format('csv').option('header','true').save('%s/Paper.csv' % OutputDir)
