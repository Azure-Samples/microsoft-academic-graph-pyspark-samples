#Load Authors data
authors = getAuthorsDataFrame(MagDir)

#Load PaperAuthorAffiliationRelationship data
header = ['PaperId', 'AuthorId', 'AffiliationId', 'AuthorSequenceNumber']
paperAuthorAffiliation = spark.read.format('csv').options(header='true', inferSchema='true').load('%s/%s' % (OutputDir, 'PaperAuthorAffiliationRelationship.csv')).toDF(*header)

orgAuthorIds = paperAuthorAffiliation.select(paperAuthorAffiliation.AuthorId).distinct()

# Get all author details.
orgAuthors = authors.join(orgAuthorIds, authors.AuthorId == orgAuthorIds.AuthorId, 'inner') \
    .select(orgAuthorIds.AuthorId, authors.DisplayName) \
    .selectExpr('AuthorId as AuthorId', 'DisplayName as AuthorName')

# Optional: peek result
orgAuthors.show()

# Output result
orgAuthors.write.csv('%s/Author.csv' % outputDir, mode='overwrite', header='true')

#Load Papers data
papers = getDataFrameForPapers(sqlContext, containerName, accountName)

Paper = papers.withColumn('Prefix', lit('https://academic.microsoft.com/#/detail/'))

# Get all paper details.
orgPaperIds = paperAuthorAffiliation.select(paperAuthorAffiliation.PaperId).distinct()
orgPapers = Paper.join(orgPaperIds, Paper.PaperId == orgPaperIds.PaperId) \
    .where(Paper.Year >= 1991) \
    .select(Paper.PaperId, Paper.PaperTitle.alias('Title'), Paper.EstimatedCitation.alias('CitationCount'), \
            Paper.Date, when(Paper.DocType.isNull(), 'Not available').otherwise(Paper.DocType).alias('PublicationType'), \
            log(Paper.Rank).alias('LogProb'), concat(Paper.Prefix, Paper.PaperId).alias('Url'), \
            when(Paper.ConferenceSeriesId.isNull(), Paper.JournalId).otherwise(Paper.ConferenceSeriesId).alias('VId'), \
            Paper.Year)

# Optional: peek result
orgPapers.show()

# Optional: Count number of rows in result
print('Number of rows in orgPapers: {}'.format(orgPapers.count()))

# Output result
orgPapers.coalesce(1).write.mode('overwrite').format('csv').option('header','true').save('%s/Paper.csv' % OutputDir)
