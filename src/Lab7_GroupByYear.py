from pyspark.sql.functions import count, sum

# Get all paper details for the input organization from previous output
header = ['PaperId', 'Title', 'CitationCount', 'Date', 'PublicationType', 'LogProb', 'Url', 'VId', 'Year']
orgPapers = spark.read.format('csv').options(header='true', inferSchema='true').load('%s/%s' % (OutputDir, 'Paper.csv')).toDF(*header)

# Get paper count and citation sum for each year
orgPaperGroupByYear = orgPapers \
    .groupBy(orgPapers.Year) \
    .agg(count(orgPapers.PaperId).alias('PaperCount'), sum(orgPapers.CitationCount).alias('CitationSum'))

# Optional: peek result
orgPaperGroupByYear.show(10)

# Output result
orgPaperGroupByYear.coalesce(1).write.mode('overwrite').format('csv').option('header','true').save('%s/PaperGroupByYear.csv' % OutputDir)
