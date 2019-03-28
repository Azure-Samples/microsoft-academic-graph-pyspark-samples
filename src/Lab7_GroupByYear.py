from pyspark.sql.functions import count, sum

# Get all paper details for the input organization from previous output
orgPapers = asu.load('Paper.csv')

# Get paper count and citation sum for each year
orgPaperGroupByYear = orgPapers \
    .groupBy(orgPapers.Year) \
    .agg(count(orgPapers.PaperId).alias('PaperCount'), sum(orgPapers.CitationCount).alias('CitationSum'))

# Optional: peek result
orgPaperGroupByYear.show(10)

# Output result
asu.save(orgPaperGroupByYear, 'PaperGroupByYear.csv')
