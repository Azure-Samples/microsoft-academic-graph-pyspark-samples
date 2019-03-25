# Load Affiliations data
df = getAffiliationsDataFrame(MagDir)

# Optional: peek the result
df.show()

# Extract the AffiliationId for Microsoft
microsoft = df.where(df.NormalizedName == 'microsoft').select(df.AffiliationId, df.DisplayName)

# Optional: peek the result
microsoft.show()

# Optional: Count number of rows in result
print("Number of rows in the dataframe: {}".format(microsoft.count()))

# Output result
microsoft.coalesce(1).write.mode('overwrite').format('csv').option('header','true').save('%s/Affiliation.csv' % OutputDir)
