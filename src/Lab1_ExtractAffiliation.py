# Load Affiliations data
affiliations = mag.getDataframe('Affiliations')

# Optional: peek the result
affiliations.show(10)

# Extract the AffiliationId for Microsoft
microsoft = affiliations.where(affiliations.NormalizedName == 'microsoft').select(
    affiliations.AffiliationId, affiliations.DisplayName)

# Optional: peek the result
microsoft.show()

# Optional: Count number of rows in result
print("Number of rows in the dataframe: {}".format(microsoft.count()))

# Output result
asu.save(microsoft, 'Affiliation.csv')
