#Load ConferenceSeries data
conferences = mag.getDataframe('ConferenceSeries')
# Optional: peek result
conferences.show(10)

#Load Journals data
journals = mag.getDataframe('Journals')
# Optional: peek result
journals.show(10)

conferences = conferences \
    .select(conferences.ConferenceSeriesId, conferences.DisplayName, conferences.NormalizedName) \
    .selectExpr('ConferenceSeriesId as VId', 'DisplayName as VenueName', 'NormalizedName as VenueShortName')

journals = journals \
    .select(journals.JournalId, journals.DisplayName, journals.NormalizedName) \
    .selectExpr('JournalId as VId', 'DisplayName as VenueName', 'NormalizedName as VenueShortName')

venue = conferences.union(journals)

# Optional: peek result
venue.show(10)

# Optional: Count number of rows in result
print('Number of rows in venue: {}'.format(venue.count()))

# Output result
asu.save(venue, 'Venue.csv')
