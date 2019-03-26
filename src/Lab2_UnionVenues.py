#Load ConferenceSeries data
conferences = getConferenceSeriesDataFrame(MagDir)
# Optional: peek result
conferences.show(10)

#Load Journals data
journals = getJournalsDataFrame(MagDir)
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
venue.write.mode('overwrite').format('csv').option('header','true').save('%s/Venue.csv' % OutputDir)
