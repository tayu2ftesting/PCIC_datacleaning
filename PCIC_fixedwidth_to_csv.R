require(tidyverse)
require(data.table)
require(readxl)



## Magic numbers for ingesting fixed width NJDOC data ----------------------
# njdoc_details = read.fwf(file = njdoc_details_filename, widths = 
#         c(10, 9, 7, 30, 30, 30, 9, 12, 8, 32, 13, 9, 9, 16, 4, 10, 9, 18, 22, 25))
# njdoc_history = read.fwf(file = njdoc_history_filename,  comment.char = "", widths = c(9, 7, 32, 40, 70, 9, 9, 30, 25))




## convert fixed width details file to csv ----------------------------------
# This version works for the may2018 file.
convert_njdoc_details_may2018 = function(njdoc_details_filename, output_filename){
  njdoc_details = read.fwf(file = njdoc_details_filename, widths =
                             c(10, 9, 7, 30, 30, 30, 9, 12, 8, 32, 13, 9, 9, 16, 4, 10, 9, 18, 22, 25))
  names(njdoc_details) = c('unknown_one', 'offender_id', 'sbi_nbr', 'lname', 'fname', 'mname', 'dob', 'sex', 'race', 'inmate_number',
                           'parole_eligible_date', 'max_date', 'admission_date', 'admission_reason', 'unknown_two', 'unknown_three',  'release_date',
                           'release_reason', 'location', 'regional_id')
  write.csv(njdoc_details, file = output_filename, quote = FALSE,  row.names = FALSE)
  return(njdoc_details)
}


## convert fixed width details file to csv ----------------------------------
# This version is for the nov2015 file.
convert_njdoc_details_nov2015 = function(njdoc_details_filename, output_filename){
  njdoc_details = read.fwf(file = njdoc_details_filename, widths =
                             c(13, 7, 30, 30, 30, 10, 6, 30, 10, 14, 10, 10, 16, 10, 18, 18, 16))
  #                            1   2  3    4   5   6  7   8   9  10  11  12  13  14  15  16  17
  #names(njdoc_details) = c('unknown_one', 'offender_id', 'sbi_nbr', 'lname', 'fname', 'mname', 'dob', 'sex', 'race', 'inmate_number',
   #                        'parole_eligible_date', 'max_date', 'admission_date', 'admission_reason', 'unknown_two', 'unknown_three',  'release_date',
   #                        'release_reason', 'location', 'regional_id')
  write.csv(njdoc_details, file = output_filename, quote = FALSE,  row.names = FALSE)
  return(njdoc_details)
}


## convert fixed width history file to csv ----------------------------------
convert_njdoc_history_may2018 = function(njdoc_history_filename, output_filename){
  njdoc_history = read.fwf(file = njdoc_history_filename,  comment.char = "", widths = c(9, 7, 32, 40, 70, 9, 9, 30, 25))
  names(njdoc_history) = c('unknown_one', 'offender_id', 'indictment', 'county', 'offense', 'offense_date', 'sentence_date', 
                          'minimum_term', 'maximum_term')
  write.csv(njdoc_history, file = output_filename, quote = FALSE, row.names = FALSE)
  return(njdoc_history)
}


## convert fixed width history file to csv ----------------------------------
convert_njdoc_history_nov2015 = function(njdoc_history_filename, output_filename){
  njdoc_history = read.fwf(file = njdoc_history_filename,  comment.char = "", widths = 
                             c(9, 7, 32, 70, 10, 10, 30, 25))
  #                            1  2   3   4   5  6    7   8   9  10  11  12  13  14  15  16  17
  names(njdoc_history) = c('unknown_one', 'offender_id', 'indictment', 'offense', 'offense_date', 'sentence_date', 
                           'minimum_term', 'maximum_term')
  write.csv(njdoc_history, file = output_filename, quote = FALSE, row.names = FALSE)
  return(njdoc_history)
}

## convert fixed width njdoc locations to csv -------------------------------
# This function works for both the may2018 and nov2015 location files.
convert_njdoc_locations = function(njdoc_locations_filename, output_filename){
  njdoc_locations = read.fwf(file = njdoc_locations_filename,  comment.char = "", widths = c(6, 40))
  names(njdoc_locations) = c('agency_location_id', 'description' )
  write.csv(njdoc_locations, file = output_filename, quote = FALSE, row.names = FALSE)
  return(njdoc_locations)
}










