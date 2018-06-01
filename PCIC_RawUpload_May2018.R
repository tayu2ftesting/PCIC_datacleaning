require(data.table)
require(tidyverse)
require(RPostgreSQL)
require(ids)
require(tools)
require(readxl)

# R functions for uploading raw data (CSV) to postgres on PCIC
# May 2018
# Note: R functions for initialization of the schema from scratch are located elsewhere. (where?)



# Load in R file with login info (with the right username):
username = Sys.info()[6]
config_filename = paste0("C:\\Users\\", username, "\\Documents\\PCIC_postgres_login_info.R")
source(config_filename)

# It should contain this (edit as needed):
# this_db = c()
# this_db$db_name = "database_name"
# this_db$this_host = '10.0.0.69'
# this_db$this_port = 5432
# this_db$this_user = 'username@camdenhealth.org'
# this_db$this_pass = 'postgres_password'



# create new source ------------------------------------------
#'
#' This function creates a new data source. Essentially, this is just
#' adding a row to the system_source_table with the source name, description,
#' contact information etc. The minimum necessary information here is just
#' the source name and the schema_name.
#' @param source What to name the new source (e.g., 'virtua'). This will be part of the table name, so
#' the source name should be all lowercase, no periods, underscores are OK.
#' @param schema_name Schema name that holds the raw file tables.
#' @param source_description (optional) Text tag that describes the source.
#' @param contact_person (optional) Contact person at this source (institution/entity).
#' @param general_notes (optional) General notes about this source.
#' @keywords source, source_name
#' @export
#' @examples
#' create_new_source(source = 'kennedy', schema_name = 'raw_data_schema', source_description = 'Kennedy claims data',
#'                   contact_person = 'Fred Wilkins', general_notes = '')
#'                   
create_new_source = function(source, schema_name, source_description = "", contact_person = "", general_notes = ""){
  #
  # create a database connection (get_db_conn() should be defined below)
  db_conn = get_db_conn()
  #
  # Create one-row data.table for appending to system_source_table...
  # The values that are just blank strings can be edited later. Idea is to make this part
  # as painless as possible.
  update_values = data.table(source, source_description, 
                             contact_person, 
                             contact_notes = "", 
                             expected_update_frequency = "", 
                             last_update_received = "", 
                             n_files = "", 
                             next_update_expected = "",
                             general_notes)
  # 
  dbWriteTable(db_conn, c(schema_name, table_name = 'system_source_table'), update_values, 
               append = TRUE, row.names = FALSE)
  #
  # Disconnect from the database.
  dbDisconnect(db_conn)
  #
  return_good = paste0("Source ", source, " created successfully.")
  return(return_good)
}



# add raw file ------------------------------------------
#'
#' This function adds a new raw data file to the database, in addition to logging the new file in
#' a separate table.
#' @param input_source_identifer What tag from the input_source_field_mappings_table should we use to remap
#' the raw field names in the input file?
#' @param source Source name (previously established using create_new_source)
#' @param schema_name Schema name that holds the raw file tables.
#' @param filename (optional) File name to add. If no filename is provided, a dialog box will pop up that will
#' allow selecting a file from your computer.
#' @param file_separator (optional) Character that separates entries in the raw source file. If not provided,
#' defaults to comma (","). Some CSV-style files use a pipe separator ("|"), this is where to specify that.
#' @param sender (optional) Person who sent us the file. Defaults to 'na' if not provided.
#' @param notes (optional) Notes about this raw file/submission. Defaults to 'na' if not provided.
#' @keywords source, source_name, file_separator, sender, notes
#' @export
#' @examples
#' add_raw_file(input_source_identifier = 'ccpd_arrests_csv', source = 'ccpd', schema_name = 'raw_data_schema')
#'   
add_raw_file = function(input_source_identifier, source, schema_name, filename = file.choose(), 
                        file_separator = ",", sender = "na", notes = "na"){
  #
  # create a database connection (get_db_conn() should be defined below)
  db_conn = get_db_conn()
  #
  # If this function terminates early (errors out etc.), close the database connection.
  on.exit(dbDisconnect(db_conn))
  #
  # Figure out which file number this is for this source.
  # if no rows are returned from dplyr::filter for this source, this must be the first file.
  file_num = 1 
  system_source_file_table = dbReadTable(db_conn, c(schema_name, table_name = 'system_source_file_table'))
  source_copy = as.character(source)
  this_source = dplyr::filter(system_source_file_table, source == source_copy)
  #
  # Are there other files in the log from this source? If so, set file_num to the next number.
  # Otherwise file_num = 1 as seen above.
  if(nrow(this_source) > 0){
    file_num = max(this_source$file_num) + 1
  } 
  #
  message(paste0("Adding file number: ", file_num, "   for source: ", source, "   using field mappings: ", input_source_identifier))
  #
  # date_received holds a string with the current date/time
  date_received = as.character(date())
  #
  # sysinfo holds (among other things) strings for the current user that's uploading data (uploader)
  sysinfo = Sys.info()
  uploader = paste0(sysinfo[6], " @ ", sysinfo[4])
  #
  # raw_file_uuid will serve as a unique identifier for this particular file
  raw_file_uuid = uuid(use_time = TRUE)
  #
  # define the raw table name
  raw_table_name = paste0('raw_', source, str_pad(file_num, width = 6, pad = "0"))
  #
  # get the mappings for that file source from the field mappings table.
  mappings = get_field_mappings(input_source_identifier, schema_name)
  #
  #
  # browser()
  #
  # if there are no rows in the mappings table, that means the mapping identifier tag is wrong or the data isn't there.
  # either way this won't work, stop if that's the case.
  if(nrow(mappings) == 0){
    message(paste0("oops! no valid source file field name mappings found for tag ", input_source_identifier))
    dbDisconnect(db_conn)
    stop("Stopping. Database connection stopped. Nothing uploaded or updated, no changes made.")
  }
  #
  # Check for source name mismatch.
  if(mappings[1, 'source_institution'] != source_copy){
    message(paste0("oops! mismatch between provided source name (", source_copy, 
                   ") and table source file name (",mappings[1, 'source'], ") for identifier tag ", input_source_identifier))
    dbDisconnect(db_conn)
    stop("Stopping. Database connection stopped. Nothing uploaded or updated, no changes made.")
  }
  #
  # now that we have the mappings, we can suck in the file.
  # this takes in a CSV with headers. separators other than commas can be specified in the 
  # function arguments.
  message("Reading in raw file...")
  file_extension = substr(filename, nchar(filename)-4+1, nchar(filename))
  if(file_extension %in% c('xlsx', '.xls')){
    input_file = read_excel(filename)
  }
  if(file_extension %in% c('.csv', '.txt')){
    input_file = read.csv(filename, header = T, sep = file_separator)
  }
  #
  # set the incoming file's field names to what was in the mappings table.
  # Note that the new field names will be applied in the order they appear in the mappings table.
  names(input_file) = mappings$field_output_name
  #
  # How many rows are in the input file?
  num_rows = nrow(input_file)
  #
  # Get a md5sum for the original file stored on disk, for ease in later identification and
  # troubleshooting.
  md5sum = md5sum(filename)
  #
  # Set the row_key with a UUID.
  # The row_key serves as a unique tag that will refer to the raw data stored in each row.
  # Note that we are using the timestamp flavor of UUID here, and not completely random values.
  input_file$row_key = uuid(n = num_rows, use_time = TRUE)
  #
  # Check to see if the raw table name we are going to use already exists in the raw data schema.
  does_it_exist = dbExistsTable(db_conn, c(schema_name, raw_table_name))
  #
  # Does the table exist?
  if(does_it_exist == TRUE){
    # message(paste0("A table with the name ", raw_table_name, " was dropped from the schema to accomodate current data upload."))
    # dbRemoveTable(db_conn, c(schema_name, raw_table_name)) 
    # 
    # Instead of just dropping the raw table (might accidentally delete something we want to keep), stop the upload.
    dbDisconnect(db_conn)
    stop(paste0("A table with the name ", raw_table_name, " is already present in schema: ", schema_name, ". Fix that and try again."))
  }
  #
  # Now we actually write the table.
  message("Uploading table...")
  #
  # browser()
  dbWriteTable(db_conn, c(schema_name, raw_table_name), input_file, append = FALSE, row.names = FALSE)
  #
  # Assemble a one-row data.table with the information needed to log the upload in the system_source_file_table.
  source_file_table_update = data.table(source, file_uuid = raw_file_uuid, file_num, num_rows, filename, md5sum, 
                                        date_received, who_received_from = sender, 
                                        who_uploaded = uploader, source_fieldname_mappings_used = input_source_identifier, notes)
  #
  source_file_update_table_name = 'system_source_file_table'
  #
  message(paste0("Appending a row to the source file table with the following information:"))
  print(source_file_table_update)
  #
  # Append the row to the system_source_file_table.
  dbWriteTable(db_conn, c(schema_name, source_file_update_table_name), 
               source_file_table_update, append = TRUE, row.names = FALSE)
  #
  # Now we need to grab a copy of the (freshly updated) source file update table and upload that to mediawiki.
  updated_table = dbReadTable(db_conn,  c(schema_name, source_file_update_table_name))
  dbDisconnect(db_conn)
  #
  vergon6_db_conn = get_db_conn(db_info = vergon6_db)
  sql_string = paste0("TRUNCATE TABLE public.system_source_file_table;")
  dbExecute(vergon6_db_conn, sql_string)
  dbWriteTable(vergon6_db_conn, c('public', 'system_source_file_table'), updated_table, append = TRUE, row.names = FALSE)
  #
  #
  # Disconnect from the database. All done.
  #
  dbDisconnect(vergon6_db_conn)
  #
  return_good = paste0("Raw file uploaded successfully, database connection closed.")
  return(return_good)
}





## . ###################################################
## . ###################################################
## . ############ Helper functions  ####################
## . ###################################################
## . ###################################################




## get db_conn (that you close yourself) -----------------------
get_db_conn = function(db_info = this_db){
  drv <- dbDriver("PostgreSQL")
  db_conn <- dbConnect(drv, dbname = db_info$db_name,
                       host = db_info$this_host, port = db_info$this_port, 
                       user = db_info$this_user, password = db_info$this_pass)
  return(db_conn)
}


## get field mappings ------------------------------------------------------------------------
get_field_mappings = function(input_source_identifier, schema_name, table_name = 'system_source_field_mappings_table'){
  #
  db_conn = get_db_conn()
  #
  sql_query = paste0("SELECT * FROM ", schema_name, ".", table_name, ";")
  #
  result = dbGetQuery(db_conn, sql_query)
  #
  filtered = dplyr::filter(result, source_file_template == input_source_identifier)
  #
  dbDisconnect(db_conn)
  #
  return(filtered)
}
