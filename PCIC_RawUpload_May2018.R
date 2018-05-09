require(data.table)
require(tidyverse)
require(RPostgreSQL)
require(ids)
require(tools)

# R functions for uploading raw data (CSV) to postgres on PCIC
# May 2018
# Note: R functions for initialization of the schema from scratch are located elsewhere. (where?)


## Database name, port information. Edit this as necessary.
this_db = c()
this_db$db_name = "temporary"
this_db$this_host = 'localhost'
this_db$this_port = 5432
this_db$this_user = 'taysql'
this_db$this_pass = 'taysql'


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
  return()
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
  # Figure out which file number this is for this source.
  file_num = 1 
  # if no rows are returned from dplyr::filter for this source, this must be the first file.
  system_source_file_table = dbReadTable(db_conn, c(schema_name, table_name = 'system_source_file_table'))
  this_source = dplyr::filter(system_source_file_table, source == source)
  if(nrow(this_source) > 0){
    file_num = max(this_source$file_num) + 1
  }
  #
  message(paste0("adding file number ", file_num, " for source ", source, " using field mappings ", input_source_identifier))
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
  # now that we have the mappings, we can suck in the file.
  # this takes in a CSV with headers. separators other than commas can be specified in the 
  # function arguments.
  message("Reading in raw file...")
  input_file = read.csv(filename, header = T, sep = file_separator)
  #
  # set the incoming file's field names to what was in the mappings table.
  # Note that the new field names will be applied in the order they appear in the mappings table.
  names(input_file) = mappings$output_field_name
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
  dbWriteTable(db_conn, c(schema_name, raw_table_name), input_file, append = FALSE, row.names = FALSE)
  #
  # Assemble a one-row data.table with the information needed to log the upload in the system_source_file_table.
  source_file_table_update = data.table(source, file_uuid = raw_file_uuid, file_num, num_rows, filename, md5sum, 
                                        date_received, who_received_from = sender, 
                                        who_uploaded = uploader, source_fieldname_mappings_used = input_source_identifier, notes)
  #
  source_file_update_table_name = 'system_source_file_table'
  #
  # Append the row to the system_source_file_table.
  dbWriteTable(db_conn, c(schema_name, source_file_update_table_name), 
               source_file_table_update, append = TRUE, row.names = FALSE)
  #
  # Disconnect from the database. All done.
  dbDisconnect(db_conn)
  #
  return()
}





## . ###################################################
## . ###################################################
## . ############ Helper functions  ####################
## . ###################################################
## . ###################################################




## get db_conn (that you close yourself) -----------------------
get_db_conn = function(){
  drv <- dbDriver("PostgreSQL")
  db_conn <- dbConnect(drv, dbname = this_db$db_name,
                       host = this_db$this_host, port = this_db$this_port, 
                       user = this_db$this_user, password = this_db$this_pass)
  return(db_conn)
}





