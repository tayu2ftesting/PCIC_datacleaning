require(RPostgreSQL)
require(data.table)
require(tidyverse)

# PCIC Raw file database initialization code
# May 2018

## Database name, port information. Edit this as necessary.
this_db = c()
this_db$db_name = "temporary"
this_db$this_host = 'localhost'
this_db$this_port = 5432
this_db$this_user = 'taysql'
this_db$this_pass = 'taysql'



# These functions initialize the system_* tables for the raw data schema.
# They consist of SQL 'CREATE TABLE' strings; the source_field_mappings_filename should point
# to a CSV with the source field mappings to be loaded into that table.
#
# This stuff only needs to be run once when setting up the raw data schema, and may
# break stuff if run on a pre-existing schema!

# Note that the schema itself needs to be manually created in pgAdmin before running any of this.

## initialize database (from scratch) -----------------------
init_database = function(schema_name, source_field_mappings_filename){
  # This creates the database tables:
  #   system_source_table (formerly main_source_table)
  #   system_raw_rows_table
  #   system_source_field_mappings
  #   system_source_file_table
  #
  #
  #   system_linked_identites
  #
  #
  message("Initializing (empty) tables...")
  message(paste0(" using schema_name: ", schema_name))
  #
  init_system_source_table(schema_name = schema_name)
  init_system_source_file_table(schema_name = schema_name)
  init_system_source_field_mappings_table(schema_name = schema_name, source_field_mappings_filename = source_field_mappings_filename)
  init_system_source_field_mapping_roles_table(schema_name = schema_name)
}


## system source table init ------------------------------
init_system_source_table = function(schema_name, table_name = 'system_source_table'){
  sql_cmd = paste0("CREATE TABLE ", schema_name, ".", table_name, "
  (
    source text,
    source_description text,
    contact_person text,
    contact_notes text,
    expected_update_frequency text,
    last_update_received text,
    n_files text,
    next_update_expected text,
    general_notes text
  )
  WITH (
    OIDS = FALSE
  );")
  #
  db_exec(sql_cmd)
}

## system source file table init ------------------------------
init_system_source_file_table = function(schema_name, table_name = 'system_source_file_table'){
  sql_cmd = paste0("CREATE TABLE ", schema_name, ".", table_name, "
  (
    source text,
    file_uuid text,
    file_num integer,
    num_rows integer,
    filename text,
    md5sum text,
    date_received text,
    who_received_from text,
    who_uploaded text,
    source_fieldname_mappings_used text,
    notes text
  )
  WITH (
    OIDS = FALSE
  );")
  #
  db_exec(sql_cmd)
}


## system source field mappings table init -------------------
init_system_source_field_mappings_table = function(schema_name, 
                                                   table_name = 'system_source_field_mappings_table', 
                                                   source_field_mappings_filename){
  sql_cmd = paste0("CREATE TABLE ", schema_name, ".", table_name, "
  (
  source text,
  source_identifier text,
  order_num text,
  field_role text,
  sub_role text,
  input_field_name text,
  output_field_name text,
  status text
  )
  WITH (
  OIDS = FALSE
  );")
  #
  # db_exec(sql_cmd)
  #
  input_table = read.csv(source_field_mappings_filename, header = TRUE)
  #
  #
  write_to_db(table_name = 'system_source_field_mappings_table', schema_name = schema_name, table = input_table)
}


## system source field mapping roles table init -------------------
init_system_source_field_mapping_roles_table = function(schema_name, 
                                                   table_name = 'system_source_field_mapping_roles_table'){
  sql_cmd = paste0("CREATE TABLE ", schema_name, ".", table_name, "
                   (
                   field_role text,
                   notes text
                   )
                   WITH (
                   OIDS = FALSE
                   );")
  #
  db_exec(sql_cmd)
  #
  #
}



## db execute  -------------------------------------
db_exec = function(sql_string){
  drv <- dbDriver("PostgreSQL")
  db_conn <- dbConnect(drv, dbname = this_db$db_name,
                       host = this_db$this_host, port = this_db$this_port, 
                       user = this_db$this_user, password = this_db$this_pass)
  dbExecute(db_conn, sql_string)
  #
  dbDisconnect(db_conn)
  #
}



