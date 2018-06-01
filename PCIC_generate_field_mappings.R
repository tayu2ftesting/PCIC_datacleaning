require(tidyverse)
require(data.table)
require(readxl)





## ingest XLS fieldnames --------------------------
ingest_xls = function(input_filename){
  raw_table = read_excel(input_filename)
  raw_headers = tolower(names(raw_table))
  #
  clean_fieldnames = gsub(" ", "_", raw_headers) %>%
    gsub(".", "_")
  #
  return(clean_fieldnames)
}










