## packages------------------------------------------
if(!require("pacman")) {install.packages("pacman")}
pacman::p_load(here, tidyverse, arrow, lubridate, janitor, scales, cli, 
               roperators, readxl, vroom, utils, stats, tidyr)

## save_data_to_dash()----------------------------------------
save_data_to_dash <- function(dataset, scheme = "cs", file_date = "today", csv = FALSE){

if(file_date == "today"){
    date <- format(Sys.Date(), format = "%Y_%m_%d")
  } else {
    date <- file_date
  }

# file location
  if(scheme == "cs"){
  filename <- paste0("/dbfs/FileStore/tables/lab/restricted/CS Data/", deparse(substitute(dataset)), "_", date)
  } else if(scheme == "es"){
  filename <- paste0("/dbfs/FileStore/tables/lab/restricted/ES Data/", deparse(substitute(dataset)), "_", date)
  } else if(scheme == "sfi"){
  filename <- paste0("/dbfs/FileStore/tables/lab/restricted/SFI Data/", deparse(substitute(dataset)), "_", date)
  }
# file format
  if(csv){
    filename <- paste0(filename, ".csv")
    print(filename)  
    
    write.csv(dataset, filename)
  } else {
    filename <- paste0(filename, ".parquet")
    print(filename)   

    arrow::write_parquet(dataset, filename)
  }
}

# load_cs-----------------------------------------------------------------------

load_cs_dash <- function(live_options = FALSE) {
  message("Loading CS data...")
  
  cs_path = "/dbfs/FileStore/tables/lab/restricted/CS Data/CS_Transition_Data_2024_10_18.xlsx"
  
  if (live_options){
    sheetname = 'CS_Live_Options'
  } else{
    sheetname = 'CS_Agreements'
  }
  
  cs_raw <- readxl::read_xlsx(cs_path, sheet = sheetname)
  
  cs_raw
}

# load_es-----------------------------------------------------------------------

load_es_dash <- function(live_options = FALSE) {
  box::use(aws = aws.s3, readxl)
  message("Loading ES data...")
  
  es_path = "/dbfs/FileStore/tables/lab/restricted/ES Data/ES_Transition_Data_2024_10_18.xlsx"
  
  if (live_options){
    sheetname = 'ES_Live_Options_Summary'
  } else{
    sheetname = 'ES_Commitment_Report'
  }
  
  es_raw <- readxl::read_xlsx(es_path, sheet = sheetname)
  
  es_raw
}

# load_sfi-----------------------------------------------------------------------

load_sfi_dash <- function(sfi) {
  
  if(sfi == 2023){
    sfi_path <- "/dbfs/FileStore/tables/lab/restricted/SFI Data/20241115_sfi_2023__1_.csv"
  
  } else if (sfi == 2022){
    sfi_path = "/dbfs/FileStore/tables/lab/restricted/SFI Data/20231215_sfi22_data_extract.csv"
    
  } else if (sfi == 2024) {
    sfi_path = "/dbfs/FileStore/tables/lab/restricted/SFI Data/sfi_2022_20240116.csv" 
    
  } else {
    message("SFI type not found.")
  }
  sfi_raw <- read_csv(sfi_path)
  sfi_raw
  
}

## num_length()--------------------------------------
# num_length: Round numbers to a specific number of significant figures based on the number of digits in the number.
num_length <- function(x) {
  floor(log10(x)) + 1
}

## signif_figures()----------------------------------
#signif_figures: use a different number of significant figures depending on the length of the number
# KK: have altered this function as I don't know if it's right to use a differnt number of significant figures
# for different lenght numbers

signif_figures <- function(x) {
  # if (num_length(x) <= 3) {
  #   n = signif(x, 1) # i.e. 23 -> 20, 111 -> 100
  #   
  # } else if (num_length(x) == 4) {
  #   n = signif(x, 2) # i.e. 1111 -> 1100
  #   
  # } else if (num_length(x) >= 5) {
  #   n = signif(x, 3) # i.e. 31766 -> 31800
  #   
  # } else {
  #   n = x
  # }
  # return(n)
  n = signif(x, 3)
  return(n)
}


