## packages------------------------------------------
install.packages(c("box", "readxl", "janitor", "here", "aws.s3"))
box::use(
  utils[...],
  stats[...],
  dp = dplyr, # alias
  #aws = aws.s3,
  readxl,
  lb = lubridate[`%m+%`],
  td = tidyr,
  janitor,
  stringr,
  magrittr[`%>%` = `%>%`, ...],
  vroom,
  arrow,
  tidyverse,
  here
)


## mutate_cond()------------------------------------
# mutate_cond function allows you to mutate only if a *different* column contains a value that is in a list
mutate_cond <-
  function(.data, condition, ..., envir = parent.frame()) {
    condition <- eval(substitute(condition), .data, envir)
    .data[condition,] <- .data[condition,] %>% mutate(...)
    .data
  }

## seq_date()----------------------------------------
# seq_date: Create a list of the days/months/years between two dates 
seq_date <- function(x, y, by) {
  list(seq(lb$floor_date(x, unit = by),
           lb$floor_date(y, unit = by),
           by = by))
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


## load_CS()------------------------------------------
# load CS data from s3 bucket
load_cs <- function(live_options = FALSE) {
  box::use(aws = aws.s3, readxl)
  message("Loading CS data...")
  # gets latest data from s3://s3-ranch-020/CS_transition_agreements/data/
  cs_path = system("aws s3 ls s3://s3-ranch-020/CS_transition_agreements/data/ --recursive | sort | tail -n 1 | awk '{print $4}'", intern = TRUE)
  bucket = 's3-ranch-020'
  
  if (live_options){
    sheetname = 'CS_Live_Options'
  } else{
    sheetname = 'CS_Agreements'
  }
  
  cs_raw <- aws$s3read_using(
    FUN = readxl$read_xlsx,
    sheet = sheetname,
    object = cs_path,
    bucket = bucket
  )
  
  cs_raw
  
}


## load_ES()--------------------------------------
load_es <- function(live_options = FALSE) {
  box::use(aws = aws.s3, readxl)
  message("Loading ES data...")
  # gets latest data from s3://s3-ranch-020/ES_transition_agreements/data/
  ES_path <- system("aws s3 ls s3://s3-ranch-020/ES_transition_agreements/data/ --recursive | sort | tail -n 1 | awk '{print $4}'", intern = TRUE)
  bucket = 's3-ranch-020'
  
  if (live_options){
    sheetname = 'ES_Live_Options_Summary'
  } else{
    sheetname = 'ES_Commitment_Report'
  }
  
  es_raw <- aws$s3read_using(
    FUN = readxl$read_xlsx,
    sheet = sheetname,
    object = ES_path,
    bucket = bucket
  )
  
  es_raw
  
}


## load_SFI23()----------------------------------
load_SFI23 <- function() {
  box::use(aws = aws.s3, 
           readr)
  message("Loading SFI data...")
  # gets latest data from s3://s3-ranch-020/sustainable_farming_incentive/data/
  bucket_contents = system("aws s3 ls s3://s3-ranch-020/sustainable_farming_incentive/data/ --recursive | sort | awk '{print $4}'", intern = TRUE)
  bucket_contents_no_22 <- bucket_contents[bucket_contents!="sustainable_farming_incentive/data/sfi_data_extract.csv"]
  SFI23_path <- utils::tail(bucket_contents_no_22, n = 1)
  bucket = 's3-ranch-020'
  
  message(paste("Reading SFI23 raw data", SFI23_path))
  sfi23_raw <- aws$s3read_using(
    FUN = readr$read_csv,
    object = SFI23_path,
    bucket = bucket
  )
  
  assign("sfi23_raw", sfi23_raw, envir = .GlobalEnv)

}


## load_SFI24()----------------------------------
load_SFI24 <- function(data_date = Sys.Date()){
  
  Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')
  sf <- lb$stamp("january_17_1999")
  # apply the template to today's date
  date <- tolower(sf(lb$ymd(data_date)))
  
  s3_bucket <- 's3-ranch-020'
  
  bucket_data <-  system("aws s3 ls s3://s3-ranch-020/SFI_expanded_offer/data/ --recursive | sort | tail -n 30 | awk '{print $4}'", intern = TRUE)
  
  this_year <- lb$year(data_date)
  date_pat <- paste0("\\d{8}")
  
  bucket_df <- data.frame(location = bucket_data) %>%
    dplyr::filter(stringr$str_detect(location, "SFI")) %>%
    dplyr::mutate(date = stringr$str_extract(location, date_pat)) %>%
    dplyr::mutate(date_parsed = lb$ymd(date)) %>%
    dplyr::mutate(dist_to_today = Sys.Date() - as.Date(date_parsed))
  
  this_months_data_locations <- bucket_df %>%
    dp$filter(!is.na(dist_to_today)) %>%
    dp$filter(dist_to_today == min(dist_to_today))
  
  for(path in this_months_data_locations$location){
    print(path)
    data <- aws$s3read_using(
      FUN = vroom$vroom,
      object = path,
      bucket = s3_bucket
    )
    
    name <- "sfi24_raw"
    
    assign(name, data, envir = .GlobalEnv)
  }
  
}

## load_SFI()-------------------------------------
# loads raw SFI data from he sustainable_farming_incentive folder
# finds latest sfi23 data and loads it
# SFI22 is updated in situ, so the file path remains the same when data is updated
load_SFI <- function(sfi = c("2022", "2023", "2024")) {
  box::use(aws = aws.s3, 
           vroom,
           utils,
           dp = dplyr,
           stringr)
  message("Loading SFI data...")
  bucket = 's3-ranch-020'
  if(sfi == "2023"){
    # gets latest data from s3://s3-ranch-020/sustainable_farming_incentive/data/
    bucket_contents = system("aws s3 ls s3://s3-ranch-020/sustainable_farming_incentive/data/ --recursive | sort | awk '{print $4}'", intern = TRUE)
    bucket_contents_no_22 = bucket_contents[bucket_contents != "sustainable_farming_incentive/data/sfi_data_extract.csv"]
    SFI23_path <- utils$tail(bucket_contents_no_22, n = 1)
    
    message(paste("Reading SFI23 raw data", SFI23_path))
    sfi23_raw <- aws$s3read_using(
      FUN = vroom$vroom,
      object = SFI23_path,
      bucket = bucket
    )
    
    assign("sfi23_raw", sfi23_raw, envir = .GlobalEnv)
    
  } else if (sfi == "2022"){
    
    SFI22_path = "sustainable_farming_incentive/data/sfi_data_extract.csv"
    
    message(paste("Reading SFI22 raw data", SFI22_path))
    sfi22_raw <- aws$s3read_using(
      FUN = vroom$vroom,
      object = SFI22_path,
      bucket = bucket
    )
    assign("sfi22_raw", sfi22_raw, envir = .GlobalEnv)
    
  } else if (sfi == "2024") {
    # gets latest data from s3://s3-ranch-020/SFI_expanded_offer/data/
    bucket_contents = system("aws s3 ls s3://s3-ranch-020/SFI_expanded_offer/data/ --recursive | sort | awk '{print $4}'", intern = TRUE)
    
    bucket_contents_df <- data.frame(location = bucket_contents) %>%
      dp$mutate(date = lb$ymd( stringr$str_extract(location, "\\d{8}"))) %>%
      dp$filter(!is.na(date)) %>%
      dp$arrange(date)
    
    SFI24_path <- utils$tail(bucket_contents_df$location, n = 1)
    
    message(paste("Reading SFI24 raw data", SFI24_path))
    sfi24_raw <- aws$s3read_using(
      FUN = vroom$vroom,
      object = SFI24_path,
      bucket = bucket
    )
    
    assign("sfi24_raw", sfi24_raw, envir = .GlobalEnv)
    
    
  } else {
    message("SFI type not found. Please give sfi as '2022', '2023' or '2024'")
  }
  
}

## save_data_to_dash()----------------------------------------
save_data_to_dash <- function(dataset, scheme = "cs", file_date = "today", csv = FALSE){

if(file_date == "today"){
    date <- sf(lb$ymd(Sys.Date()))
  } else {
    date <- file_date
  }

# /FileStore/tables/lab/restricted/CS Data
# file location
  if(scheme == "cs"){
  filename <- paste0("/dbfs/FileStore/tables/lab/restricted/CS Data/", deparse(substitute(dataset)), "_", date)
  } else if(scheme == "es"){
  filename <- paste0("transition_outputs/data/ES Data/", deparse(substitute(dataset)), "_", date)
  } else if(scheme == "sfi"){
  filename <- paste0("transition_outputs/data/SFI Data/", deparse(substitute(dataset)), "_", date)
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


## save_data_to_s3()----------------------------------------
save_data_to_s3 <- function(dataset, file_date = "today", csv = FALSE, sfi = FALSE){
  
  Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')
  sf <- lb$stamp("19990110")
  # apply the template to today's date
  if(file_date == "today"){
    date <- sf(lb$ymd(Sys.Date()))
  } else {
    date <- file_date
  }
  
  # file location
  if(sfi){
  filename <- paste0("transition_outputs/data/sfi_only/v", date, "/", deparse(substitute(dataset)), "_", date)
  } else {
  filename <- paste0("transition_outputs/data/v", date, "/", deparse(substitute(dataset)), "_", date)
  }
  
  # file format
  if(csv){
    filename <- paste0(filename, ".csv")
    print(filename)  
    
    aws.s3::s3write_using(
      x = dataset, 
      FUN = write.csv,
      row.names = FALSE,
      object = filename,
      bucket = 's3-ranch-020',
      opts = list('headers'=list('x-amz-acl'='bucket-owner-full-control')))
  } else {
    filename <- paste0(filename, ".parquet")
    print(filename)   
    
    aws.s3::s3write_using(
      x = dataset, 
      FUN = write.parquet,
      row.names = FALSE,
      object = filename,
      bucket = 's3-ranch-020',
      opts = list('headers'=list('x-amz-acl'='bucket-owner-full-control')))
  }
}


# this function outputs all dataframes in the 
save_all_data_to_s3 <- function() {
  question1 = readline("Would you like to output all dataframes in the environment to S3-ranch-020? (Y/N)")
  
  if (regexpr(question1, 'n', ignore.case = TRUE) == 1) {
    return("output not written")
    
  }
  if (regexpr(question1, 'y', ignore.case = TRUE) == 1) {
    continue = TRUE
    # create date format template
    Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')
    sf <- lb$stamp("january_17_1999")
    # apply the template to today's date
    date <- tolower(sf(lb$ymd(Sys.Date())))
    
    dfs <-
      Filter(function(x)
        methods::is(x, "data.frame"), mget(ls()))
    
    for (df in dfs) {
      filename <-
        paste0("transition_outputs/data/",
               date,
               "/",
               deparse(substitute(df)),
               "_",
               date,
               ".csv")
      
      aws.s3::s3write_using(
        x = df,
        FUN = arrow::write_parquet,
        row.names = FALSE,
        object = filename,
        bucket = 's3-ranch-020',
        opts = list('headers' = list('x-amz-acl' = 'bucket-owner-full-control')),
        multipart = TRUE
      )
    }
  } else {
    return("output not written")
  }
}

## load_latest_data_from_s3()----------------------------------
load_latest_data_from_s3 <- function(data_date = Sys.Date()){
  
  Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')
  sf <- lb$stamp("january_17_1999")
  # apply the template to today's date
  date <- tolower(sf(lb$ymd(data_date)))
  
  s3_bucket <- 's3-ranch-020'
  
  bucket_data <-  system("aws s3 ls s3://s3-ranch-020/transition_outputs/data/ --recursive | sort | tail -n 30 | awk '{print $4}'", intern = TRUE)
  
  this_year <- lubridate::year(data_date)
  date_pat <- paste0("([a-z]{3,9}_\\d{2}_", this_year, ")")
  
  bucket_df <- data.frame(location = bucket_data) %>%
    dplyr::filter(!stringr::str_detect(location, "sfi_only")) %>%
    dplyr::mutate(date = stringr::str_extract(location, date_pat)) %>%
    dplyr::mutate(date_parsed = lubridate::parse_date_time(date, "%B_%d_%Y")) %>%
    dplyr::mutate(dist_to_today = Sys.Date() - as.Date(date_parsed))
  
  this_months_data_locations <- bucket_df %>%
    dplyr::filter(dist_to_today == min(dist_to_today)) 
  
  for(path in this_months_data_locations$location){
    print(path)
    data <- aws.s3::s3read_using(
      FUN = vroom::vroom,
      object = path,
      bucket = s3_bucket
    )
    
    months <- c("_january", "_february", "_march", "_april", "_may", "_june", "_july", "_august", "_september", 
                "_october", "_november", "_december")
    
    extraction_pattern <- paste0("(", paste0(months, collapse = "|"), ")_\\d{2}_\\d{4}.csv")
    name <- stringr::str_remove(stringr::str_extract(path, "([^/]*$)"), extraction_pattern )
    
    assign(name, data, envir = .GlobalEnv)
  }
  
}

# data_date must be in format full month _ day _ year eg january_17_1999
load_dataset_from_s3 <- function(dataset_name, data_date = "today"){
  
  Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')
  sf <- lb$stamp("january_17_1999")
  
  s3_bucket <- 's3-ranch-020'
  bucket_data <-  system("aws s3 ls s3://s3-ranch-020/transition_outputs/data/ --recursive | sort | tail -n 30 | awk '{print $4}'", intern = TRUE)
  
  # apply the template to today's date
  if(data_date == "today") {
  date <- tolower(sf(lb$ymd(Sys.Date())))
  
  this_year <- lubridate::year(Sys.Date())
  date_pat <- paste0("([a-z]{3,9}_\\d{2}_", this_year, ")")
  
  bucket_df <- data.frame(location = bucket_data) %>%
    dplyr::mutate(date = stringr::str_extract(location, date_pat)) %>%
    dplyr::mutate(date_parsed = lubridate::parse_date_time(date, "%B_%d_%Y")) %>%
    dplyr::mutate(dist_to_today = Sys.Date() - as.Date(date_parsed))
  
  months <- paste0("_", tolower(month.name))
  
  extraction_pattern <- paste0("(", paste0(months, collapse = "|"), ")_\\d{2}_\\d{4}.csv")
  
  this_months_data_location <- bucket_df %>%
    dplyr::filter(dist_to_today == min(dist_to_today)) %>%
    dplyr::mutate(data_name = stringr::str_remove(stringr::str_extract(location, "([^/]*$)"), extraction_pattern )) %>%
    dplyr::filter(data_name == dataset_name)
  
    path = this_months_data_location$location
  } else {
    date <- tolower(sf(lb$ymd(data_date)))
    path = paste0("transition_outputs/data/", date, "/", dataset_name, "_", date, ".csv")
  }
    
    message(paste("loading: ", path))
    
    data <- aws.s3::s3read_using(
      FUN = vroom::vroom,
      object = path,
      bucket = s3_bucket
    )
    
    name <- stringr::str_remove(stringr::str_extract(path, "([^/]*$)"), paste0("_[a-z]{3,8}_\\d{2}_\\d{4}", "\\.csv") )
    assign(name, data,   envir = .GlobalEnv)
 
}

## load_latest_sfi2324_data_from_s3()----------------------------

load_latest_sfi2324_data_from_s3 <- function(data_date = Sys.Date()){
  
  Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')
  sf <- lb$stamp("january_17_1999")
  # apply the template to today's date
  date <- tolower(sf(lb$ymd(data_date)))
  
  s3_bucket <- 's3-ranch-020'
  
  bucket_data <-  system("aws s3 ls s3://s3-ranch-020/transition_outputs/data/ --recursive | sort | tail -n 30 | awk '{print $4}'", intern = TRUE)
  
  this_year <- lubridate::year(data_date)
  date_pat <- paste0("([a-z]{3,9}_\\d{2}_", this_year, ")")
  
  bucket_df <- data.frame(location = bucket_data) %>%
    dplyr::filter(stringr::str_detect(location, "sfi")) %>%
    dplyr::mutate(date = stringr::str_extract(location, date_pat)) %>%
    dplyr::mutate(date_parsed = lubridate::parse_date_time(date, "%B_%d_%Y")) %>%
    dplyr::mutate(dist_to_today = Sys.Date() - as.Date(date_parsed))
  
  this_months_data_locations <- bucket_df %>%
    dplyr::filter(dist_to_today == min(dist_to_today)) 
  
  for(path in this_months_data_locations$location){
    print(path)
    data <- aws.s3::s3read_using(
      FUN = vroom::vroom,
      object = path,
      bucket = s3_bucket
    )
    
    months <- c("_january", "_february", "_march", "_april", "_may", "_june", "_july", "_august", "_september", 
                "_october", "_november", "_december")
    
    extraction_pattern <- paste0("(", paste0(months, collapse = "|"), ")_\\d{2}_\\d{4}.csv")
    name <- stringr::str_remove(stringr::str_extract(path, "([^/]*$)"), extraction_pattern )
    
    assign(name, data, envir = .GlobalEnv)
  }
  
}


## load_latest_es_cs_data_from_s3()-----------------------------------

load_latest_es_cs_data_from_s3 <- function(data_date = Sys.Date()){
  
  Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')
  sf <- lb$stamp("january_17_1999")
  # apply the template to today's date
  date <- tolower(sf(lb$ymd(data_date)))
  
  s3_bucket <- 's3-ranch-020'
  
  bucket_data <-  system("aws s3 ls s3://s3-ranch-020/transition_outputs/data/ --recursive | sort | tail -n 30 | awk '{print $4}'", intern = TRUE)
  
  this_year <- lubridate::year(data_date)
  date_pat <- paste0("([a-z]{3,9}_\\d{2}_", this_year, ")")
  
  bucket_df <- data.frame(location = bucket_data) %>%
    dplyr::filter(!stringr::str_detect(location, "sfi")) %>%
    dplyr::mutate(date = stringr::str_extract(location, date_pat)) %>%
    dplyr::mutate(date_parsed = lubridate::parse_date_time(date, "%B_%d_%Y")) %>%
    dplyr::mutate(dist_to_today = Sys.Date() - as.Date(date_parsed))
  
  this_months_data_locations <- bucket_df %>%
    dplyr::filter(dist_to_today == min(dist_to_today)) 
  
  for(path in this_months_data_locations$location){
    print(path)
    data <- aws.s3::s3read_using(
      FUN = vroom::vroom,
      object = path,
      bucket = s3_bucket
    )
    
    months <- c("_january", "_february", "_march", "_april", "_may", "_june", "_july", "_august", "_september", 
                "_october", "_november", "_december")
    
    extraction_pattern <- paste0("(", paste0(months, collapse = "|"), ")_\\d{2}_\\d{4}.csv")
    name <- stringr::str_remove(stringr::str_extract(path, "([^/]*$)"), extraction_pattern )
    
    assign(name, data, envir = .GlobalEnv)
  }
  
}


## get_latest_data_date()--------------------------------
# get the date of the most recent raw data in S3
get_latest_data_date <- function(scheme = c("CS", "ES", "SFI")) {
  Sys.setenv("AWS_DEFAULT_REGION" = 'eu-west-1')
  s3_bucket <- 's3-ranch-020'
  if(tolower(scheme) == "cs"){
    folder = "CS_transition_agreements"
  } else if(tolower(scheme) == "es") {
    folder = "ES_transition_agreements"
  }  else if(tolower(scheme) == "sfi") {
    folder = "sustainable_farming_incentive"
  }
  latest_data <-  system(paste0("aws s3 ls s3://s3-ranch-020/", folder, "/data/ --recursive | sort | tail -n 1 | awk '{print $4}'"), intern = TRUE)
  latest_data_date <- stringr$str_extract(latest_data, "\\d{6}")
  return(latest_data_date)
}
