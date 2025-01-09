# CODE TO PRODUCE COMBINED AES DATA WITH AREA UNDER ALL SCHEMES
# schema: id_parcel, sbi, options_code, scheme, agreement_start, agreement_end, 
# status, payment_amount, area
# Created by: Ashwini Petchiappan

# PACKAGES----------------------------------------------------------------------

if(!require("pacman")) { install.packages("pacman") }
pacman::p_load(here, tidyverse, arrow, lubridate, janitor, scales, cli, 
               miceadds, roperators, readxl, vroom, utils, stats, tidyr, aws.s3)

# PATHS-------------------------------------------------------------------------
bucket20 <- 's3-ranch-020'
cs_path <- 's3://s3-ranch-020/CS_parcel_level/data/20240503_CS_Data.csv'
es_path <- 's3://s3-ranch-020/ES_Parcel_Level/es_parcels_live_20230901.csv'
sfi22_path <- 'sustainable_farming_incentive/data/sfi_data_extract.csv'
sfi23_path <- '/SFI/data/SFI_option_data_20231127.csv'
sfi24_path <- 's3://s3-ranch-020/SFI_expanded_offer/data/20241202_SFI24.csv'

# PARAMETERS--------------------------------------------------------------------

cutoff_date <- "2024-10-15"

standardise_dates <- function(date_column, date_formats = c("dmy", "dmy HMS", "ymd", "ymd HMS")) {
  
  date_column %>%
    # Try parsing in various formats
    lubridate::parse_date_time(orders = date_formats) %>%
    lubridate::floor_date(unit = "day")
}

# CS----------------------------------------------------------------------------

capital_opts <- c('AC1', 'AC2', 'AQ1', 'AQ2', 
                  'BN1', 'BN10', 'BN11', 'BN12', 'BN13', 'BN14', 'BN15', 
                  'BN2', 'BN3', 'BN4', 'BN5', 'BN6', 'BN7', 'BN8', 'BN9', 
                  'FG1', 'FG10', 'FG11', 'FG12', 'FG13', 'FG14', 'FG15', 'FG16', 
                  'FG17', 'FG2', 'FG3', 'FG4', 'FG5', 'FG7', 'FG8', 'FG9', 
                  'FM1', 'FM2', 'FY1', 'FY2', 'HE1', 'HE3', 
                  'LV1', 'LV2', 'LV3', 'LV4', 'LV5', 'LV6', 'LV7', 'LV8', 
                  'PA1', 'PA2', 'PA3', 
                  'RP1', 'RP10', 'RP11', 'RP12', 'RP13', 'RP14', 'RP15', 'RP16', 
                  'RP17', 'RP18', 'RP19', 'RP2', 'RP20', 'RP21', 'RP24', 'RP26', 
                  'RP27', 'RP28', 'RP29', 'RP3', 'RP30', 'RP31', 'RP32', 'RP33', 
                  'RP4', 'RP5', 'RP6', 'RP7', 'RP9', 
                  'SB1', 'SB2', 'SB3', 'SB4', 'SB5', 'SB6', 
                  'TE1', 'TE10', 'TE11', 'TE12', 'TE13', 'TE14', 
                  'TE2', 'TE3', 'TE4', 'TE5', 'TE6', 'TE7', 'TE8', 'TE9', 
                  'WB1', 'WB2', 'WB3', 
                  'WN1', 'WN10', 'WN2', 'WN3', 'WN4', 'WN5', 
                  'WN6', 'WN7', 'WN8', 'WN9')


cs_raw <- aws.s3::s3read_using(
  FUN = read.csv,
  object = cs_path,
  bucket = bucket20
) %>%
  janitor::clean_names() %>%
  select(-c(frn, application_type, contract_id))

cs <- cs_raw %>%
  #format dates
  mutate(agreement_end_date = standardise_dates(agreement_end_date),
         agreement_start_date = standardise_dates(agreement_start_date),
  parcel_id = stringr::str_replace_all(parcel_id, ' ', ''), #empty values
  parcel_id = ifelse(stringr::str_length(parcel_id) < 10 & stringr::str_length(parcel_id) > 0,
                     paste0(substr(parcel_id, 1, 2), stringr::str_pad(substr(parcel_id, 3, stringr::str_length(parcel_id)), width = 8, side = "left", pad = "0")),
                     parcel_id),) %>% #parcel_id format fix
  #validation
  dplyr::filter(lubridate::year(agreement_start_date) <= lubridate::year(Sys.Date())+1) %>%
  #only keep live agreements
  filter(agreement_status == "AGREEMENT LIVE",
         css_options_year == year(Sys.Date()),
         agreement_end_date >= cutoff_date, 
         unit_of_measurement %in% c("HA", "Metres"),
         !parcel_id == "") %>%
  # calculating annual value:
  #for capital options if the agreements is in the first 3 years, divide the value by 3 (since they can claim for the first 3 years)
  # if the agreement is after the 3rd year, don't count the capital options since they will already have been claimed
  # for revenue options, divide the value by the length of the agreement. 
                dplyr::mutate(contract_years = interval(agreement_start_date, agreement_end_date) %/% years(1) + 1,
                year_in_agreement = css_options_year - lubridate::year(agreement_start_date) + 1,
                first_three_years = dplyr::case_when(year_in_agreement <= 3 ~ TRUE,
                                                     year_in_agreement > 3 ~ FALSE),
                #area calculation (unit-wise)
                area = dplyr::case_when(unit_of_measurement == "HA" ~ measurement_value,
                                        unit_of_measurement == "Metres" ~  (measurement_value*2)/1e4),
                unit_of_measurement = dplyr::case_when(unit_of_measurement == "HA" ~ "ha",
                                                       unit_of_measurement == "Metres" ~  "m"),
                contract_years = dplyr::case_when(options_code %in% capital_opts & first_three_years == TRUE ~ 3, # capital items have to be claimed within the first 3 years
                                                  options_code %in% capital_opts & first_three_years == FALSE ~ 0,
                                                  !options_code %in% capital_opts ~ contract_years),
                annual_payment = payment / contract_years,
                scheme = "CS") %>% # this assumes capital items are claimed evenly across the first three years.
  select(-c(payment, css_options_year, measurement_value, agreement_status,
            first_three_years, year_in_agreement, contract_years))

rm(cs_raw)  

# ES----------------------------------------------------------------------------

es_raw <- aws.s3::s3read_using(
  FUN = read.csv,
  object = es_path,
  bucket = bucket20
) %>%
  janitor::clean_names() %>%
  select(-c(option_start_date, option_end_date))

es <- es_raw %>%
  rename(parcel_id = parcel_ref,
         options_code = opt,
         application_id = agreement_ref,
         unit_of_measurement = unit_measure) %>%
  #format dates
  mutate(agreement_start_date = standardise_dates(agreement_start_date),
         agreement_end_date = standardise_dates(agreement_end_date),
         parcel_id = stringr::str_replace_all(parcel_id, ' ', ''),
         parcel_id = ifelse(stringr::str_length(parcel_id) < 10 & stringr::str_length(parcel_id) > 0,
                            paste0(substr(parcel_id, 1, 2), stringr::str_pad(substr(parcel_id, 3, stringr::str_length(parcel_id)), width = 8, side = "left", pad = "0")),
                            parcel_id)
  ) %>%
  dplyr::filter(lubridate::year(agreement_start_date) <= lubridate::year(Sys.Date())+1) %>%
  filter(agreement_end_date >= cutoff_date,
         !parcel_id == "") %>%
  dplyr::mutate(scheme = "ES",
  area = dplyr::case_when(unit_of_measurement == "ha" ~ opt_quantity,
                          unit_of_measurement == "m" ~  (opt_quantity*2)/1e4,
                          unit_of_measurement %in% c("Units","Tonnes","Trees",
                                                     "Cubed Meters","Pounds",
                                                     "Plant", "Variety") ~ 0),
  annual_payment = 0,
  sbi = NA) %>%
  select(-c(opt_quantity))

rm(es_raw) 
  
# SFI22-------------------------------------------------------------------------

sfi22_raw <- aws.s3::s3read_using(
  FUN = read.csv,
  object = sfi22_path,
  bucket = bucket20
) %>%
  janitor::clean_names() %>%
  select(-c(nuts2, frn, verified_amb,
            verified_area, app_start_date, app_end_date))

sfi22 <- sfi22_raw %>%
  dplyr::rename(parcel_id = parcel_number,
                application_id = app_id,
                options_code = claimed_amb,
                area = claimed_area,
                annual_payment = annual_value_of_option,
                agreement_start_date = agreement_start,
                agreement_end_date = agreement_end) %>%
  dplyr::mutate(agreement_start_date = standardise_dates(agreement_start_date),
                agreement_end_date = standardise_dates(agreement_end_date),
                parcel_id = stringr::str_replace_all(parcel_id, ' ', ''),
                parcel_id = ifelse(stringr::str_length(parcel_id) < 10 & stringr::str_length(parcel_id) > 0,
                   paste0(substr(parcel_id, 1, 2), stringr::str_pad(substr(parcel_id, 3, stringr::str_length(parcel_id)), width = 8, side = "left", pad = "0")),
                   parcel_id)) %>%
  filter(agreement_end_date >= cutoff_date, 
         !parcel_id == "",
         status %in% c("AGREEMENT LIVE", "PAID", "ANNUAL DECLARATION AVAILABLE", 
                       "ANNUAL DECLARATION PENDING", "IN YEAR CHECKS", 
                       "POST PAYMENT ADJUSTMENT")) %>%
  mutate(scheme = "SFI22",
         unit_of_measurement = NA) %>%
  select(-status)

rm(sfi22_raw)

# SFI23-------------------------------------------------------------------------

sfi23_raw <- aws.s3::s3read_using(
  FUN = read.csv,
  object = sfi23_path,
  bucket = bucket20
) %>%
  janitor::clean_names() %>%
  select(-c(frn, agr_opt_year, parcel_id))

sfi23 <- sfi23_raw %>%
  rename(parcel_id = parcelref,
         options_code = option_code,
         unit_of_measurement = uom) %>%
  dplyr::mutate(parcel_id = stringr::str_replace_all(parcel_id, ' ', ''),
                parcel_id = ifelse(stringr::str_length(parcel_id) < 10 & stringr::str_length(parcel_id) > 0,
                                   paste0(substr(parcel_id, 1, 2), stringr::str_pad(substr(parcel_id, 3, stringr::str_length(parcel_id)), width = 8, side = "left", pad = "0")),
                                   parcel_id),) %>%
  filter(opt_year == year(Sys.Date()),
         application_status == "AGREEMENT LIVE",
         unit_of_measurement %in% c("HA", "Metres")) %>%
  dplyr::mutate(area = dplyr::case_when(unit_of_measurement == "HA" ~ option_quantity,
                                        unit_of_measurement == "Metres" ~  (option_quantity*2)/1e4),
                unit_of_measurement = dplyr::case_when(unit_of_measurement == "HA" ~ "ha",
                                                       unit_of_measurement == "Metres" ~  "m"),
                annual_payment = 0,
                agreement_start_date = NA,
                agreement_end_date = NA,
                scheme = "SFI23") %>%
  select(-c(application_status, opt_year, option_quantity))

rm(sfi23_raw)

# SFI24-------------------------------------------------------------------------

sfi24_raw <- aws.s3::s3read_using(
  FUN = read.csv,
  object = sfi24_path,
  bucket = bucket20
) %>%
  janitor::clean_names() %>%
  select(-c(frn, nuts2, app_start_date, app_end_date, contract_status, payment_rate))

sfi24 <- sfi24_raw %>%
  rename(parcel_id = parcel_number,
         annual_payment = payment_amount,
         options_code = option_description) %>%
  dplyr::mutate(agreement_start_date = standardise_dates(agreement_start_date),
                agreement_end_date = standardise_dates(agreement_end_date),
                parcel_id = stringr::str_replace_all(parcel_id, ' ', ''),
                parcel_id = ifelse(stringr::str_length(parcel_id) < 10 & stringr::str_length(parcel_id) > 0,
                                   paste0(substr(parcel_id, 1, 2), stringr::str_pad(substr(parcel_id, 3, stringr::str_length(parcel_id)), width = 8, side = "left", pad = "0")),
                                   parcel_id),
                unit_of_measurement = case_when(
                  !is.na(area) ~ "ha",
                  !is.na(metres) ~ "m",
                  !is.na(units) ~ "units",
                  TRUE ~ NA_character_)) %>%
  filter(year == year(Sys.Date()),
         application_status == "AGREEMENT LIVE",
         unit_of_measurement %in% c("ha", "m")) %>%
  dplyr::mutate(area = dplyr::case_when(unit_of_measurement == "ha" ~ area,
                                        unit_of_measurement == "m" ~  (metres*2)/1e4),
                scheme = "SFI") %>%
  select(-c(application_status, year, metres, units))

rm(sfi24_raw) 

# COMBINE-----------------------------------------------------------------------

aes <- rbind(cs, es, sfi22, sfi23, sfi24)
rm(cs, es, sfi22, sfi23, sfi24)

sf <- lubridate::stamp("19990117") #date template
# apply the template to today's date
date <- sf(lubridate::ymd(Sys.Date()))

# add date to filename
filename <- paste0("aes_parcel_level/data/aes_",date, ".parquet")

# write to s3 as parquet
aws.s3::s3write_using(
  x = aes, 
  FUN = arrow::write.parquet,
  row.names = FALSE,
  object = filename,
  bucket = 's3-ranch-020',
  opts = list('headers'=list('x-amz-acl'='bucket-owner-full-control'))
)