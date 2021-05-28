# Import libraries
library(data.table)
library(readr)
library(dplyr)
library(haven)
library("doMC")
library(future)
library(vroom)
library(RCurl)
library(googledrive)
library(ipumsr)
library(zoo)
library(collections)
plan(multiprocess)
# disable scientific notation
options(scipen = 999)
registerDoMC(cores = 4)

# Set to my personal directory - adjust accordingly
setwd("~/Google Drive/Non-Academic Work/Research/Traina/occupation-productivity/")

ACS_url <- "https://drive.google.com/file/d/1uu-MMSrEFL-QC5TbVXlfUC7OMKMlI9Dj/view?usp=sharing"
fname <- "Datasets/Imported/ACS/usa_00008.dat"
if (!(file.exists(fname))) {
  ret <- drive_download(ACS_url, fname, overwrite = TRUE)
}

# read in ACS data
ddi <- read_ipums_ddi("Datasets/Imported/ACS/usa_00008.xml")
data <- data.table(read_ipums_micro(ddi))

# Handling occupation/industry grouping
# remove unneeded columns
columns <- c(
  "YEAR", "SAMPLE", "SERIAL", "CBSERIAL", "CLUSTER", "STRATA", "PERWT",
  "OCC1990", "OCC2010", "OCCSOC", "INDNAICS", "INDNAICS"
)
data_naics <- data %>%
  select(columns) %>%
  filter(YEAR > 2000)

# aggregate occupation count
count_occsoc <- data_naics %>%
  group_by(OCCSOC) %>%
  summarise(count = sum(PERWT))
# aggregate population count per year
count_occsoc_year <- data_naics %>%
  group_by(OCCSOC, YEAR) %>%
  summarise(count = sum(PERWT))
# group occupations by industry and aggregate population counts
count_occsoc_indnaics <- data_naics %>%
  group_by(OCCSOC, INDNAICS) %>%
  summarise(count = sum(PERWT))
# group occupations by industry and aggregate population counts for each year
count_occsoc_indnaics_year <- data_naics %>%
  group_by(OCCSOC, INDNAICS, YEAR) %>%
  summarise(count = sum(PERWT))
# creating dictionary to match occupation codes with total number of people in occupation
occ_sum_dict <- dict(
  items = count_occsoc$count,
  keys = count_occsoc$OCCSOC
)
occyear <- paste(count_occsoc_year$OCCSOC, count_occsoc_year$YEAR, sep = "")
occ_sum_dict_year <- dict(
  items = count_occsoc_year$count,
  keys = occyear
)
# dictonary matching function
f <- function(x, y) y$get(x)
# adding occupation total people column to find percentage of people in industry in occupation
count_occsoc_indnaics$tot_sum <- unlist(lapply(count_occsoc_indnaics$OCCSOC, f, occ_sum_dict))
count_occsoc_indnaics$percent <- count_occsoc_indnaics$count / count_occsoc_indnaics$tot_sum * 100
occyear_tsum <- paste(count_occsoc_indnaics_year$OCCSOC, count_occsoc_indnaics_year$YEAR, sep = "")
count_occsoc_indnaics_year$tot_sum <- unlist(lapply(occyear_tsum, f, occ_sum_dict_year))
count_occsoc_indnaics_year$percent <- count_occsoc_indnaics_year$count / count_occsoc_indnaics_year$tot_sum * 100

# reading in productivity dataset
ctypes <- unlist("nccccnnn")
lp_current <- read_delim(file = "Datasets/Cleaned/lp_current.csv", delim = ",", col_types = ctypes)
# extracting a list of all NAICS industries in the productivity dataset,.
lp_naics <- lp_current %>%
  select(c("industry_code", "industry_text")) %>%
  distinct()

# exporting files
system.time(fwrite(lp_naics, "Datasets/Cleaned/lp_naics.csv", nThread = 10))
system.time(fwrite(count_occsoc_indnaics_year, "Datasets/Cleaned/occsoc_indnaics_year.csv", nThread = 10))
system.time(fwrite(count_occsoc_indnaics, "Datasets/Cleaned/occsoc_indnaics.csv", nThread = 10))
