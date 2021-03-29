library(readr)
library(vroom)
library(collections)
library(dplyr)
library(glue)
library(stringr)
library(data.table)

#change directory as appropriate
setwd("~/Google Drive/Non-Academic Work/Research/Traina/Productivity/")

#reading in Labor productivity data - Mapping Files
area <- read_delim("Datasets/Imported/Industry Data/ip.area.txt",
                   col_types = list(col_character(), col_character(), col_number(), col_character(), col_number()),
                   delim = "\t")
duration <- read_delim("Datasets/Imported/Industry Data/ip.duration.txt",
                       col_types = list(col_number(), col_character()), 
                       delim = "\t")
footnote <- read_delim("Datasets/Imported/Industry Data/ip.footnote.txt",
                       col_types = list(col_character(), col_character()), 
                       delim = "\t")
industry <- read_delim("Datasets/Imported/Industry Data/ip.industry.txt",
                       col_types = list(col_character(), col_character(), col_character(), col_number(), col_character(), col_number()),
                       delim = "\t")
measure <- read_delim("Datasets/Imported/Industry Data/ip.measure.txt",
                      col_types = list(col_character(), col_character(), col_number(), col_character(), col_number()), 
                      delim = "\t")
seasonal <- read_delim("Datasets/Imported/Industry Data/ip.seasonal.txt",
                       col_types = list(col_character(), col_character()), 
                       delim = "\t")
sector <- read_delim("Datasets/Imported/Industry Data/ip.sector.txt",
                     col_types = list(col_character(), col_character()), 
                     delim = "\t")
type <- read_delim("Datasets/Imported/Industry Data/ip.type.txt",
                   col_types = list(col_character(), col_character()), 
                   delim = "\t")

#reading in Labor productivity data - time series and data
ctypes_series <- list(col_character(), col_character(), col_character(), col_character(), 
                      col_character(), col_number(), col_character(), 
                      col_character(), col_character(), col_character(), col_character(),
                      col_number(), col_character(),col_number(), col_character())
series <- read_delim("Datasets/Imported/Industry Data/ip.series.txt",
                     col_types = ctypes_series,
                     delim = "\t")

data_current <- read_delim("Datasets/Imported/Industry Data/ip.data.0.Current.txt",
                           col_types = list(col_character(), col_number(), col_character(),  col_number(), 
                                            col_character()),
                           delim = "\t")

data_all <- read_delim("Datasets/Imported/Industry Data/ip.data.1.AllData.txt",
                           col_types = list(col_character(), col_number(), col_character(),  col_number(), 
                                            col_character()),
                           delim = "\t")


#expanding series
seasonal_dict = dict(items = seasonal$seasonal_text, keys = seasonal$seasonal_code)
type_dict = dict(items = type$type_text, keys = type$type_code)
sector_dict = dict(items = sector$sector_text, keys = sector$sector_code)
measure_dict = dict(items = measure$measure_text, keys = measure$measure_code)
industry_dict = dict(items = industry$industry_text, keys = industry$industry_code)
footnote_dict = dict(items = footnote$footnote_text, keys = footnote$footnote_code)
footnote_dict$set("", "")
area_dict = dict(items = area$area_text, keys = area$area_code)
duration_dict = dict(items = duration$duration_text, keys = duration$duration_code)

#function for matching key of dictionary to value 
f <- function(x, y) y$get(x)

#adding value of each dictionary text to dataframe and reordering
series_expanded <- series
series_expanded$seasonal_text <- lapply(series_expanded$seasonal, f, seasonal_dict)
series_expanded$seasonal_text <- unlist(series_expanded$seasonal_text)
target_col <- which(colnames(series_expanded) == "seasonal")
series_expanded <- series_expanded[,c(1:target_col, 
                                      length(colnames(series_expanded)), 
                                      (target_col+2):length(colnames(series_expanded))-1)]

series_expanded$type_text <- lapply(series_expanded$type_code, f, type_dict)
series_expanded$type_text <- unlist(series_expanded$type_text)
target_col <- which(colnames(series_expanded) == "type_code")
series_expanded <- series_expanded[,c(1:target_col, 
                                      length(colnames(series_expanded)), 
                                      (target_col+2):length(colnames(series_expanded))-1)]
series_expanded$sector_text <- lapply(series_expanded$sector_code, f, sector_dict)
series_expanded$sector_text <- unlist(series_expanded$sector_text)
target_col <- which(colnames(series_expanded) == "sector_code")
series_expanded <- series_expanded[,c(1:target_col, 
                                      length(colnames(series_expanded)), 
                                      (target_col+2):length(colnames(series_expanded))-1)]
series_expanded$measure_text <- lapply(series_expanded$measure_code, f, measure_dict)
series_expanded$measure_text <- unlist(series_expanded$measure_text)
target_col <- which(colnames(series_expanded) == "measure_code")
series_expanded <- series_expanded[,c(1:target_col, 
                                      length(colnames(series_expanded)), 
                                      (target_col+2):length(colnames(series_expanded))-1)]
series_expanded$industry_text <- lapply(series_expanded$industry_code, f, industry_dict)
series_expanded$industry_text <- unlist(series_expanded$industry_text)
target_col <- which(colnames(series_expanded) == "industry_code")
series_expanded <- series_expanded[,c(1:target_col, 
                                      length(colnames(series_expanded)), 
                                      (target_col+2):length(colnames(series_expanded))-1)]
series_expanded$area_text <- lapply(series_expanded$area_code, f, area_dict)
series_expanded$area_text <- unlist(series_expanded$area_text)
target_col <- which(colnames(series_expanded) == "area_code")
series_expanded <- series_expanded[,c(1:target_col, 
                                      length(colnames(series_expanded)), 
                                      (target_col+2):length(colnames(series_expanded))-1)]
series_expanded$duration_text <- lapply(series_expanded$duration_code, f, duration_dict)
series_expanded$duration_text <- unlist(series_expanded$duration_text)
target_col <- which(colnames(series_expanded) == "duration_code")
series_expanded <- series_expanded[,c(1:target_col, 
                                      length(colnames(series_expanded)), 
                                      (target_col+2):length(colnames(series_expanded))-1)]
#set NA values to ""
series_expanded$footnote_codes[is.na(series_expanded$footnote_codes)] <- ""
series_expanded$footnote_text <- lapply(series_expanded$footnote_codes, f, footnote_dict)
series_expanded$footnote_text <- unlist(series_expanded$footnote_text)
target_col <- which(colnames(series_expanded) == "footnote_codes")
series_expanded <- series_expanded[,c(1:target_col, 
                                      length(colnames(series_expanded)), 
                                      (target_col+2):length(colnames(series_expanded))-1)]

#merge data_current and series_expanded
#merge data_all and series_expanded
colnames(series_expanded) <- lapply(colnames(series_expanded), str_trim)
colnames(data_current) <- lapply(colnames(data_current), str_trim)
colnames(data_all) <- lapply(colnames(data_all), str_trim)
data_current$footnote_codes[is.na(data_current$footnote_codes)] <- ""
data_all$footnote_codes[is.na(data_all$footnote_codes)] <- ""
data_current_expanded <- left_join(data_current, series_expanded, by = c("series_id", "footnote_codes"))
data_all_expanded <- left_join(data_all, series_expanded, by = c("series_id", "footnote_codes"))

#filtering for just the labor productivity data
lp_current <- data_current_expanded %>% filter(measure_code == 'L00')
lp_all <- data_all_expanded %>% filter(measure_code == 'L00')

system.time(fwrite(lp_current, "Datasets/Cleaned/lp_current.csv", nThread = 10))
system.time(fwrite(lp_all, "Datasets/Cleaned/lp_all.csv", nThread = 10))
