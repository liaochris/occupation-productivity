# Purpose of this file is to read in the dataset on computerizability and clean it
# Will also use a O*NET SOC to OCC crosswalk to map this data to ACS data

library(readr)
library(vroom)
library(collections)
library(dplyr)
library(glue)
library(stringr)
library(data.table)
library(tidyr)
library(reshape2)
library(haven)
library(future)
library(doMC)
library(vroom)
library(easyr)
library(RCurl)
setwd("~/Google Drive/Non-Academic Work/Research/Traina/occupation-productivity/")


# read in data file on computerizability of individidual jobs
comp <- vroom(file = "Datasets/Imported/ONET Data/Computerizability/computerizability.txt", delim = " ")
comp_cleaned <- comp
# remove all label values that aren't "0" or "1"
comp_cleaned$Label <- as.numeric(comp_cleaned$Label)
# combine SOC codes split across the label and SOC column into the SOC column
comp_cleaned$SOC <- ifelse(is.na(comp_cleaned$Label), comp$Label, comp$SOC)

# remove all numerical values which represent SOC codes - in the occupation description columns
comp[4:6] <- apply(comp %>% select(`SOC`, `code`, `Occupation`), 2, function(x) gsub("[[:digit:]]+", "", x))
comp$SOC <- ifelse(comp$SOC == "-", NA, comp$SOC)
comp_cleaned$Occupation <- NULL
comp_cleaned$code <- NULL
# create occupation column that merges the separated occupation lists
comp_cleaned$Occupation <-
  (comp %>% unite("Occupation", c(`SOC`, `code`, `Occupation`), sep = " ", na.rm = TRUE))$Occupation
comp_cleaned <- comp_cleaned %>% rename("SOC code" = "SOC")
comp_cleaned$`SOC code` <- as.character(gsub("-", "", comp_cleaned$`SOC code`))

# read in crosswalks between 2000 and 2010 SOC, and 2010 and 2018 SOC
# clean both dataframes
naics0010 <- readxl::read_xls(path = "Datasets/Imported/ONET Data/Computerizability/soc_2000_to_2010_crosswalk.xls")
soc0010 <- c("2000 SOC code", "2010 SOC code")
colnames(naics0010) <- naics0010[6, ]
naics0010 <- naics0010[8:dim(naics0010)[1], ]
naics0010[, soc0010] <- apply(naics0010[, soc0010], 2, function(x) gsub("-", "", x))
naics0010_change <- naics0010 %>% filter(naics0010$`2000 SOC code` != naics0010$`2010 SOC code`)

# 2010 - 2018
naics1018 <- readxl::read_xlsx(path = "Datasets/Imported/ONET Data/Computerizability/soc_2010_to_2018_crosswalk.xlsx")
soc1018 <- c("2010 SOC Code", "2018 SOC Code")
colnames(naics1018) <- naics1018[8, ]
naics1018 <- naics1018[9:dim(naics1018)[1], ]
naics1018[, soc1018] <- apply(naics1018[, soc1018], 2, function(x) gsub("-", "", x))
naics1018_change <- naics1018 %>% filter(naics1018$`2010 SOC Code` != naics1018$`2018 SOC Code`)

# We will join the SOC column on the 2010 SOC code because after running the code below we found
# that the join on 2010 had the highest match rate
print(paste("2000 match rate", mean(unique(comp_cleaned$`SOC code`) %in% unique(naics0010$`2000 SOC code`))))
print(paste("2010 match rate", mean(unique(comp_cleaned$`SOC code`) %in% unique(naics0010$`2010 SOC code`))))
print(paste("2018 match rate", mean(unique(comp_cleaned$`SOC code`) %in% unique(naics1018$`2018 SOC Code`))))
comp_cleaned <- comp_cleaned %>% rename(`2010 SOC Code` = `SOC code`)
# assume 2018 2010 SOC codes are the same
comp_cleaned$`2018 SOC` <- comp_cleaned$`2010 SOC Code`
# replace 2018 values that were changed with the changed values
comp_cleaned <- comp_cleaned %>% left_join(naics1018_change %>% select(soc1018))
na_2018soc <- !is.na(comp_cleaned$`2018 SOC Code`)
comp_cleaned[na_2018soc, ]$`2018 SOC` <- comp_cleaned[na_2018soc, ]$`2018 SOC Code`
comp_cleaned$`2018 SOC Code` <- NULL

# assume 2010 and 2000 SOC codes are the same
comp_cleaned$`2000 SOC` <- comp_cleaned$`2010 SOC Code`
# replace 2018 values that were changed with the changed values
comp_cleaned <- comp_cleaned %>% left_join(naics0010_change %>% select(soc0010),
  by = c("2010 SOC Code" = "2010 SOC code")
)
na_2000soc <- !is.na(comp_cleaned$`2000 SOC code`)
comp_cleaned[na_2000soc, ]$`2000 SOC` <- comp_cleaned[na_2000soc, ]$`2000 SOC code`
comp_cleaned$`2000 SOC code` <- NULL

# combining all 3 columns into one SOC code column
comp_cleaned$`SOC Code` <- apply(
  comp_cleaned %>% select(`2010 SOC Code`, `2000 SOC`, `2018 SOC`),
  1, function(x) str_trim(Reduce(union, x[!is.na(x)]))
)
comp_cleaned <- unnest(comp_cleaned, `SOC Code`)
# removing old SOC code columns
comp_cleaned <- comp_cleaned %>% select(-c(`2010 SOC Code`, `2018 SOC`, `2000 SOC`))

# produces result with productivity score for occupation, SOC code for occupation (potential multiple matches)
# and occupation name, along with some other miscellaneous info like rank
fwrite(comp_cleaned, "Datasets/Cleaned/join_computerized.csv")
