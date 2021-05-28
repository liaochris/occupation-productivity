# This is the code I used to make a crosswalk between the INDNAICS codes used in the ACS data
# and the NAICS codes that were used in the BLS Labor Productivity data

# Import libraries
library(tidyr)
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
library(glue)
library(stringr)
library(tidyverse)
library(easyr)
# disable scientific notation
options(scipen = 999)
registerDoMC(cores = 4)

# Set to my personal directory - adjust accordingly
setwd("~/Google Drive/Non-Academic Work/Research/Traina/occupation-productivity/")

# read in productivity data - all the NAICS codes that we have BLS productivity data for
lp_naics <- read_delim(file = "Datasets/Cleaned/lp_naics.csv", delim = ",")
# remove prefix N from NAICS classifications and "_" suffixes
# makes it easier to merge with INDNAICS codes for direct matches between INDNAICS and NAICS codes
ind_codes <- substr(lp_naics$industry_code, start = 2, stop = 7)
ind_codes <- str_replace_all(ind_codes, "_", "")
lp_naics$ind_codes_mod <- ind_codes

# Next I will read in files that describe NAICS code changes over time
# BLS Labor Productivity data uses NAICS codes from the 2017 classification but our ACS data goes back to 2005
# that means we want to have NAICS codes for 2017, 2012, 2007 and 2002 for each industry
# This meand that when we merge our data with the ACS data we don't lose industries simply beacuse the NAICS code changed over time
# reading in data
naics0207 <- readxl::read_xls(path = "Datasets/Imported/NAICS/2007_to_2002_NAICS.xls")
naics0712 <- readxl::read_xls(path = "Datasets/Imported/NAICS/2012_to_2007_NAICS.xls")
naics1217 <- readxl::read_xlsx(path = "Datasets/Imported/NAICS/2017_to_2012_NAICS.xlsx")[, 1:4]

# Cleaning the crossyear NAICS crosswalks
cleanNAICS <- function(x) {
  colnames(x) <- x[2, ]
  x <- x[3:dim(x)[1], ]
}
naics0207 <- cleanNAICS(naics0207)
naics0712 <- cleanNAICS(naics0712)
naics1217 <- cleanNAICS(naics1217)

# selecting only columns where NAICS codes changed
naics0207_change <- (naics0207 %>% filter(`2007 NAICS Code` != `2002 NAICS Code`))[, c(1, 3)]
naics0712_change <- (naics0712 %>% filter(`2012 NAICS Code` != `2007 NAICS Code`))[, c(1, 3)]
naics1217_change <- (naics1217 %>% filter(`2017 NAICS Code` != `2012 NAICS Code`))[, c(1, 3)]

# assume that all codes do not change over time, then change the codes that change over time
lp_naics$naics2017 <- lp_naics$ind_codes_mod
lp_naics$naics2012 <- lp_naics$ind_codes_mod

# replace values that changed with changed values
lp_naics <- lp_naics %>% left_join(naics1217_change, by = c("naics2017" = "2017 NAICS Code"))
lp_naics[!is.na(lp_naics$`2012 NAICS Code`), ]$naics2012 <- lp_naics[!is.na(lp_naics$`2012 NAICS Code`), ]$`2012 NAICS Code`
lp_naics$`2012 NAICS Code` <- NULL
# assume 2012 and 2007 are the same
lp_naics$naics2007 <- lp_naics$naics2012

# replace 2007 values that were changed with the changed values
lp_naics <- lp_naics %>% left_join(naics0712_change, by = c("naics2012" = "2012 NAICS Code"))
lp_naics[!is.na(lp_naics$`2007 NAICS Code`), ]$naics2007 <- lp_naics[!is.na(lp_naics$`2007 NAICS Code`), ]$`2007 NAICS Code`
lp_naics$`2007 NAICS Code` <- NULL
# assume 2007 and 2002 are the same
lp_naics$naics2002 <- lp_naics$naics2007

# replace 2002 values that were changed with the changed values
lp_naics <- lp_naics %>% left_join(naics0207_change, by = c("naics2007" = "2007 NAICS Code"))
lp_naics[!is.na(lp_naics$`2002 NAICS Code`), ]$naics2002 <- lp_naics[!is.na(lp_naics$`2002 NAICS Code`), ]$`2002 NAICS Code`
lp_naics$`2002 NAICS Code` <- NULL
# remove ind_codes_mod column - unneded
lp_naics$ind_codes_mod <- NULL

lp_naics$naics <- apply(
  lp_naics %>% select("naics2017", "naics2012", "naics2007", "naics2002"),
  1, function(x) str_trim(Reduce(union, x[!is.na(x)]))
)
lp_naics <- unnest(lp_naics, "naics")

# Now we have a crosswalk between the NAICS code used in the BLS Labor Productivity data and the corresponding NAICS codes
# from other years


# The next step is to create the crosswalk between INDNAICS and NAICS codes
# read in crosswalk for converting between indnaics and naics
# Cleaning the data
indnaics_crosswalk <- read_delim(
  file = "Datasets/Imported/ACS/2003indnaics.csv",
  delim = ","
)
indnaics_crosswalk <- indnaics_crosswalk[3:dim(indnaics_crosswalk)[1], ]
colnames(indnaics_crosswalk) <- trimws(colnames(indnaics_crosswalk))
# remove invalid values that provide no information
indnaics_crosswalk <- indnaics_crosswalk %>%
  filter(!is.na(indnaics_crosswalk$`2007 NAICS EQUIVALENT`)) %>%
  filter(`2007 NAICS EQUIVALENT` != "------") %>%
  filter(`2002 NAICS EQUIVALENT` != "N/A")

print("Parts of")
mean(unlist(lapply(indnaics_crosswalk$`2007 NAICS EQUIVALENT`, function(x) grepl("Part of", x))))
print("Pts")
mean(unlist(lapply(indnaics_crosswalk$`2007 NAICS EQUIVALENT`, function(x) grepl("Pts", x))))
print("-")
mean(unlist(lapply(indnaics_crosswalk$`2007 NAICS EQUIVALENT`, function(x) grepl("-", x))))
print("exc")
mean(unlist(lapply(indnaics_crosswalk$`2007 NAICS EQUIVALENT`, function(x) grepl("exc", x))))
print(",")
mean(unlist(lapply(indnaics_crosswalk$`2007 NAICS EQUIVALENT`, function(x) grepl(",", x))))

c_replace <- c("\\)", "\\(", "\\)", "pt. ", "Part of ", "Pts. ", " ")
for (c in c_replace) {
  indnaics_crosswalk[["2007 NAICS EQUIVALENT"]] <- gsub(c, "", indnaics_crosswalk[["2007 NAICS EQUIVALENT"]])
  indnaics_crosswalk[["2002 NAICS EQUIVALENT"]] <- gsub(c, "", indnaics_crosswalk[["2002 NAICS EQUIVALENT"]])
  indnaics_crosswalk[["2007 NAICS EQUIVALENT"]] <- gsub("and", ",", indnaics_crosswalk$`2007 NAICS EQUIVALENT`)
  indnaics_crosswalk[["2002 NAICS EQUIVALENT"]] <- gsub("and", ",", indnaics_crosswalk$`2002 NAICS EQUIVALENT`)
}
# fixing known issues in the data
indnaics_crosswalk[indnaics_crosswalk[, 3] == "81,211,381,219", ][, 3:4] <- list("812113, 81219", "812113, 81219")
indnaics_crosswalk[indnaics_crosswalk[, 3] == "321,991,321,992", ][, 3:4] <- list("321991, 321992", "321991, 321992")
indnaics_crosswalk[indnaics_crosswalk[, 3] == "45412003-2004", ][, 3] <- list("4541")

# show that all items that say "exclude" are the same between 2002 NAICS and 2007 NAICS
filt07 <- grepl("exc.", indnaics_crosswalk$`2007 NAICS EQUIVALENT`)
filt02 <- grepl("exc.", indnaics_crosswalk$`2002 NAICS EQUIVALENT`)

# shift all values that should be excluded to a new column
removeExclude <- function(x, col) {
  foreach(i = 1:dim(x)[1], .combine = "bind_rows") %do% {
    selRow <- x[i, ]
    val <- selRow[[col]]
    selRow[[col]] <- as.character(selRow[[col]])
    if (grepl("exc", val)) {
      start <- str_locate_all(pattern = "exc.", val)[[1]][1]
      end <- str_locate_all(pattern = "exc.", val)[[1]][2]
      selRow[col] <- substr(val, 1, start - 1)
      selRow[[paste("EXCLUDE", col - 2, sep = "")]] <- substr(val, end + 1, str_length(val))
    }
    else {
    }
    selRow
  }
}
indnaics_crosswalk <- removeExclude(indnaics_crosswalk, 3)
indnaics_crosswalk <- removeExclude(indnaics_crosswalk, 4)


# clean up columns where 2007 and 2002 NAICS codes do not align have discrepancies
unequal <- indnaics_crosswalk$`2002 NAICS EQUIVALENT` != indnaics_crosswalk$`2007 NAICS EQUIVALENT`
fixedCol <- indnaics_crosswalk[unequal, ]
# the only cleaning that has to be done is to split values separated by a string into two columns
# so this is fairly simple
# can also avoid using for loop when splitting by commas because the max number of commas is one in this case
fixedCol <- foreach(i = 1:sum(unequal), .combine = "bind_rows") %do% {
  if (grepl(",", fixedCol[[i, 3]])) {
    nums <- str_split(fixedCol[[i, 3]], ",")[[1]]
    p1 <- fixedCol[i, ]
    p2 <- fixedCol[i, ]
    p1[[3]] <- nums[1]
    p2[[3]] <- nums[2]
    rbind(p1, p2)
  }
  else {
    fixedCol[i, ]
  }
}
# data where rows for 2002 and 2007 naics are equal
unfixedCol <- indnaics_crosswalk[!unequal, ]

# function to clean the strings separated by commas or dashes
# for further detail examine the imported files or the dataframe created before the function is called onit
cleanStrings <- function(df, col_ind) {
  temp <- foreach(i = 1:dim(df)[1], .combine = "bind_rows") %do% {
    # select row being modified
    selRow <- df[i, ]
    val <- selRow[[col_ind]]
    selRow[[col_ind]] <- as.character(selRow[[col_ind]])
    # check if there is a dash and a comma in this row
    if (grepl("-", val) & grepl(",", val)) {
      start <- str_locate_all(pattern = "-", val)[[1]][1]
      end <- str_locate_all(pattern = "-", val)[[1]][2]
      commalst <- str_locate_all(pattern = ",", val)[[1]]
      commaend <- commalst[dim(commalst)[1], ][1]
      startnum <- as.numeric(substr(val, commaend + 1, start - 1))
      endnum <- as.numeric(substr(val, end + 1, str_length(val)))
      # separate each component in the dash into its own row
      selRow_temp1 <- foreach(i = startnum:endnum, .combine = "bind_rows") %do% {
        temp <- selRow
        temp[col_ind] <- i
        temp
      }

      vallist <- strsplit(val, ",")[[1]]
      vallist <- vallist[1:length(vallist) - 1]
      len <- length(vallist)
      # separate each component separated by a comma into its own row
      selRow_temp2 <- foreach(i = 1:len, .combine = "bind_rows") %do% {
        temp <- selRow
        temp[col_ind] <- vallist[i]
        temp
      }
      # combine the rows
      selRow <- rbind(selRow_temp1, selRow_temp2) %>% arrange("NAICS_mod")
      selRow[[col_ind]] <- as.character(selRow[[col_ind]])
    }
    # separate if there is just a dash in between
    # procedure is just the first part of the if statement
    else if (grepl("-", val)) {
      start <- str_locate_all(pattern = "-", val)[[1]][1]
      end <- str_locate_all(pattern = "-", val)[[1]][2]
      startnum <- as.numeric(substr(val, 1, start - 1))
      endnum <- as.numeric(substr(val, end + 1, str_length(val)))
      selRow_temp <- foreach(i = startnum:endnum, .combine = "bind_rows") %do% {
        temp <- selRow
        temp[col_ind] <- i
        temp
      }
      selRow <- selRow_temp
      selRow[[col_ind]] <- as.character(selRow[[col_ind]])
    }
    # separate if there are commas separating the values
    # procedure same as the second part of the if statement
    else if (grepl(",", val)) {
      vallist <- strsplit(val, ",")[[1]]
      len <- length(vallist)
      selRow_temp <- foreach(i = 1:len, .combine = "bind_rows") %do% {
        temp <- selRow
        temp[col_ind] <- vallist[i]
        temp
      }
      selRow <- selRow_temp
      selRow[[col_ind]] <- as.character(selRow[[col_ind]])
    }
    # do nothing if no non-numeric characters in specific row-column pairing being examined
    else {
    }
    # return row
    selRow
  }
  # return created dataframe
  temp
}
# clean strings for 2007 NAICS column
indnaics_crosswalk_mod_fs <- cleanStrings(unfixedCol, 4)
# can do this because checked earlier that this dataframe has identical 2002 and 2007 naics values
indnaics_crosswalk_mod_fs$`2002 NAICS EQUIVALENT` <- indnaics_crosswalk_mod_fs$`2007 NAICS EQUIVALENT`
indnaics_crosswalk_mod <- rbind(indnaics_crosswalk_mod_fs, fixedCol)

# now we will add the 2012 and 2017 naics columns to this crosswalk using a similar procedure as earlier
# first assume NAICS 2007 and NAICS 2012 are the same
indnaics_crosswalk_mod$`2012 NAICS EQUIVALENT` <- indnaics_crosswalk_mod$`2007 NAICS EQUIVALENT`
# replace changed values with the changes
indnaics_crosswalk_mod <- indnaics_crosswalk_mod %>%
  left_join(naics0712_change, by = c(`2007 NAICS EQUIVALENT` = "2007 NAICS Code"))
indnaics_crosswalk_mod[!is.na(indnaics_crosswalk_mod$`2012 NAICS Code`), ]$`2012 NAICS EQUIVALENT` <-
  indnaics_crosswalk_mod[!is.na(indnaics_crosswalk_mod$`2012 NAICS Code`), ]$`2012 NAICS Code`
indnaics_crosswalk_mod$`2012 NAICS Code` <- NULL

# next assume NAICS 2012 and NAICS 2017 are the same
indnaics_crosswalk_mod$`2017 NAICS EQUIVALENT` <- indnaics_crosswalk_mod$`2012 NAICS EQUIVALENT`
# replace changed values with the changes
indnaics_crosswalk_mod <- indnaics_crosswalk_mod %>%
  left_join(naics1217_change, by = c(`2012 NAICS EQUIVALENT` = "2012 NAICS Code"))
indnaics_crosswalk_mod[!is.na(indnaics_crosswalk_mod$`2017 NAICS Code`), ]$`2017 NAICS EQUIVALENT` <-
  indnaics_crosswalk_mod[!is.na(indnaics_crosswalk_mod$`2017 NAICS Code`), ]$`2017 NAICS Code`
indnaics_crosswalk_mod$`2017 NAICS Code` <- NULL

# manually adding some rows that were lost/obfuscated due to parentheses
# added 5913 - 5913 only existed after 2005, so not included as an entry because it was not included in the
# NAICS to INDNAICS crosswalk which only links 2003 INDNAICS to 2003 NAICS, so had to be manuallyadded
# added 454111 with the same data as 4541, in the indnaics column it says that 4541 only applies to 2003-2004
# 45411 was post-2005, crosswalk only links 2003 INDNAICS to 2003 NAICS and does not include INDNAICS values added later

indnaics_crosswalk_mod <-
  rbind(
    indnaics_crosswalk_mod,
    c(
      "6672", "51913", "51913", "51913",
      "Internet publishing and broadcasting and web search portals", NA, NA, "51913", "51913"
    ),
    c(
      "5590", "454111", "4541", "454111", "Electronic shopping and mail-order houses",
      NA, NA, "454111", "454110"
    )
  )

# read in crosswalk for indnaics codes across years
indnaics_crosswalk_years <-
  read_delim(
    file = "Datasets/Imported/ACS/indnaics_crosswalk_2000_onward_without_code_descriptions.csv",
    delim = ","
  )
# filter out unneeded columns
indnaics_crosswalk_years$X1 <- NULL
indnaics_crosswalk_years <- indnaics_crosswalk_years[, c(4:dim(indnaics_crosswalk_years)[2])]

# columns for indexing
indnaics_cols <- colnames(indnaics_crosswalk_years)[1:4]

# remove rows where all values are NA
indnaics_crosswalk_years <- indnaics_crosswalk_years %>%
  filter(rowSums(is.na(indnaics_crosswalk_years[indnaics_cols])) != 4)
# trim whitespace
indnaics_crosswalk_years[indnaics_cols] <- apply(indnaics_crosswalk_years[indnaics_cols], 2, trimws)

# basically because of the way the data is organized (look at indnaics_crosswalk_years)
# there are some rows that have the same/similar codes and industries that can be aggregated but were not
# an interesting characteristic of these rows is that the sum of their non-empty elements is 5 across both rows
# we use the column rs to identify such column pairings
match_lead <- data.table(apply(indnaics_crosswalk_years[1:(dim(indnaics_crosswalk_years)[1] - 1), indnaics_cols] != "", 2, as.numeric))
match_lag <- data.table(apply(indnaics_crosswalk_years[2:(dim(indnaics_crosswalk_years)[1]), indnaics_cols] != "", 2, as.numeric))
match_lead[is.na(match_lead)] <- 0
match_lag[is.na(match_lag)] <- 0
matching_codes <- match_lag + match_lead

# checking if for both rows, there is a 1 in just 1 column for each of the 4
# ie examples:
# 1 1 0 0
# 0 0 1 1  works
# 1 1 1 0
# 0 0 1 0  does not work even though both add up to 4
matching_codes$valid <- apply(matching_codes, 1, function(x) Reduce(intersect, x == 1))
# coercing to booleans
matching_codes$valid <- ifelse(matching_codes$valid == TRUE, TRUE, FALSE)
# logical(0) got converted to NA so filling those in as false
matching_codes[is.na(matching_codes$valid)]$valid <- FALSE
# setting both merge columns to TRUE or TRUE2
for (i in 2:dim(matching_codes)[1]) {
  if (matching_codes$valid[i - 1] == TRUE) {
    matching_codes$valid[i] <- "TRUE2"
  }
  else {
    matching_codes$valid[i] <- matching_codes$valid[i]
  }
}
matching_codes$valid <- ifelse(matching_codes$valid == "TRUE2", TRUE, matching_codes$valid)
indnaics_crosswalk_years$merge <- c(matching_codes$valid, TRUE)

indnaics_crosswalk_years_m <- indnaics_crosswalk_years %>% filter(tobool(indnaics_crosswalk_years$merge))
indnaics_crosswalk_years_m$group <- rep(1:(dim(indnaics_crosswalk_years_m)[1] / 2), each = 2)
# change NA to "" so that paste function joins correctly
indnaics_crosswalk_years_m[is.na(indnaics_crosswalk_years_m)] <- ""
indnaics_crosswalk_years_m <- indnaics_crosswalk_years_m %>%
  group_by(`group`) %>%
  summarise_all(~ (trimws(paste(., collapse = ":"))))
indnaics_crosswalk_years_m$group <- NULL
indnaics_crosswalk_years_m$`Industry Title` <- NULL
indnaics_crosswalk_years_m$merge <- NULL
# clean strings
indnaics_crosswalk_years_m <- data.table(apply(indnaics_crosswalk_years_m, 2, function(x) trimws(gsub(":", "", x))))

# produce two of each row to laod into original dataframe to preserve structure - will eventually remove duplicates
indnaics_crosswalk_years[tobool(indnaics_crosswalk_years$merge), ][indnaics_cols] <-
  indnaics_crosswalk_years_m %>% slice(rep(1:n(), each = 2))

# removing duplicates
indnaics_crosswalk_years <- indnaics_crosswalk_years[, indnaics_cols] %>% distinct()

# examined data, manually addressing some issues
direct_grouping <- function(x) {
  temp <- x
  temp[is.na(temp)] <- ""
  temp$code <- 1
  temp <- aggregate(. ~ code,
    data = temp,
    FUN = "paste", collapse = ""
  )
  temp$code <- NULL
  temp
}
pulling <- function(x, status) {
  na.locf(x, fromLast = status)
}
replace_rows <- function(data, replacement, startind, len) {
  df <- data
  if (dim(replacement)[1] > len + 1) {
    ?stop
  }
  else {
    repl_ind <- c(startind:startind + dim(replacement)[1] - 1)
    df[repl_ind, ] <- replacement

    rem_ind <- c((startind + dim(replacement)[1]):(startind + len))
    df <- df[-rem_ind, ]
  }
  df
}

# direct match
#    316M	NA	  NA	  NA
#    NA	  316M	316M	NA
#    NA	  NA	  NA	  316M
# becomes
#    316M	316M	316M	316M

startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "316M")
replacement <- direct_grouping(indnaics_crosswalk_years[seq(startind, startind + 2), ])
indnaics_crosswalk_years <- replace_rows(indnaics_crosswalk_years, replacement, startind, 2)

# direct match
# ie: 51M	NA	NA	NA
#    NA	  515	515	NA
#    NA	  NA	NA	515
# becomes
#    51M	515	515	515
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "51M")
replacement <- direct_grouping(indnaics_crosswalk_years[seq(startind, startind + 2), ])
indnaics_crosswalk_years <- replace_rows(indnaics_crosswalk_years, replacement, startind, 2)

# pull up
#    8114Z	NA	NA	NA
#    81143	8114	8114	8114
# becomes
#    8114Z	8114	8114	8114
#    81143	8114	8114	8114
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "8114Z")
replacement <- pulling(indnaics_crosswalk_years[seq(startind, startind + 1), ], T)
indnaics_crosswalk_years[seq(startind, startind + 1), ] <- replacement

# pull up
#    333M	333M	333M	NA
#    333S	333S	333S	333MS
# becomes
#    333M	333M	333M	333MS
#    333S	333S	333S	333MS
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "333M")
replacement <- pulling(indnaics_crosswalk_years[seq(startind, startind + 1), ], T)
indnaics_crosswalk_years[seq(startind, startind + 1), ] <- replacement

# pull up
#    53223	53223	53223	NA
#    532M	  532M	532M	532M2
# becomes
#    53223	53223	53223	532M2
#    532M	  532M	532M	532M2
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "53223")
replacement <- pulling(indnaics_crosswalk_years[seq(startind, startind + 1), ], T)
indnaics_crosswalk_years[seq(startind, startind + 1), ] <- replacement


# pull down
# ie: 622	622	622	622M
#    NA	  NA	NA	6222
# becomes
#    622	622	622	622M
#    622	622	622	6222
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "622")
replacement <- pulling(indnaics_crosswalk_years[seq(startind, startind + 1), ], F)
indnaics_crosswalk_years[seq(startind, startind + 1), ] <- replacement

# pull down
#    4451	4451	4451	44511
#    NA	  NA	  NA	  44512
# becomes
#    4451	4451	4451	44511
#    4451	4451	4451	44512
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "4451")
replacement <- pulling(indnaics_crosswalk_years[seq(startind, startind + 1), ], F)
indnaics_crosswalk_years[seq(startind, startind + 1), ] <- replacement

# pull down and pull up (index 1 is down, index 3:4 is up)
#    323	323	  NA    NA
#    NA	  3231	3231	3231
#    becomes
#    323	323	  3231	3231
#    323	3231	3231	3231
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "323")
repl_down <- pulling(indnaics_crosswalk_years[seq(startind, startind + 1), 1], F)
repl_up <- pulling(indnaics_crosswalk_years[seq(startind, startind + 1), 2:4], T)
replacement <- cbind(repl_down, repl_up)
indnaics_crosswalk_years[seq(startind, startind + 1), ] <- replacement

# pull one down, two up
#    5191Z	5191Z	  NA	    NA
#    NA	    5191ZM	5191ZM	5191ZM
# becomes
#    5191Z	5191Z	   NA	    NA
#    5191Z  5191ZM	5191ZM	5191ZM
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "5191Z")
repl_down <- pulling(indnaics_crosswalk_years[seq(startind, startind + 1), 1], F)
repl_up <- pulling(indnaics_crosswalk_years[seq(startind, startind + 1), 2:4], T)
replacement <- cbind(repl_down, repl_up)
indnaics_crosswalk_years[seq(startind, startind + 1), ] <- replacement

# direct match 3 and pull down
#    711	711	NA	NA
#    NA	  NA	711	NA
#    NA	  NA	NA	7111
#    NA	  NA	NA	7112
#    NA	  NA	NA	711M
#    NA	  NA	NA	7115
# becomes
#    711	711	711	7111
#    711	711	711	7112
#    711	711	711	711M
#    711	711	711	7115
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "711")
replacement <- direct_grouping(indnaics_crosswalk_years[seq(startind, startind + 2), ])
indnaics_crosswalk_years <- replace_rows(indnaics_crosswalk_years, replacement, startind, 2)

startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "711")
replacement <- pulling(indnaics_crosswalk_years[seq(startind, startind + 3), ], F)
indnaics_crosswalk_years[seq(startind, startind + 3), ] <- replacement

# direct match 3 and then pull up
#    454111	454111	454111	NA
#    454112	454112	454112	NA
#    454113	454113	NA	    NA
#    NA	    NA	    454113	NA
#    NA	    NA	    NA	    454110
# becomes
#    454111	454111	454111	454110
#    454112	454112	454112	454110
#    454113	454113	454113	454110
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "454113")
replacement <- direct_grouping(indnaics_crosswalk_years[seq(startind, startind + 2), ])
indnaics_crosswalk_years <- replace_rows(indnaics_crosswalk_years, replacement, startind, 2)

startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "454111")
replacement <- pulling(indnaics_crosswalk_years[seq(startind, startind + 2), ], T)
indnaics_crosswalk_years[seq(startind, startind + 2), ] <- replacement

# manual adjustment - error created by consecutive rows of joins
startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "524")
indnaics_crosswalk_years[seq(startind, startind + 3), ] <-
  t(data.table(
    c("524", "524", "524", "5241"),
    c("524", "524", "524", "5242"),
    c("531", "531", "531", "531M"),
    c("531", "531", "531", "5313")
  ))

startind <- which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "5161")
indnaics_crosswalk_years[startind, ] <- t(data.table(c("51913", "51913", "51913", "51913")))

indnaics_crosswalk_years[which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "21S"), ] <- NA
indnaics_crosswalk_years[which(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` == "5181"), ] <- NA
indnaics_crosswalk_years <-
  indnaics_crosswalk_years %>% filter(rowSums(is.na(indnaics_crosswalk_years)) != ncol(indnaics_crosswalk_years))


# crosswalk robustness checks - make sure first two digits are generally the same

remParen <- function(x) {
  if (grepl("\\(", x)) {
    locs <- str_locate_all(x, "\\(")[[1]]
    trimws(substr(x, 0, locs - 1))
  }
  else {
    x
  }
}


# remove parentheses from all indnaics codes
indnaics_crosswalk_mod$indnaics <- unlist(lapply(indnaics_crosswalk_mod$`INDNAICS CODE 			(2003-onward ACS/PRCS)`, remParen))

indnaics_crosswalk_mod$indnaics <- trimws(indnaics_crosswalk_mod$indnaics)
indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE ` <- trimws(indnaics_crosswalk_years$`2003-2007 ACS/PRCS INDNAICS CODE `)




# join to include indnaics values from other classification years
naics_indnaics_full <- indnaics_crosswalk_mod %>%
  left_join(indnaics_crosswalk_years, by = c("indnaics" = "2003-2007 ACS/PRCS INDNAICS CODE "), keep = TRUE)

indnaics_sel <- c(
  "2003-2007 ACS/PRCS INDNAICS CODE ", "2008-2012 ACS/PRCS INDNAICS CODE", "2013-2017 ACS/PRCS INDNAICS CODE",
  "2018 ACS/PRCS INDNAICS CODE"
)

naics_indnaics_nafill <- naics_indnaics_full %>% filter((rowSums(is.na(naics_indnaics_full[, indnaics_sel])) == 4))
naics_indnaics_nafill[, indnaics_sel] <- naics_indnaics_nafill$indnaics
# filling in values that are null for all with the indnaics column
naics_indnaics_full[(rowSums(is.na(naics_indnaics_full[, indnaics_sel])) == 4), ][, indnaics_sel] <- naics_indnaics_nafill[, indnaics_sel]


naics_indnaics_full <- naics_indnaics_full %>%
  select(c(
    "2003-2007 ACS/PRCS INDNAICS CODE ", "2008-2012 ACS/PRCS INDNAICS CODE", "2013-2017 ACS/PRCS INDNAICS CODE",
    "2018 ACS/PRCS INDNAICS CODE", "2002 NAICS EQUIVALENT", "2007 NAICS EQUIVALENT",
    "2012 NAICS EQUIVALENT", "2017 NAICS EQUIVALENT", `EXCLUDE1`, `EXCLUDE2`
  ))
naics_indnaics_full[naics_indnaics_full == ""] <- NA


# collapse the four NAICS columns into one column, if values across the four columns change then there will be multiple rows
# with one INDNAICS code corresponding to the multiple NAICS codes
# this makes ultimately joining with ACS data a lot easier/more convenient
# change "" to NA first so "" doesn't get included in the nested list
naics_yrs <- c("2002 NAICS EQUIVALENT", "2007 NAICS EQUIVALENT", "2012 NAICS EQUIVALENT", "2017 NAICS EQUIVALENT")
naics_indnaics_full$naics <- apply(
  naics_indnaics_full %>% select(naics_yrs),
  1, function(x) str_trim(Reduce(union, x[!is.na(x)]))
)
naics_indnaics_full <- unnest(naics_indnaics_full, "naics") %>%
  select(-naics_yrs)
naics_indnaics_full$indnaics <- apply(
  naics_indnaics_full %>% select(indnaics_sel),
  1, function(x) str_trim(Reduce(union, x[!is.na(x)]))
)
naics_indnaics_full <- unnest(naics_indnaics_full, "indnaics") %>%
  select(-indnaics_sel)


# direct matching of naics codes
total_match <- left_join(naics_indnaics_full, lp_naics)
direct_matches <- total_match %>%
  filter(!is.na(total_match$industry_code))

# will now reduce the specificity of naics codes to see if they match
# (ie: 22131 might not match, see if 2213 has a match in the form of the NAICS code N2213__ (represented in the form of 2213)
unjoined <- total_match %>%
  filter(is.na(total_match$industry_code)) %>%
  select(c(`EXCLUDE1`, `EXCLUDE2`, `naics`, `indnaics`))

# reduce to 5 character joins
unjoined$five_naics <- substr(unjoined$naics, 0, 5)
five_join <- left_join(unjoined, lp_naics, by = c("five_naics" = "naics"))
five_join <- five_join %>% filter(!is.na(five_join$industry_code))
five_join$five_naics <- NULL
# remove joined indnaics codes from the unjoined dataframe
unjoined <- unjoined %>% filter(!indnaics %in% five_join$indnaics)
unjoined$five_naics <- NULL

# reduce to 4 character joins
unjoined$four_naics <- substr(unjoined$naics, 0, 4)
four_join <- left_join(unjoined, lp_naics, by = c("four_naics" = "naics"))
four_join <- four_join %>% filter(!is.na(four_join$industry_code))
four_join$four_naics <- NULL
# remove joined indnaics codes from the unjoined dataframe
unjoined <- unjoined %>% filter(!indnaics %in% four_join$indnaics)
unjoined$four_naics <- NULL

# reduce to 3 character joins
unjoined$three_naics <- substr(unjoined$naics, 0, 3)
three_join <- left_join(unjoined, lp_naics, by = c("three_naics" = "naics"))
three_join <- three_join %>% filter(!is.na(three_join$industry_code))
three_join$three_naics <- NULL
# remove joined indnaics codes from the unjoined dataframe
unjoined <- unjoined %>% filter(!indnaics %in% three_join$indnaics)
unjoined$three_naics <- NULL


# reduce to 2 character joins
unjoined$two_naics <- substr(unjoined$naics, 0, 2)
two_join <- left_join(unjoined, lp_naics, by = c("two_naics" = "naics"))
two_join <- two_join %>% filter(!is.na(two_join$industry_code))
two_join$two_naics <- NULL
# remove joined indnaics codes from the unjoined dataframe
unjoined <- unjoined %>% filter(!indnaics %in% two_join$indnaics)
unjoined$two_naics <- NULL

full_match <- rbindlist(list(direct_matches, five_join, four_join, three_join, two_join))

# dealing with the exclusion - removing NAICS columns the corresponding INDNAICS code should not correspond to
full_match_clean_excl1 <- cleanStrings(full_match, 1)
full_match_clean_excl1 <- setdiff(
  full_match_clean_excl1,
  full_match_clean_excl1 %>% filter(naics == EXCLUDE1)
)
full_match_clean_excl1$EXCLUDE1 <- NULL

full_match_clean_excl2 <- cleanStrings(full_match_clean_excl1, 1)
full_match_clean_excl2 <- setdiff(
  full_match_clean_excl2,
  full_match_clean_excl2 %>% filter(naics == EXCLUDE2)
)
full_match_clean_excl2$EXCLUDE2 <- NULL
full_match_clean_excl2$naics <- NULL
full_match_cleaned <- full_match_clean_excl2 %>% distinct()

print("percent of indnaics codes originally given to us in crosswalk preserved after merging to BLS")
mean(unique(naics_indnaics_full$indnaics) %in% unique(full_match_cleaned$indnaics))

fwrite(full_match_cleaned, "Datasets/Cleaned/full_crosswalk.csv", nThread = 10)
