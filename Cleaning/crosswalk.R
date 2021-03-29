#Import libraries
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
#disable scientific notation
options(scipen=999)
registerDoMC(cores = 4)

#Set to my personal directory - adjust accordingly
setwd("~/Google Drive/Non-Academic Work/Research/Traina/Productivity/")

#read in productivity data
lp_naics <- read_delim(file = "Datasets/Cleaned/lp_naics.csv", delim = ",")
#remove prefix N from NAICS classifications
ind_codes <- substr(lp_naics$industry_code, start = 2, stop = 7)
#remove "_" suffixes
ind_codes <- str_replace_all(ind_codes, "_", "")
lp_naics$ind_codes_mod <- ind_codes
#read in crosswalk
indnaics_crosswalk <- read_delim(file = "Datasets/Imported/ACS/2003indnaics.csv",
                                 delim = ",")
indnaics_crosswalk <- indnaics_crosswalk[3:dim(indnaics_crosswalk)[1],]
sector_filter <- is.na(indnaics_crosswalk$` 			INDNAICS CODE 			(2003-onward ACS/PRCS) 		`)
sectors <- indnaics_crosswalk$` 			IND CODE 			(2003-onward ACS/PRCS) 		`[sector_filter]
sector_clean <- unlist(lapply(indnaics_crosswalk$` 			IND CODE 			(2003-onward ACS/PRCS) 		`, 
                              function (x) ifelse(x %in% sectors, x, NA)))
indnaics_crosswalk$`NAICS Sector` <- na.locf(sector_clean)
indnaics_crosswalk <- indnaics_crosswalk[!sector_filter,]
indnaics_crosswalk <- indnaics_crosswalk[,c(6, 5, 1:4) ]
indnaics_crosswalk <- indnaics_crosswalk %>% 
  select(-c(" \t\t\tIND CODE \t\t\t(2003-onward ACS/PRCS) \t\t", "2002 NAICS EQUIVALENT"))
colnames(indnaics_crosswalk) <- trimws(colnames(indnaics_crosswalk))
#removing values where INDNAICS does not map to a valid NAICS column
indnaics_crosswalk <- indnaics_crosswalk %>% filter(`2007 NAICS EQUIVALENT` != NA |
                                                      `2007 NAICS EQUIVALENT` != "------")

print("Parts of")
mean(unlist(lapply(indnaics_crosswalk$`2007 NAICS EQUIVALENT`, function (x) grepl("Part of", x))))
print("Pts")
mean(unlist(lapply(indnaics_crosswalk$`2007 NAICS EQUIVALENT`, function (x) grepl("Pts", x))))
print("-")
mean(unlist(lapply(indnaics_crosswalk$`2007 NAICS EQUIVALENT`, function (x) grepl("-", x))))
print("exc")
mean(unlist(lapply(indnaics_crosswalk$`2007 NAICS EQUIVALENT`, function (x) grepl("exc", x))))
print(",")
mean(unlist(lapply(indnaics_crosswalk$`2007 NAICS EQUIVALENT`, function (x) grepl(",", x))))

#manually correcting some issues
indnaics_crosswalk[247, 4] <- "812113, 812119"
indnaics_crosswalk[94, 4] <- "321991,321992"  
indnaics_crosswalk$`2007 NAICS EQUIVALENT` <- gsub("\\)", "", indnaics_crosswalk$`2007 NAICS EQUIVALENT`)
indnaics_crosswalk$`2007 NAICS EQUIVALENT` <- gsub("\\(", "",  indnaics_crosswalk$`2007 NAICS EQUIVALENT`)
indnaics_crosswalk$`2007 NAICS EQUIVALENT` <- gsub("\\)", "", indnaics_crosswalk$`2007 NAICS EQUIVALENT`)
indnaics_crosswalk$`2007 NAICS EQUIVALENT` <- gsub("pt. ", "", indnaics_crosswalk$`2007 NAICS EQUIVALENT`)
indnaics_crosswalk$`2007 NAICS EQUIVALENT` <- gsub("Part of ", "",indnaics_crosswalk$`2007 NAICS EQUIVALENT`)
indnaics_crosswalk$`2007 NAICS EQUIVALENT` <- gsub("Pts. ", "",  indnaics_crosswalk$`2007 NAICS EQUIVALENT`)
indnaics_crosswalk$`2007 NAICS EQUIVALENT` <- gsub("and", ",",  indnaics_crosswalk$`2007 NAICS EQUIVALENT`)
indnaics_crosswalk$`2007 NAICS EQUIVALENT` <- gsub(" ", "", indnaics_crosswalk$`2007 NAICS EQUIVALENT`)

indnaics_crosswalk$EXCLUDE <- NA
indnaics_crosswalk_mod <- foreach(i = 1:dim(indnaics_crosswalk)[1], .combine = 'bind_rows') %do% {
  selRow <- indnaics_crosswalk[i,]
  val <- selRow[[4]]
  if (grepl("exc", val)) {
    start <- str_locate_all(pattern ='exc.', val)[[1]][1]
    end <- str_locate_all(pattern ='exc.', val)[[1]][2]
    selRow[4] <- substr(val, 1, start-1)
    selRow[5] <- substr(val, end+1, str_length(val)) 
  }
  else if (grepl("-", val) & grepl(",", val)) {
    start <- str_locate_all(pattern ='-', val)[[1]][1]
    end <- str_locate_all(pattern ='-', val)[[1]][2]
    commalst <- str_locate_all(pattern = ",", val)[[1]]
    commaend <- commalst[dim(commalst)[1],][1]
    startnum <- as.numeric(substr(val, commaend+1, start-1))
    endnum <- as.numeric(substr(val, end+1, str_length(val)))
    
    selRow_temp1 <- foreach(i=startnum:endnum, .combine = 'bind_rows') %do% {
      temp <- selRow
      temp[4] <- i
      temp
    }
    vallist <- strsplit(val, ",")[[1]]
    vallist <- vallist[1:length(vallist)-1]
    len <- length(vallist)
    selRow_temp2 <- foreach(i = 1:len, .combine =  'bind_rows') %do% {
      temp <- selRow
      temp[4] <- vallist[i]
      temp
    }
    selRow <- rbind(selRow_temp1, selRow_temp2) %>% arrange(`2007 NAICS EQUIVALENT`)
  }
  else if (grepl("-", val)) {
    start <- str_locate_all(pattern ='-', val)[[1]][1]
    end <- str_locate_all(pattern ='-', val)[[1]][2]
    startnum <- as.numeric(substr(val, 1, start-1))
    endnum <- as.numeric(substr(val, end+1, str_length(val)))
    selRow_temp <- foreach(i=startnum:endnum, .combine = 'bind_rows') %do% {
      temp <- selRow
      temp[4] <- i
      temp
    }
    selRow <- selRow_temp
  }
  else if (grepl(",", val)) {
    vallist <- strsplit(val, ",")[[1]]
    len <- length(vallist)
    selRow_temp <- foreach(i = 1:len, .combine =  'bind_rows') %do% {
      temp <- selRow
      temp[4] <- vallist[i]
      temp
    }
    selRow <- selRow_temp
  }
  else {
  }
  selRow
}

selRow <- indnaics_crosswalk_mod[84,]
val <- selRow[[4]]
vallist <- strsplit(val, ",")[[1]]
len <- length(vallist)
selRow_temp <- foreach(i = 1:len, .combine =  'bind_rows') %do% {
  temp <- selRow
  temp[4] <- vallist[i]
  temp
}
selRow <- selRow_temp
indnaics_crosswalk_mod <- indnaics_crosswalk_mod[-84,]
indnaics_crosswalk_mod <- rbind(indnaics_crosswalk_mod, selRow) %>% 
  arrange("2007 NAICS EQUIVALENT")

direct_crosswalk <- inner_join(indnaics_crosswalk_mod, lp_naics, by = c("2007 NAICS EQUIVALENT" = "ind_codes_mod"))

nonmatch <- indnaics_crosswalk_mod %>% 
  filter(`2007 NAICS EQUIVALENT` %in% setdiff(indnaics_crosswalk_mod$`2007 NAICS EQUIVALENT`, 
                                              direct_crosswalk$`2007 NAICS EQUIVALENT`))

nonmatch$five_code <- substr(nonmatch$`2007 NAICS EQUIVALENT`, 1, 5)
nonmatch$four_code <- substr(nonmatch$`2007 NAICS EQUIVALENT`, 1, 4)
nonmatch$three_code <- substr(nonmatch$`2007 NAICS EQUIVALENT`, 1, 3)
nonmatch$two_code <- substr(nonmatch$`2007 NAICS EQUIVALENT`, 1, 2)

five_join <- as_tibble(inner_join(nonmatch, lp_naics, by = c("five_code" = "ind_codes_mod")))[,-c(6:9)]
four_join <- as_tibble(inner_join(nonmatch, lp_naics, by = c("four_code" = "ind_codes_mod")))[,-c(6:9)]
three_join <- as_tibble(inner_join(nonmatch, lp_naics, by = c("three_code" = "ind_codes_mod")))[,-c(6:9)]
two_join <- as_tibble(inner_join(nonmatch, lp_naics, by = c("two_code" = "ind_codes_mod")))[,-c(6:9)]
indirect_crosswalk <- (rbind(five_join, four_join, three_join, two_join) %>% distinct())
#i would write code to check if there are any NAICS codes that are in the "exclude column"

#but I checked it manually since there are so few values in the exclude column and no require removing
#merge two crosswalks
full_crosswalk <- rbind(direct_crosswalk, indirect_crosswalk) %>%
  arrange(`2007 NAICS EQUIVALENT`)



#read in crosswalk
indnaics_crosswalk_2 <- read_delim(file = "Datasets/Imported/ACS/indnaics_crosswalk_2000_onward_without_code_descriptions.csv",
                                 delim = ",")
#rename column
indnaics_crosswalk_2 <- indnaics_crosswalk_2 %>% 
  rename("NAICS Sector" = "X1")
#filter out unneeded columns
indnaics_crosswalk_2 <- indnaics_crosswalk_2[seq(from = 2, to = dim(indnaics_crosswalk_2)[1]),]
#fill in missing data with data from above (last non NA value)
indnaics_crosswalk_2$`NAICS Sector` <- na.locf(indnaics_crosswalk_2$`NAICS Sector`)
#filter out unneeded census columns
indnaics_crosswalk_2 <- indnaics_crosswalk_2 %>% 
  select(-c(`2000 Census 1% INDNAICS CODE`, `2000 Census 5% INDNAICS CODE`, `2000-2002 ACS INDNAICS CODE`))
#remove values that are NA for all census classifications
remNull <- rowSums(is.na(indnaics_crosswalk_2[,2:5])) == ncol(indnaics_crosswalk_2[,2:5])
indnaics_crosswalk_2 <- indnaics_crosswalk_2[!remNull,]
#basically what this shows is that all the codes, across years, are the same
indnaics_crosswalk_temp <- indnaics_crosswalk_2
indnaics_crosswalk_temp$`2003-2007 ACS/PRCS INDNAICS CODE ` <-
  str_trim(unlist(indnaics_crosswalk_temp$`2003-2007 ACS/PRCS INDNAICS CODE `))
indnaics_crosswalk_temp$`2008-2012 ACS/PRCS INDNAICS CODE` <- 
  str_trim(unlist(indnaics_crosswalk_temp$`2008-2012 ACS/PRCS INDNAICS CODE`))
indnaics_crosswalk_temp$`2013-2017 ACS/PRCS INDNAICS CODE` <- 
  str_trim(unlist(indnaics_crosswalk_temp$`2013-2017 ACS/PRCS INDNAICS CODE`))
indnaics_crosswalk_temp$`2018 ACS/PRCS INDNAICS CODE` <- 
  str_trim(unlist(indnaics_crosswalk_temp$`2018 ACS/PRCS INDNAICS CODE`))
uq_filter <- apply(indnaics_crosswalk_2[,2:5], 1, 
                   function (x) length(unique(x[!is.na(str_trim(unlist(x)))], na.rm = TRUE)) == 1)
indnaics_crosswalk_temp[!uq_filter,] #shows that supposed "non-unique" values are bogus and no uniqueness among values exists
indnaics_crosswalk_temp$code <- apply(indnaics_crosswalk_temp[2:5], 
                                      1, function (x) str_trim(Reduce(union, x[!is.na(x)])))

#create direct crosswlak with perfect matches
direct_crosswalk <- as_tibble(inner_join(indnaics_crosswalk_temp, lp_naics, by = c("code" = "ind_codes_mod")))
direct_crosswalk_new <- direct_crosswalk[,c(1, 6, 7, 8)]
direct_crosswalk_new$EXCLUDE <- NA
direct_crosswalk_new <- as_tibble(cbind(direct_crosswalk_new, direct_crosswalk$industry_code))
direct_crosswalk_new <- as_tibble(cbind(direct_crosswalk_new, direct_crosswalk$industry_text))
ind_codes2 <- substr(direct_crosswalk_new$industry_code, start = 2, stop = 7)
#remove "_" suffixes
ind_codes2 <- gsub("_", "", ind_codes2)
direct_crosswalk_new$industry_code <- ind_codes2
colnames(direct_crosswalk_new) <- colnames(full_crosswalk)      
rem_rep <- !direct_crosswalk_new$industry_code %in% full_crosswalk$industry_code
direct_crosswalk_new <- direct_crosswalk_new[rem_rep,]

#find values with no match
nonmatch <- indnaics_crosswalk_temp %>% filter(code %in% setdiff(indnaics_crosswalk_temp$code, direct_crosswalk$code))
nonmatch <- nonmatch %>% filter(!(grepl("M", nonmatch$code) | grepl("P", nonmatch$code)
                                  | grepl("S", nonmatch$code) | grepl("Z", val)))
#reduce specificity of codes (allow codes to be joined if only the first 5, 4, 3, or 2 values match)
#ie: 321234 has no match, but for 4 digits, 3212 matches with N3212__ from LPS (hypothetical example)
nonmatch$five_code <- substr(nonmatch$code, 1, 5)
nonmatch$four_code <- substr(nonmatch$code, 1, 4)
nonmatch$three_code <- substr(nonmatch$code, 1, 3)
nonmatch$two_code <- substr(nonmatch$code, 1, 2)
#perform joins
five_join <- as_tibble(inner_join(lp_naics, nonmatch, by = c("ind_codes_mod" = "five_code")))[1:10]
four_join <- as_tibble(inner_join(lp_naics, nonmatch, by = c("ind_codes_mod" = "four_code")))[1:10]
three_join <- as_tibble(inner_join(lp_naics, nonmatch, by = c("ind_codes_mod" = "three_code")))[1:10]
two_join <- as_tibble(inner_join(lp_naics, nonmatch, by = c("ind_codes_mod" = "two_code")))[1:10]
indirect_crosswalk <- (rbind(five_join, four_join, three_join, two_join) %>% distinct())[c(4:10, 1)]
indirect_crosswalk <- indirect_crosswalk %>% filter(!industry_code %in% full_crosswalk$industry_code)
indirect_crosswalk_new <- indirect_crosswalk[,c(1, 6, 7, 8)]
indirect_crosswalk_new$EXCLUDE <- NA
indirect_crosswalk_new <- as_tibble(cbind(indirect_crosswalk_new, indirect_crosswalk$industry_code))
lp_naics_2 <- lp_naics
lp_naics_2$ind_codes_mod <- lp_naics_2$industry_code
indirect_crosswalk_new <- inner_join(indirect_crosswalk_new, 
                                     lp_naics_2, c("indirect_crosswalk$industry_code" = "ind_codes_mod"))
indirect_crosswalk_new$industry_code.y <- NULL
ind_codes3 <- substr(indirect_crosswalk_new$industry_code.x, start = 2, stop = 7)
#remove "_" suffixes
ind_codes3 <- gsub("_", "", ind_codes3)
colnames(indirect_crosswalk_new) <- colnames(full_crosswalk)      
indirect_crosswalk_new$`2007 NAICS EQUIVALENT`<- ind_codes3
indirect_crosswalk_new <- indirect_crosswalk_new[,c(1:7)]
full_crosswalk2 <- rbind(direct_crosswalk_new, indirect_crosswalk_new)

full_crosswalk_final <- rbind(full_crosswalk, full_crosswalk2) %>% 
  distinct() %>%
  arrange("industry_code")

count_occsoc_indnaics_year <- read_delim("Datasets/Cleaned/occsoc_indnaics_year.csv", delim = ",")
print("overlap with ACS data, 1/9 NAICS codes included")
mean(unique(count_occsoc_indnaics_year$INDNAICS) %in% 
       full_crosswalk_final$`INDNAICS CODE 			(2003-onward ACS/PRCS)`)

free_indnaics <- unique(count_occsoc_indnaics_year$INDNAICS)[unique(count_occsoc_indnaics_year$INDNAICS) %in% 
                                                               full_crosswalk_final$`INDNAICS CODE 			(2003-onward ACS/PRCS)`]
acs_filtered_crosswalk <- full_crosswalk_final %>% filter(`INDNAICS CODE 			(2003-onward ACS/PRCS)` %in% free_indnaics)
print("number of naics codes")
length(unique(acs_filtered_crosswalk$industry_code))

rem19 <- unlist(lapply(unique(count_occsoc_indnaics_year$INDNAICS), 
                       function (x) substr(x, 1, 1) == "9" || substr(x, 1, 1) == "1"))
acs_indnaics <- unique(count_occsoc_indnaics_year$INDNAICS)[!rem19]
print("crosswalk overlap with ACS data, 1/9 NAICS codes excluded")
cw_acs <- mean(acs_indnaics %in% full_crosswalk_final$`INDNAICS CODE 			(2003-onward ACS/PRCS)`)
print(cw_acs)
print("input crosswalks overlap with ACS data, 1/9 NAICS codes excluded")
indnaics_acs <- mean(acs_indnaics %in% union(indnaics_crosswalk_temp$code, 
                                             indnaics_crosswalk$`INDNAICS CODE 			(2003-onward ACS/PRCS)`))
print(indnaics_acs)

fwrite(full_crosswalk_final, "Datasets/Cleaned/full_crosswalk.csv", nThread = 10)
