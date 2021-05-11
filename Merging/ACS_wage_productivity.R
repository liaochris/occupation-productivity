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
library(plm)
library(tidyverse)
library(sjlabelled)
library(Hmisc)
library(expss)
library(foreign)
library(janitor)

plan(multiprocess)
# disable scientific notation
options(scipen = 999)
registerDoMC(cores = 4)

# Set to my personal directory - adjust accordingly
setwd("~/Google Drive/Non-Academic Work/Research/Traina/occupation-productivity/")

ACS_url <- "https://drive.google.com/file/d/1bI0Bp7FCATaCvGyX-2SNk9PR3VvFcSnd/view?usp=sharing"
fname <- "Datasets/Imported/ACS/Wages/usa_00010.dat"
if (!(file.exists(fname))) {
  ret <- drive_download(ACS_url, fname, overwrite = TRUE)
}

# read in ACS data
ddi <- read_ipums_ddi("Datasets/Imported/ACS/Wages/usa_00010.xml")
data <- data.table(read_ipums_micro(ddi)) %>%
  filter(YEAR > 2000)

#filter for the wanted columns
data_sel <- data %>%
  select(c(
    `YEAR`, `CPI99`, `STATEFIP`, `COUNTYFIP`, `PUMA`, `PERWT`, `AGE`, `EDUC`, `EDUCD`,
    `RACE`, `RACED`, `EMPSTAT`, `EMPSTATD`, `OCCSOC`, `OCC1990`, `OCC2010`, `INDNAICS`,
    `IND`, `IND1990`, `WKSWORK1`, `WKSWORK2`, `UHRSWORK`, `INCWAGE`, `SEX`
  )) %>%
  filter(WKSWORK2 > 3) %>% #only keep workers who worked 40 weeks a year
  filter(UHRSWORK > 20) #remove workers who

#convert wkswork2 - categorical variable into quantity
#replaced category with midpoint of range
data_sel$WKSWORKS2_conv <- 0
data_sel <- data_sel %>% mutate(WKSWORKS2_conv = replace(WKSWORKS2_conv, WKSWORK2 == 4, 43.5))
data_sel <- data_sel %>% mutate(WKSWORKS2_conv = replace(WKSWORKS2_conv, WKSWORK2 == 5, 48.5))
data_sel <- data_sel %>% mutate(WKSWORKS2_conv = replace(WKSWORKS2_conv, WKSWORK2 == 6, 51))
data_sel$WKSWORKS2_conv <- as.numeric(data_sel$WKSWORKS2_conv)
data_sel$wkswork <- as.integer(data_sel$WKSWORK1)
data_sel <- data_sel %>% mutate(wkswork = coalesce(wkswork, WKSWORKS2_conv))

#calculate hourly wage, adjusting for inflation
data_sel$hrwage <- (data_sel$INCWAGE / (data_sel$wkswork * data_sel$UHRSWORK)) * data_sel$CPI99

data_sel_wage <- data_sel

# quick test to make sure wage metric is accurate
#see whether college skill premium is present in dataset
skill_prem <- data_sel_wage

skill_prem <- skill_prem %>% mutate(skill = ifelse(EDUCD < 101, 0, 1))
skill_prem_grouped <- skill_prem %>%
  group_by(YEAR, skill) %>%
  summarise(wage = mean(hrwage))
skill_prem_year <- skill_prem_grouped %>% slice(which(row_number() %% 2 == 0))
skill_prem_year$wage <- NULL
even <- seq(from = 0, to = dim(skill_prem_grouped)[1], by = 2)
odd <- seq(from = 1, to = dim(skill_prem_grouped)[1], by = 2)
skill_prem_year$premium <- (skill_prem_grouped[even, ] / skill_prem_grouped[odd, ])$wage
plot(x = skill_prem_year$YEAR, y = log(skill_prem_year$premium))

# preparing crosswalk
indnaics_crosswalk <- fread("Datasets/Cleaned/full_crosswalk.csv")
data_sel_wage$id <- 1:dim(data_sel_wage)[1]
data_sel_wage <- data_sel_wage %>% select(c(dim(data_sel_wage)[2], 1:(dim(data_sel_wage)[2] - 1)))

#join indnaics with NAICS through crosswalk
data_joined <- data_sel_wage %>% inner_join(indnaics_crosswalk %>% select(c(
  "indnaics",
  "industry_code", "industry_text"
)),
by = c("INDNAICS" = "indnaics")
)
print(paste("this much of the data was preserved post merge", length(unique(data_joined$id)) / length(unique(data_sel_wage$id)), sep = " "))
print(paste("this proportion of the indnaics codes from the ACS data are present in the crosswalk", mean((unique(data_sel_wage$INDNAICS)) %in% unique(indnaics_crosswalk$indnaics)), sep = " "))
print(paste("roughly", mean(duplicated(data_joined$id)), "of the data is duplicated due to one INDNAICS code corresponding to multiple NAICS industries", sep = " "))

#join data with productivity data for industries
lp_current <- fread("Datasets/Cleaned/lp_current.csv")
data_joined$`industry_text` <- str_trim(data_joined$`industry_text`)
data_sel_wage_ind <- data_joined %>% inner_join(lp_current, by = c("industry_code", "YEAR" = "year"))

print(paste("amount of data preserved is", sum(unique(data_sel_wage_ind$id)) / sum(unique(data_joined$id)), sep = " "))
print("this rate is high because the crosswalk from indnaics to naics already filters for only naics codes in the BLS data")
print("Ought to be 100% but likely due to some industries not being present in all years")
# creating column to note duplicate entries because of multiple matching industries
data_sel_wage_ind$productivity <- data_sel_wage_ind$productivity * data_sel_wage_ind$CPI99

# statistics on how variable count varies across years
temp <- data_sel_wage_ind %>%
  group_by(`YEAR`, `industry_code`) %>%
  summarise(cnt = n()) %>%
  arrange(`industry_code`)
temp$pct <- c(NA, 100 * temp$cnt[2:length(temp$cnt)] / temp$cnt[1:(length(temp$cnt) - 1)])
temp[!duplicated(temp$industry_code), ]$pct <- NA
inspect_naics <- (temp %>% filter(YEAR > 2007) %>% group_by(`industry_code`) %>% summarise(m = mean(pct, na.rm = TRUE)) %>% filter(m < 95))$industry_code
temp2 <- temp %>% filter(industry_code %in% inspect_naics)

#rename column
data_sel_wage_ind2 <- data_sel_wage_ind %>%
  rename("industry_text" = "industry_text.x")

# itemizing chararacter variables - for space saving purposes
itemizeChars <- function(col) {
  dict_col <- data.table(
    keys = unique(data_sel_wage_ind2[[col]]),
    items = c(1:length(unique(data_sel_wage_ind2[[col]])))
  )
  colnames(dict_col)[1] <- col
  data_sel_wage_ind2 <- data_sel_wage_ind2 %>%
    inner_join(dict_col)
  data_sel_wage_ind2[[col]] <- data_sel_wage_ind2$items
  data_sel_wage_ind2$items <- NULL
  indnaics_vec <- c(dict_col$items)
  names(indnaics_vec) <- c(dict_col[[col]])
  data_sel_wage_ind2[[col]] <- labelled(x = data_sel_wage_ind2[[col]], labels = indnaics_vec)
  data_sel_wage_ind2
}
#itemize the following varaibles
data_sel_wage_ind2 <- itemizeChars("INDNAICS")
data_sel_wage_ind2 <- itemizeChars("industry_code")
data_sel_wage_ind2 <- itemizeChars("industry_text")
data_sel_wage_ind2 <- itemizeChars("sector_text")

#filter for workers who worked below minimum wage or had excessively high wages
data_sel_wage_ind2 <- data_sel_wage_ind2 %>% filter(hrwage > 5.15 & hrwage < 200)
data_sel_wage_ind2$log_productivity <- log(data_sel_wage_ind2$productivity)

#label the different metrics obtained from the productivity dataset
labelvec <- rep("", length(colnames(data_sel_wage_ind2)))
names(labelvec) <- colnames(data_sel_wage_ind2)
labelvec["W20"] <- "Employment in Thousands"
labelvec["L20"] <- "Hours Worked in Millions"
labelvec["T30"] <- "Production Value in Millions"
attr(data_sel_wage_ind2, "var.labels") <- labelvec

#bucket ages into groups
agelabs <- c(paste(seq(25, 54, by = 5), seq(25 + 5 - 1, 55 - 1, by = 5),
  sep = "-"
))
data_sel_wage_ind2$age_group <- cut(data_sel_wage_ind2$AGE,
  breaks = c(seq(25, 54, by = 5), Inf),
  labels = agelabs, right = FALSE
)

#bucket education into groups
data_sel_wage_ind2$educ_group <- cut(data_sel_wage_ind2$EDUCD,
  breaks = c(0, 62, 81, 101, 114, Inf),
  labels = c("no hs", "hs", "cc", "college", "graduate"), right = FALSE
)

#import Dorn data on the offshoreability of a task
task_data <- read.dta("Datasets/Imported/Dorn/occ1990dd_task_alm.dta")
offshore_data <- read.dta("Datasets/Imported/Dorn/occ1990dd_task_offshore.dta")
data_moreprod <- data_sel_wage_ind2 %>%
  inner_join(task_data, by = c("OCC1990" = "occ1990dd")) %>%
  inner_join(offshore_data, by = c("OCC1990" = "occ1990dd"))
data_moreprod <- data_moreprod %>% distinct()
print(paste(sum(unique(data_moreprod$id)) / sum(unique(data_sel_wage_ind2$id)),
  " of the data was preserved post merge", sep = " "
))

#import data on the computerizability of jobs
data_joinprod <- vroom("Datasets/Cleaned/join_computerized.csv")
data_joinprod$`SOC Code` <- as.character(data_joinprod$`SOC Code`)
data_moreprod$OCCSOC_adj <- gsub("X", "", data_moreprod$OCCSOC)
data_moreprod$OCCSOC_adj <- gsub("Y", "", data_moreprod$OCCSOC_adj)
data_moreprod$mid <- 1:dim(data_moreprod)[1]

#use a diect matching to join data on computerizability with the dataset
data_moreprod_comp <- data_moreprod %>%
  inner_join(data_joinprod, by = c("OCCSOC_adj" = "SOC Code"), keep = TRUE) %>%
  select(-c("Occupation")) %>%
  distinct()

# filtering for unjoined values - see if any wildcard joins can be made
"%ni%" <- Negate("%in%")
data_moreprod_unjoined <- data_moreprod %>%
  filter(mid %ni% data_moreprod_comp$mid) %>%
  select(colnames(data_moreprod))

# joining columns based on decreasing length of string matches (ie: 4510XX matches to 4510)
minstr <- min(str_length(data_moreprod$OCCSOC_adj))
maxstr <- max(str_length(data_moreprod$OCCSOC_adj))
data_moreprod_wc <-
  foreach(i = (maxstr - 1):(minstr), .combine = "bind_rows") %do% {
    data_joinprod$`SOC Code` <- substr(data_joinprod$`SOC Code`, 0, i)
    data_moreprod_temp <- data_moreprod_unjoined %>%
      inner_join(data_joinprod, by = c("OCCSOC_adj" = "SOC Code"), keep = TRUE) %>%
      select(-c("Occupation")) %>%
      distinct()
    data_moreprod_unjoined <- data_moreprod_unjoined %>%
      filter(mid %ni% data_moreprod_temp$mid) %>%
      select(colnames(data_moreprod))
    data_moreprod_temp
  }
# removing mid column that was used to filter out already joined data
data_moreprod_wc$mid <- NULL
data_moreprod_comp$mid <- NULL

# join direct and wildcard matches
data_productivity <- rbindlist(list(data_moreprod_wc, data_moreprod_comp)) %>% distinct()
print(paste("this much of the unique data points was preserved:", sum(unique(data_productivity$id)) / sum(unique(data_moreprod$id))))

# clean column names
data_productivity <- janitor::clean_names(data_productivity)
data_productivity <- data_productivity %>%
  group_by(`id`) %>%
  mutate(dup_cnt = n())
# create fuzzy factor to adjust for duplicate values when calculating perwt
data_productivity$fuzzy <- 1 / data_productivity$dup_cnt

write_dta(janitor::clean_names(data_productivity), "Datasets/Merged/ACS_wage_productivity.dta")
