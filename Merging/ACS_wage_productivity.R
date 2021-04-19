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
library(plm)

plan(multiprocess)
#disable scientific notation
options(scipen=999)
registerDoMC(cores = 4)

#Set to my personal directory - adjust accordingly
setwd("~/Google Drive/Non-Academic Work/Research/Traina/occupation-productivity/")

ACS_url <- "https://drive.google.com/file/d/1M5cAqNDsrZXGvIvwgghoHBPRwvvMvila/view?usp=sharing"
fname <- "Datasets/Imported/ACS/Wages/usa_00009.dat"
if (!(file.exists(fname))) {
  ret <- drive_download(ACS_url, fname, overwrite = TRUE)
}

#read in ACS data
ddi <- read_ipums_ddi("Datasets/Imported/ACS/Wages/usa_00009.xml")
data <- data.table(read_ipums_micro(ddi)) %>% 
  filter(YEAR > 2000)

data_sel <- data %>% 
  select(c(`YEAR`, `CPI99`, `STATEFIP`, `COUNTYFIP`, `PUMA`, `PERWT`, `AGE`, `EDUCD`,
                              `EMPSTATD`, `OCCSOC`, `INDNAICS`, `WKSWORK1`, `WKSWORK2`, `UHRSWORK`, `INCWAGE`)) %>%
  filter(WKSWORK2 > 3) %>%
  filter(UHRSWORK > 20)

data_sel$WKSWORKS2_conv <- 0
data_sel <- data_sel %>% mutate(WKSWORKS2_conv = replace(WKSWORKS2_conv, WKSWORK2 == 4, 43.5))
data_sel <- data_sel %>% mutate(WKSWORKS2_conv = replace(WKSWORKS2_conv, WKSWORK2 == 5, 48.5))
data_sel <- data_sel %>% mutate(WKSWORKS2_conv = replace(WKSWORKS2_conv, WKSWORK2 == 6, 51))
data_sel$WKSWORKS2_conv <- as.numeric(data_sel$WKSWORKS2_conv)
data_sel$wkswork <- as.integer(data_sel$WKSWORK1)
data_sel <- data_sel %>% mutate(wkswork = coalesce(wkswork,WKSWORKS2_conv))
data_sel$hrwage <- (data_sel$INCWAGE/(data_sel$wkswork * data_sel$UHRSWORK)) * data_sel$CPI99
data_sel_wage <- data_sel %>% filter(hrwage < 200 && hrwage > 3.8)

#quick test to make sure wage metric is accurate
skill_prem <- data_sel_wage
skill_prem <- skill_prem %>% mutate(skill = ifelse(EDUCD<101, 0, 1))
skill_prem_grouped <- skill_prem %>% group_by(YEAR, skill) %>% summarise(wage = mean(hrwage))
skill_prem_year <- skill_prem_grouped %>% slice(which(row_number() %% 2 == 0))
skill_prem_year$wage <- NULL
even <- seq(from = 0, to = dim(skill_prem_grouped)[1], by = 2)
odd <- seq(from = 1, to = dim(skill_prem_grouped)[1], by = 2)
skill_prem_year$premium <- (skill_prem_grouped[even,]/skill_prem_grouped[odd,])$wage
plot(x = skill_prem_year$YEAR, y = log(skill_prem_year$premium))

#preparing crosswalk
indnaics_crosswalk <- fread("Datasets/Cleaned/full_crosswalk.csv")
coln <- colnames(indnaics_crosswalk)
data_sel_wage$id <- seq(1:dim(data_sel_wage)[1])
data_sel_wage <- data_sel_wage %>% select(c(dim(data_sel_wage)[2], 1:(dim(data_sel_wage)[2]-1)))

data_joined <- data_sel_wage %>% inner_join(indnaics_crosswalk %>% select(c("INDNAICS CODE \t\t\t(2003-onward ACS/PRCS)",
                                                                    "industry_code")),
                                    by = c("INDNAICS" = "INDNAICS CODE \t\t\t(2003-onward ACS/PRCS)"))
print(paste("roughly", mean(duplicated(data_joined$id)), "of the data is duplicated due to one INDNAICS code corresponding to multiple NAICS industries", sep = " "))

lp_current <- fread("Datasets/Cleaned/lp_current.csv")

data_sel_wage_ind <- data_joined %>% inner_join(lp_current %>% select(c("industry_code", "industry_text",
                                                                          "productivity", "year", "sector_code",
                                                                          "sector_text")),
                                                  by = c("industry_code", "YEAR" = "year"))
print(paste("amount of data preserved is", dim(data_sel_wage_ind)[1]/dim(data_joined)[1], sep = " "))
#creating column to note duplicate entries because of multiple matching industries
data_sel_wage_ind <- data_sel_wage_ind %>% group_by(`id`) %>% mutate(dup_cnt = n())
data_sel_wage_ind$productivity <- data_sel_wage_ind$productivity * data_sel_wage_ind$CPI99

data_sel_wage_ind_export <- data_sel_wage_ind %>% select(c("id", "YEAR", "PERWT", "AGE", "EDUCD", "EMPSTATD", "OCCSOC", "hrwage", 
                                                           "industry_code", "productivity", "dup_cnt"))
system.time(fwrite(data_sel_wage_ind_export, "Datasets/Merged/ACS_wage_productivity.csv", nThread = 8))
#summary(lm(hrwage ~ productivity + factor(YEAR) + factor(industry_code), weights= PERWT, data_sel_wage_ind))