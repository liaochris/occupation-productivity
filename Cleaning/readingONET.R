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
library(RCurl)
setwd("~/Google Drive/Non-Academic Work/Research/Traina/Productivity/")

#function for reading in files
read_files <- function(x, ctypes, start) {
  index <- if (start < 20) {c(start:19, 20:25) } else {c(start:25) }
  foreach(i = index, .combine = 'bind_rows') %do% {
    if (i<20) {
      floc <- paste("Datasets/Imported/ONET Data/db_",i,"_0/", sep = "")
      }
    else {
      floc <- paste("Datasets/Imported/ONET Data/db_",i,"_1_text/", sep = "")
      }
    read_delim(file = paste(floc , x, sep = ""), delim = "\t", col_types = ctypes) 
    } %>% distinct() 
}

#knowledge, skills and abiltiies
start <- 11 #start version number for read_files function
#datatypes for importing
ctypes <- paste(c(rep("c", 4), rep("n", 5), rep("c", 4)), collapse = "")
#using function to read in versions 11-25 of ONET database
knowledge <- read_files("Knowledge.txt", ctypes, start)
#merging results with manual reading of versions 5.1, 7, 9
knowledge <- rbind(knowledge,
                   read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",5,"1/", "Knowledge.txt", sep = "")), delim = "\t"),
                   read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",7,"0/", "Knowledge.txt", sep = "")), delim = "\t"),
                   read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",9,"0/", "Knowledge.txt", sep = "")), delim = "\t")) %>%
  distinct() %>% #removing duplicate values
  arrange(`O*NET-SOC Code`, `Element ID`) #reorganizing values
skills <- read_files("Skills.txt", ctypes, start)
skills <- rbind(skills,
                read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",5,"1/", "Skills.txt", sep = "")), delim = "\t"),
                read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",7,"0/", "Skills.txt", sep = "")), delim = "\t"),
                read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",9,"0/", "Skills.txt", sep = "")), delim = "\t")) %>%
  distinct()  %>% 
  arrange(`O*NET-SOC Code`, `Element ID`)
abilities <- read_files("Abilities.txt", ctypes, start)
abilities <- rbind(abilities,
                   read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",5,"1/", "Ability.txt", sep = "")), delim = "\t"),
                   read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",7,"0/", "Ability.txt", sep = "")), delim = "\t"),
                   read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",9,"0/", "Abilities.txt", sep = "")), delim = "\t")) %>%
  distinct()  %>% 
  arrange(`O*NET-SOC Code`, `Element ID`)

#occupation data, content model and scales reference
start <- 11
ctypes <- paste(c(rep("c", 3)), collapse = "")
occupation_data <- rbind(read_files("Occupation Data.txt", ctypes, start),
                         read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",5,"1/", "onetsoc_data.txt", sep = "")), delim = "\t"),
                         read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",7,"0/", "onetsoc_data.txt", sep = "")), delim = "\t"),
                         read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",9,"0/", "Occupation Data.txt", sep = "")), delim = "\t")) %>%
  arrange(`O*NET-SOC Code`) %>% 
  distinct() %>%
  filter(!duplicated(`O*NET-SOC Code`)) #removing duplicated occupations
ctypes <- paste(c(rep("c", 3), rep("n", 2), rep("c", 1)), collapse = "")
occupation_metadata <- rbind(read_files("Occupation Level Metadata.txt", ctypes, start),
                             read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",7,"0/", "OccLevelMetadata.txt",
                                                           sep = "")), col_types = ctypes, delim = "\t"), 
                             read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",9,"0/", "Occupation Level Metadata.txt",
                                                           sep = "")), col_types = ctypes, delim = "\t"))
occupation_metadata_temp <- read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",5,"1/", "OccLevelMetadata.txt",
                                                          sep = "")), col_types = ctypes, delim = "\t")
colnames(occupation_metadata_temp) <- colnames(occupation_metadata)
occupation_metadata <- rbind(occupation_metadata, occupation_metadata_temp) %>% 
  distinct() %>%
  arrange(`O*NET-SOC Code`)
content_model <- rbind(read_files("Content Model Reference.txt", ctypes, start),
                       read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",5,"1/", "onet_content_model_reference.txt",
                                                     sep = "")), delim = "\t"), 
                       read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",7,"0/", "onet_content_model_reference.txt",
                                                     sep = "")), delim = "\t"), 
                       read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",9,"0/", "Content Model Reference.txt",
                                                     sep = "")), delim = "\t")) %>%
  distinct %>%
  arrange(`Element ID`)
scales_reference <- rbind(read_files("Scales Reference.txt",ctypes, start),
                          read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",9,"0/", "Scales Reference.txt",
                                                        sep = "")), delim = "\t"), 
                          read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",7,"0/", "scales_reference.txt",
                                                        sep = "")), delim = "\t"), 
                          read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",5,"1/", "scales_reference.txt",
                                                        sep = "")), delim = "\t")) %>%
  distinct() %>% 
  arrange(`Scale ID`)

#tasks
start <- 13 #start at version 13, pre-version 13 tasks were imported in a different way
ctypes <- paste(c(rep("c", 1), rep("n", 1), rep("c", 2), rep("n", 1), rep("c", 2)), collapse = "")
task_statements <- read_files("Task Statements.txt", ctypes, start) %>% 
  arrange(`O*NET-SOC Code`, `Task ID`)  %>%
  filter(!duplicated(`Task ID`))
#task ratings and categories
start <- 13
ctypes <- paste(c(rep("c", 1), rep("n", 1), rep("c", 1), rep("n", 6), rep("c", 3)), collapse = "")
task_rat <- read_files("Task Ratings.txt", ctypes, start)  %>% 
  arrange(`O*NET-SOC Code`, `Task ID`, `Date`)
ctypes <- paste(c(rep("c", 1), rep("n", 1), rep("c", 1)), collapse = "")
task_cat <- read_files("Task Categories.txt", ctypes, start)

#splitting up task tings based off of the scale ID  
#filtering to create dataset with just the IM score for tasks
task_rat_IM <- task_rat %>% filter(`Scale ID` == "IM") %>% dplyr::select(-"Category")

#reading in the task ratings
ctypes <- paste(c(rep("c", 1), rep("n", 1), rep("c", 1), rep("n", 6), rep("c", 3)), collapse = "")
#rereading in the task ratings data because it has to be untouched by the distinct() function
#filtering for the FT scale id
task_rat_RT <- task_rat %>% filter(`Scale ID` == "FT")
#creating dictionary to match category numbers to category descriptions
task_rat_dict = dict(items = task_cat$`Category Description`, keys = task_cat$Category)
#dictonary matching function
f <- function(x, y) y$get(x)
#replacing category numbers with category descriptions and removing
task_rat_RT$Category <- unlist(lapply(task_rat_RT$Category, f, task_rat_dict))
task_rat_RT <- task_rat_RT %>%
  distinct()
task_rat_RT$temp <- paste(task_rat_RT$`O*NET-SOC Code`, task_rat_RT$`Task ID`, task_rat_RT$Date)

#incomplete categories that have data for more than 7 occurences of a particular date-task_id-occupation combos
temp_tb <- table((task_rat_RT %>% filter(temp %in% names(which(table(task_rat_RT$temp) != 7))))$temp)
rem_temp <- names(temp_tb)
task_rat_RT <- task_rat_RT %>% filter(!temp %in% rem_temp) %>% 
  dplyr::select(-c(`temp`))

#removing undesired statistical measures contained in columns
task_rat_RT <- task_rat_RT %>% 
  dplyr::select(-c("Standard Error", "Lower CI Bound", "Upper CI Bound", "Recommend Suppress"))
#turning long data into wide data by making categories columns
task_rat_RT_w <- task_rat_RT   %>%
  spread(Category, c('Data Value')) 

#read in tasks.txt for all pre version 13 data
tasks_old <- rbind(read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",5,"1/", "Tasks.txt", sep = "")), delim = "\t"),
                   read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",7,"0/", "Tasks.txt", sep = "")), delim = "\t"),
                   read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",9,"0/", "Tasks.txt", sep = "")), delim = "\t"),
                   read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",11,"_0/", "Tasks.txt", sep = "")), delim = "\t"),
                   read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",12,"_0/", "Tasks.txt", sep = "")), delim = "\t")) %>%
  distinct()

#Removing the columns with the suffix ending in R
pattern <- "[R]$"
remove_pos <- grep(pattern, colnames(tasks_old))
tasks_old_wide <- tasks_old[,-remove_pos] %>%
  dplyr::select(-c("Task", "Task Type", "Incumbents Responding", )) #also removing the additional columns listed
#selecting the columns used in the IM scale dataframe created  earlier to allow for merging
tasks_old_wide_IM <- tasks_old_wide[,c(1:9, (dim(tasks_old_wide)[2]-1):dim(tasks_old_wide)[2])] %>%
  distinct()
#merging this with earlier IM dataset
task_rat_IM <- rbind(task_rat_IM, tasks_old_wide_IM %>% 
                       filter(`Scale ID` == "IM")) %>%
  distinct()

#working on creating a wide FT tasks dataset to merge with the wide FT tasks dataset created earlier
#finding all columns with phrase in pattern1 to select and keep in dataframe
pattern1 <- "Percent Frequency"
match_vals <- grep("Percent Frequency", colnames(tasks_old_wide))
tasks_old_wide_RT <- tasks_old_wide[,c(1:(match_vals[1]-1), match_vals, (dim(tasks_old_wide)[2]-1):dim(tasks_old_wide)[2])] %>%
  dplyr::select(-c(`Scale ID`, `Standard Error`, `Lower CI Bound`, `Upper CI Bound`, `Recommend Suppress`)) %>% #removing columns
  rename(`Scale ID` = `Data Value`) #renaming column
#correting scale id to FT
tasks_old_wide_RT$`Scale ID` <- "FT"
#removing percent frequency and -F suffix from columns
colnames(tasks_old_wide_RT) <- str_replace(str_replace(colnames(tasks_old_wide_RT), "Percent Frequency: ", ""), 
                                           "-F[1-7]", "")
#finding index of Date and Domain Source columns so dataframe can be reordered
date_ind <- grep("Date", colnames(tasks_old_wide_RT))
domain_ind <- grep("Domain Source", colnames(tasks_old_wide_RT))
tasks_old_wide_RT <- tasks_old_wide_RT[,c(1:4, date_ind, domain_ind, 5:(date_ind-1))]
#reordering category columns to match with task_rat_RT_w dataframe
tasks_old_wide_RT <- tasks_old_wide_RT[,c(1:6, 11, 13, 9, 10, 8, 12, 7)]
#renaming category columns to match with task_rat_RT_w
tasks_old_wide_RT <- tasks_old_wide_RT %>% 
  rename("Yearly or less" = "Yearly Or Less",
         "More than yearly" = "More Than Yearly",
         "More than monthly" = "More Than Monthly",
         "More than weekly" = "More Than Weekly",
         "Several times daily" = "Several Times Daily",
         "Hourly or more" = "Hourly Or More")

#merging two wide dataframes
task_rat_RT_wide <- rbind(task_rat_RT_w, tasks_old_wide_RT) %>%
  distinct() 
#converting column to numeric type
task_rat_RT_wide$`Task ID` <- as.numeric(task_rat_RT_wide$`Task ID`)
#removing na values
task_rat_RT_wide <- task_rat_RT_wide %>% 
  filter(!is.na(task_rat_RT_wide$`Task ID`)) %>% distinct()


#work activities
start <- 11
ctypes <- paste(c(rep("c", 4), rep("n", 5), rep("c", 4)), collapse = "")
work_act <- rbind(read_files("Work Activities.txt", ctypes, start),
                  read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",5,"1/", "WorkActivity.txt", sep = "")), delim = "\t"),
                  read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",7,"0/", "WorkActivity.txt", sep = "")), delim = "\t"),
                  read_delim(file = paste(paste("Datasets/Imported/ONET Data/db_",9,"0/", "Work Activities.txt", sep = "")), delim = "\t")) %>%
  distinct() %>%
  arrange(`O*NET-SOC Code`, `Element ID`, `Date`)
#IWA DWA and task to DWA references only available for the data versions 19 and onwards
start <- 19
ctypes <- paste(c(rep("c", 3)), collapse = "")
IWA_ref <- read_files("IWA Reference.txt", ctypes, start) %>%
  arrange(`Element ID`, `IWA ID`) 
ctypes <- paste(c(rep("c", 4)), collapse = "")
DWA_ref <- read_files("DWA Reference.txt", ctypes, start) %>%
  arrange(`Element ID`, `IWA ID`, `DWA ID`) 
ctypes <- paste(c(rep("c", 1), rep("n", 1), rep("c", 3)), collapse = "")
task_to_DWA <- read_files("Tasks to DWAs.txt", ctypes, start) %>%
  arrange(`O*NET-SOC Code`, `Task ID`, `DWA ID`, `Date`) 

#mappings
#mapping naics sector percentages to occupations and converting to wide format
occupation_naics <- occupation_metadata %>% filter(Item == 'NAICS Sector')
occupation_naics_wide <- spread(occupation_naics, Response, Percent)

#cross-domain linkages - reading in files that map abilities/skills to work activities
ctypes <- paste(c(rep("c", 4)), collapse = "")
floc <- "Datasets/Imported/ONET Data/db_25_1_text/"
abilities_to_work <- read_delim(paste(floc, "Abilities to Work Activities.txt", sep = ""), col_types = ctypes, delim = "\t")
skills_to_work <- read_delim(paste(floc, "Skills to Work Activities.txt", sep = ""), col_types = ctypes, delim = "\t")

#merge skills/abilities to work and then work to tasks (through work IWA DWA task linkage)
#end goal: skills/abilities-work-task-occupation linkage?

#matching occupations and their corresponding abilities to work activities
#removing undesired columns
ability_scores <- abilities %>% dplyr::select(-c(`Element Name`, `Recommend Suppress`, `Domain Source`, `Not Relevant`))
#joining work activities and abilities dataset to match work activities to abilities
#abilities dataset also includes occupations 
occ_abilities_work <- inner_join(abilities_to_work, 
                                 abilities %>% 
                                   dplyr::select(-c(`Standard Error`, `Lower CI Bound`,`Upper CI Bound`)) %>%
                                   rename(`Occupation Ability Score` = `Data Value`,
                                          `Ability Scale ID` = `Scale ID`,
                                          `Ability N` = `N`,
                                          `Ability Date` = `Date`,
                                          `Ability Recommend Suppress` = `Recommend Suppress`,
                                          `Ability Domain Source` = `Domain Source`,
                                          `Ability Not Relevant` = `Not Relevant`),
                                 by = c("Abilities Element ID" = "Element ID", 
                                        "Abilities Element Name" = "Element Name"))

#matching work activities to a corresponding DWA (detailed work activity)
work_DWA <- inner_join(work_act %>%
                         dplyr::select(-c(`Standard Error`, `Lower CI Bound`,`Upper CI Bound`))  %>% 
                         rename(`Work Activities Element ID` = `Element ID`,
                                `Work Activities Element Name` = `Element Name`,
                                `Occupation Work Activities Score` = `Data Value`,
                                `Work Activities Scale ID` = `Scale ID`,
                                `Work Activities N` = `N`,
                                `Work Activities Date` = `Date`,
                                `Work Activities Recommend Suppress` = `Recommend Suppress`,
                                `Work Activities Domain Source` = `Domain Source`,
                                `Work Activities Not Relevant` = `Not Relevant`), 
                       DWA_ref, 
                       by = c("Work Activities Element ID" = "Element ID"))
work_act_scores <- work_act %>% dplyr::select(-c(`Element Name`, `Recommend Suppress`, `Domain Source`, `Not Relevant`))

#dictionary of task IDs and task statements
task_dict <- dict(items = task_statements$Task, keys = task_statements$`Task ID`)
#matching a task to a work activity
task_work <- inner_join(work_DWA, task_to_DWA %>% dplyr::select(-c("Domain Source", "Date"))) %>%
  dplyr::select(-c("IWA ID", "DWA ID", "DWA Title")) %>% 
  distinct()
#matching task ID to task name
task_work$Task <- unlist(lapply(task_work$`Task ID`, f, task_dict))
#merging task and work activities matching with the work activities and abilities dataset
task_work_abilities <- inner_join(task_work, 
                                  occ_abilities_work, 
                                  by = c("O*NET-SOC Code", "Work Activities Element ID", "Work Activities Element Name",
                                         "Work Activities Date" = "Ability Date")) %>%
  rename("Date" = "Work Activities Date") %>%
  distinct()
#adding task rating data to the dataset
task_rat_work_abilities <- inner_join(task_work_abilities,
                                      task_rat_RT_wide %>%
                                        rename("Task Ratings Scale ID" = "Scale ID",
                                               "Task Ratings Domain Source" = "Domain Source",
                                               "Task Ratings N" = "N"))

task_rat_work_skills$Date <- str_replace(task_work_abilities$Date, "3/2003", "03/2003")
task_rat_work_skills$Date <- str_replace(task_work_abilities$Date, "003/2003", "03/2003")
task_rat_work_skills$Date <- str_replace(task_work_abilities$Date, "7/2004", "07/2004")
task_rat_work_skills$Date <- str_replace(task_work_abilities$Date, "007/2004", "07/2004")

#creating final matching dataset, removing unimportant columns
matching_dataset_abilities <- task_rat_work_abilities %>% 
  dplyr::select(-c("Work Activities N", "Work Activities Recommend Suppress",  "Work Activities Not Relevant", 
                   "Work Activities Domain Source", "Ability N", "Ability Not Relevant", "Ability Domain Source", "Task Ratings N", 
                   "Task Ratings Domain Source"))

system.time(fwrite(matching_dataset_abilities, "Datasets/Cleaned/matching_dataset_abilities.csv", nThread = 10))

#matching occupations and their corresponding skills to work activities
skill_scores <- skills %>% dplyr::select(-c(`Element Name`, `Recommend Suppress`, `Domain Source`, `Not Relevant`))
occ_skills_work <- inner_join(skills_to_work, 
                              skills %>% 
                                dplyr::select(-c(`Standard Error`, `Lower CI Bound`,`Upper CI Bound`)) %>%
                                rename(`Occupation Ability Score` = `Data Value`,
                                       `Skills Scale ID` = `Scale ID`,
                                       `Skills N` = `N`,
                                       `Skills Date` = `Date`,
                                       `Skills Domain Source` = `Domain Source`,
                                       `Skills Not Relevant` = `Not Relevant`),
                              by = c("Skills Element ID" = "Element ID", 
                                     "Skills Element Name" = "Element Name"))

#merging task and work activities matching with the work activities and abilities skills
task_work_skills <- inner_join(task_work, 
                               occ_skills_work, 
                               by = c("O*NET-SOC Code", "Work Activities Element ID", "Work Activities Element Name",
                                      "Work Activities Date" = "Skills Date")) %>%
  rename("Date" = "Work Activities Date") %>%
  distinct()

#adding task rating data to the dataset
task_rat_work_skills <- inner_join(task_work_skills,
                                   task_rat_RT_wide %>%
                                     rename("Task Ratings Scale ID" = "Scale ID",
                                            "Task Ratings Domain Source" = "Domain Source",
                                            "Task Ratings N" = "N"))
task_rat_work_skills$Date <- str_replace(task_rat_work_skills$Date, "3/2003", "03/2003")
task_rat_work_skills$Date <- str_replace(task_rat_work_skills$Date, "003/2003", "03/2003")
task_rat_work_skills$Date <- str_replace(task_rat_work_skills$Date, "7/2004", "07/2004")
task_rat_work_skills$Date <- str_replace(task_rat_work_skills$Date, "007/2004", "07/2004")

#creating final matching dataset, removing unimportant columns
matching_dataset_skills <- task_rat_work_skills %>% 
  dplyr::select(-c("Work Activities N", "Work Activities Recommend Suppress",  "Work Activities Not Relevant", 
                   "Work Activities Domain Source", "Skills N", "Skills Not Relevant", "Skills Domain Source", 
                   "Task Ratings N", "Task Ratings Domain Source"))

system.time(fwrite(matching_dataset_skills, "Datasets/Cleaned/matching_dataset_skills.csv", nThread = 10))

