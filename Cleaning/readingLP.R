library(readr)
library(vroom)
library(collections)
library(dplyr)
library(glue)
library(stringr)
library(data.table)
library(sjmisc)
library(tidyr)

# change directory as appropriate
setwd("~/Google Drive/Non-Academic Work/Research/Traina/occupation-productivity/")

# reading in the various labor productivity data files - see ip.txt for further information
area <- read_delim("Datasets/Imported/Industry Data/ip.area.txt",
  col_types = list(col_character(), col_character(), col_number(), col_character(), col_number()),
  delim = "\t"
) %>%
  select(c(`area_code`, `area_text`))

duration <- read_delim("Datasets/Imported/Industry Data/ip.duration.txt",
  col_types = list(col_number(), col_character()),
  delim = "\t"
) %>%
  select(c(`duration_code`, `duration_text`))
footnote <- read_delim("Datasets/Imported/Industry Data/ip.footnote.txt",
  col_types = list(col_character(), col_character()),
  delim = "\t"
) %>%
  select(c(`footnote_code`, `footnote_text`))
footnote[nrow(footnote) + 1, ] <- list("", "")
industry <- read_delim("Datasets/Imported/Industry Data/ip.industry.txt",
  col_types = list(col_character(), col_character(), col_character(), col_number(), col_character(), col_number()),
  delim = "\t"
) %>%
  select(c(`industry_code`, `industry_text`))
measure <- read_delim("Datasets/Imported/Industry Data/ip.measure.txt",
  col_types = list(col_character(), col_character(), col_number(), col_character(), col_number()),
  delim = "\t"
) %>%
  select(c(`measure_code`, `measure_text`))
seasonal <- read_delim("Datasets/Imported/Industry Data/ip.seasonal.txt",
  col_types = list(col_character(), col_character()),
  delim = "\t"
) %>%
  select(c(`seasonal_code`, `seasonal_text`))
sector <- read_delim("Datasets/Imported/Industry Data/ip.sector.txt",
  col_types = list(col_character(), col_character()),
  delim = "\t"
) %>%
  select(c(`sector_code`, `sector_text`))
type <- read_delim("Datasets/Imported/Industry Data/ip.type.txt",
  col_types = list(col_character(), col_character()),
  delim = "\t"
) %>%
  select(c(`type_code`, `type_text`))

# reading in Labor productivity time series data
# setting column types
ctypes_series <- list(
  col_character(), col_character(), col_character(), col_character(),
  col_character(), col_number(), col_character(),
  col_character(), col_character(), col_character(), col_character(),
  col_number(), col_character(), col_number(), col_character()
)
series <- read_delim("Datasets/Imported/Industry Data/ip.series.txt",
  col_types = ctypes_series,
  delim = "\t"
)

# reading in labor productivity data for industries from 2003 onwards
data_current <- read_delim("Datasets/Imported/Industry Data/ip.data.0.Current.txt",
  col_types = list(
    col_character(), col_number(), col_character(), col_number(),
    col_character()
  ),
  delim = "\t"
)

# adding applying dictionary get function to dataframe to get values for codes for columns
series_expanded <- series %>%
  rename(
    "seasonal_code" = "seasonal",
    "footnote_code" = "footnote_codes"
  )

# function for applying dictionary containing descriptions mapped to codes
matching_func <- function(col, df) {
  col_code <- paste(col, "code", sep = "_")
  # matching descriptions to values
  series_expanded <- series_expanded %>% inner_join(df)
  target_col <- which(colnames(series_expanded) == col_code)
  # reordering dataframe columns
  series_expanded <- series_expanded[, c(
    1:target_col,
    length(colnames(series_expanded)),
    (target_col + 2):length(colnames(series_expanded)) - 1
  )]
  series_expanded
}
series_expanded <- matching_func("seasonal", seasonal)
series_expanded <- matching_func("type", type)
series_expanded <- matching_func("sector", sector)
series_expanded <- matching_func("measure", measure)
series_expanded <- matching_func("industry", industry)
series_expanded <- matching_func("area", area)
series_expanded <- matching_func("duration", duration)
series_expanded$footnote_code[is.na(series_expanded$footnote_code)] <- ""
series_expanded <- matching_func("footnote", footnote)


# merge data_current from the BLS time series data and series_expanded with information on each series
# matchines descriptors to each numerical value
colnames(series_expanded) <- lapply(colnames(series_expanded), str_trim)
colnames(data_current) <- lapply(colnames(data_current), str_trim)
data_current$footnote_codes[is.na(data_current$footnote_codes)] <- ""
data_current_expanded <- data_current %>% left_join(series_expanded, by = c("series_id", "footnote_codes" = "footnote_code"))

# filtering for just the columns that have numbers that represent real values, not scaled
lp_current_prod <- data_current_expanded %>%
  filter(!grepl("(2007=100)", data_current_expanded$measure_text)) %>%
  filter(duration_code == 0) %>%
  filter(industry_code != "N______")

# getting labor productivity columns (hours worked in millions, value of production in millions)
lp_current <- lp_current_prod %>% filter(measure_code %in% c("L20", "T30"))
# select for desired columns, changing to wide data
lp_current_wide <- pivot_wider(
  data = lp_current %>%
    select(c(
      `year`, `sector_code`, `sector_text`, `industry_code`,
      `industry_text`, `measure_code`, `value`
    )) %>%
    distinct(),
  names_from = measure_code, values_from = value
)
# filtering for rows that do not have na values for the specified columns
lp_current_rows <- which(rowSums(is.na(lp_current_wide %>% select(c("L20", "T30")))) == 0)
lp_current_wide_filled <- lp_current_wide[lp_current_rows, ]
# calculate productivity - factor of how much a worker produces per hour, dollar wise
lp_current_wide_filled$productivity <- lp_current_wide_filled$T30 / lp_current_wide_filled$L20

system.time(fwrite(lp_current_wide_filled, "Datasets/Cleaned/lp_current.csv", nThread = 10))