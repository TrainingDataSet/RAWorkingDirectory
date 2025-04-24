# [Last Modified: 2025-02-18]
# 2025-02-08: Find variables for the entire country
# 2025-02-18: Considering Gross Output data in the right column

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

# Merge Export_to_sales and Import_to_sales
# export_to_sales <- read_csv("Export_to_sales/Export_to_sales.csv")
# import_to_sales <- read_csv("Import_to_sales/Import_to_sales.csv")

# df <- full_join(export_to_sales, import_to_sales, by = c("Year", "Country_i", "Industry_s"))

df <- read_csv("Export_Import_Sales_VA2/Export_Import_Sales_VA_EntireCountry (2).csv")

# df$Export_to_sales <- df$Export / df$Sales
# df$Import_to_sales <- df$Import / df$Sales
df$Export_to_sales <- df$Export / df$Export_Sales
df$Import_to_sales <- df$Import / df$Import_Sales

# df <- df %>% select(-c(VA_cur, VA_con))


#------------------ Method 1. ------------------#

# Calculate median and top25%, top75%
df2 <- df %>%
  group_by(Year, Country_i) %>%
  mutate(
    Export_tradable_top25 = as.numeric(Export_to_sales >= quantile(Export_to_sales, 0.75, na.rm = TRUE)),
    Export_tradable_median = as.numeric(Export_to_sales >= median(Export_to_sales, na.rm = TRUE)),
    Export_tradable_top75 = as.numeric(Export_to_sales >= quantile(Export_to_sales, 0.25, na.rm = TRUE)),
         
    Import_tradable_top25 = as.numeric(Import_to_sales >= quantile(Import_to_sales, 0.75, na.rm = TRUE)),
    Import_tradable_median = as.numeric(Import_to_sales >= median(Import_to_sales, na.rm = TRUE)),
    Import_tradable_top75 = as.numeric(Import_to_sales >= quantile(Import_to_sales, 0.25, na.rm = TRUE))
    )



#------------------ Method 2. ------------------#

# Determining Export Tradable Sector
df12 <- df %>%
  group_by(Year, Country_i) %>%
  arrange(desc(Export), .by_group = TRUE) %>%
  mutate(
    Export_tradable_cumsum75 = as.numeric(cumsum(Export) <= 0.75 * sum(Export)),
    Export_tradable_cumsum90 = as.numeric(cumsum(Export) <= 0.90 * sum(Export))
  ) %>%
  ungroup()

# Determining Import Tradable Sector
df13 <- df %>%
  group_by(Year, Country_i) %>%
  arrange(desc(Import), .by_group = TRUE) %>%
  mutate(
    Import_tradable_cumsum75 = as.numeric(cumsum(Import) <= 0.75 * sum(Import)),
    Import_tradable_cumsum90 = as.numeric(cumsum(Import) <= 0.90 * sum(Import))
  ) %>%
  ungroup()

# Merge
df14 <- full_join(df12, df13, by = c("Year", "Country_i", "Industry_s", 
                                     "Export", "Export_to_sales",
                                     "Import", "Import_to_sales", 
                                     "Export_Sales", "Import_Sales",
                                     "VA_cur", "VA_con"))

df_fin <- full_join(df2, df14, by = c("Year", "Country_i", "Industry_s", 
                                       "Export", "Export_to_sales",
                                       "Import", "Import_to_sales", 
                                       "Export_Sales", "Import_Sales",
                                       "VA_cur", "VA_con"))

write_csv(df_fin, "Tradable_dummy/Tradable_dummy_EntireCountry (2).csv")


#------------------ Mean of Dummy Variable ------------------#

dummy <- read_csv("Tradable_dummy/Tradable_dummy_EntireCountry (2).csv")

# Group by Year and Country_i
dummy2 <- dummy %>%
  group_by(Year, Country_i) %>%
  summarize(Mean_Export_tradable_top25 = mean(Export_tradable_top25), Mean_Export_tradable_median = mean(Export_tradable_median),
            Mean_Export_tradable_top75 = mean(Export_tradable_top75), Mean_Import_tradable_top25 = mean(Import_tradable_top25),   
            Mean_Import_tradable_median = mean(Import_tradable_median), Mean_Import_tradable_top75 = mean(Import_tradable_top75),
            Mean_Export_tradable_cumsum75 = mean(Export_tradable_cumsum75), Mean_Export_tradable_cumsum90 = mean(Export_tradable_cumsum90), 
            Mean_Import_tradable_cumsum75 = mean(Import_tradable_cumsum75), Mean_Import_tradable_cumsum90 = mean(Import_tradable_cumsum90),
            .groups = "drop")

# Group by Country_i
dummy3 <- dummy %>%
  group_by(Country_i) %>%
  summarize(Mean_Export_tradable_top25 = mean(Export_tradable_top25), Mean_Export_tradable_median = mean(Export_tradable_median),
            Mean_Export_tradable_top75 = mean(Export_tradable_top75), Mean_Import_tradable_top25 = mean(Import_tradable_top25),   
            Mean_Import_tradable_median = mean(Import_tradable_median), Mean_Import_tradable_top75 = mean(Import_tradable_top75),
            Mean_Export_tradable_cumsum75 = mean(Export_tradable_cumsum75), Mean_Export_tradable_cumsum90 = mean(Export_tradable_cumsum90), 
            Mean_Import_tradable_cumsum75 = mean(Import_tradable_cumsum75), Mean_Import_tradable_cumsum90 = mean(Import_tradable_cumsum90),
            .groups = "drop")

write_csv(dummy2, "Tradable_dummy/Mean/Mean_Year_Country_tradable_dummy_EntireCountry (2).csv")
write_csv(dummy3, "Tradable_dummy/Mean/Mean_Country_tradable_dummy_EntireCountry (2).csv")



#--------------- Correlation of Dummy Variable ---------------#

dummy <- read_csv("Tradable_dummy/Tradable_dummy_EntireCountry (2).csv")

# Group by Year and Country_i
dummy4 <- dummy %>%
  group_by(Year, Country_i) %>%
  summarize(Corr_top25 = cor(Export_tradable_top25, Import_tradable_top25),
            Corr_median = cor(Export_tradable_median, Import_tradable_median),
            Corr_top75 = cor(Export_tradable_top75, Import_tradable_top75),
            Corr_cumsum75 = cor(Export_tradable_cumsum75, Import_tradable_cumsum75),
            Corr_cumsum90 = cor(Export_tradable_cumsum90, Import_tradable_cumsum90),
            .groups = "drop")

# Group by Country_i
dummy5 <- dummy %>%
  group_by(Country_i) %>%
  summarize(Corr_top25 = cor(Export_tradable_top25, Import_tradable_top25),
            Corr_median = cor(Export_tradable_median, Import_tradable_median),
            Corr_top75 = cor(Export_tradable_top75, Import_tradable_top75),
            Corr_cumsum75 = cor(Export_tradable_cumsum75, Import_tradable_cumsum75),
            Corr_cumsum90 = cor(Export_tradable_cumsum90, Import_tradable_cumsum90),
            .groups = "drop")

write_csv(dummy4, "Tradable_dummy/Corr/Corr_Year_Country_tradable_dummy_EntireCountry (2).csv")
write_csv(dummy5, "Tradable_dummy/Corr/Corr_Country_tradable_dummy_EntireCountry (2).csv")

