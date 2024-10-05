# [Last Modified: 2024-08-05]
# 2024-08-05: Using time window considering nothing 

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

cal_window <- function(df, variables, file_name) {
  
  # Create Time Window (ex.1990~1999, 2000~2009, 2010~2019, 2010~2022)
  t1 <- df %>% filter(Year >= 1990 & Year <= 1999)
  t2 <- df %>% filter(Year >= 2000 & Year <= 2009)
  t3 <- df %>% filter(Year >= 2010 & Year <= 2019)
  t4 <- df %>% filter(Year >= 2010 & Year <= 2022)
  
  
  # Calculate average for each period (subsequently log conversion)
  ln_t1 <- t1 %>%
    group_by(Country_i, Country_j) %>%
    summarise(
      across(.cols = all_of(variables), 
             .fns = ~mean(.x, na.rm = TRUE), 
             .names = "{.col}"), .groups = "drop"
    )
  
  ln_t2 <- t2 %>%
    group_by(Country_i, Country_j) %>%
    summarise(
      across(.cols = all_of(variables), 
             .fns = ~mean(.x, na.rm = TRUE), 
             .names = "{.col}"), .groups = "drop"
    )
  
  ln_t3 <- t3 %>%
    group_by(Country_i, Country_j) %>%
    summarise(
      across(.cols = all_of(variables), 
             .fns = ~mean(.x, na.rm = TRUE), 
             .names = "{.col}"), .groups = "drop"
    )
  
  ln_t4 <- t4 %>%
    group_by(Country_i, Country_j) %>%
    summarise(
      across(.cols = all_of(variables), 
             .fns = ~mean(.x, na.rm = TRUE), 
             .names = "{.col}"), .groups = "drop"
    )
  
  # Create feature "t" for each variable
  ln_t1 <- ln_t1 %>% mutate(t = 1)
  ln_t2 <- ln_t2 %>% mutate(t = 2)
  ln_t3 <- ln_t3 %>% mutate(t = 3)
  ln_t4 <- ln_t4 %>% mutate(t = 4)
  
  # Bind rows and send a feature "t" to the front
  tw <- bind_rows(ln_t1, ln_t2)
  tw <- bind_rows(tw, ln_t3)
  tw <- bind_rows(tw, ln_t4)
  tw <- tw %>% select(t, everything())
  
  # Save it as a csv file
  write_csv(tw, file_name)
  
}

# Run
df <- read_csv("consideringNothing/MB/MB.csv")
variables <- "MB"
file_name <- "consideringNothing/Time_window/MB_tw.csv"

cal_window(df, variables, file_name)


