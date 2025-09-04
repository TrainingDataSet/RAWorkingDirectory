# 패키지 로드
library(readxl)
library(dplyr)
library(tidyr)

# 엑셀 파일 불러오기
df <- read_excel("ADB/constant_GDP.xlsx", sheet = 1)

# wide 형식 → long 형식으로 변환
df_long <- df %>%
  pivot_longer(
    cols = -Year,             # Year 열을 제외한 모든 열을 변환
    names_to = "Country",     # 열 이름을 저장할 새 열 이름
    values_to = "GDP"         # 값이 저장될 열 이름
  )


