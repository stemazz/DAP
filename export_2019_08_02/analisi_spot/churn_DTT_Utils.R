
source("connection_R.R")

## OPTIONS
options(scipen = 10000)

## LIBRERIE
library(magrittr)
library(stringr)



join_COD_CLIENTE_CIFRATO <- function(df1, df2){
  df2 <- withColumnRenamed(df2, "COD_CLIENTE_CIFRATO", "COD_CLIENTE_CIFRATO_y")
  df1 <- merge(df1, df2, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_y")
  df1 <- drop(df1, "COD_CLIENTE_CIFRATO_y")
  return(df1)
}



remove_too_short <- function(df0, duration){
  ## ORDINO PER SKYID E TIME DELLA VISITA
  df0 <- orderBy(df0, "COD_CLIENTE_CIFRATO", "date_time_ts")
  
  ## AGGIUNGO TIMESTAMP DELLA VISITA ALLA PAGINA SUCCESSIVA
  mywindow = orderBy(windowPartitionBy(df0$COD_CLIENTE_CIFRATO), desc(df0$date_time_ts))
  df0 <- withColumn(df0, "nextvisit", over(lag(df0$date_time_ts), mywindow))
  df0 <- orderBy(df0, asc(df0$COD_CLIENTE_CIFRATO), asc(df0$date_time_ts))
  
  ## CALCOLO LA DURATA DI VIEW DI UNA PAGINA
  createOrReplaceTempView(df0, "df0")
  df0 <- sql("SELECT *, CAST(nextvisit AS LONG) - CAST(date_time_ts AS LONG) as sec_on_page from df0")
  df0 <- filter(df0, isNull(df0$sec_on_page) | df0$sec_on_page >= duration)
  df0$sec_on_page <- ifelse(df0$sec_on_page >= 3600, NA, df0$sec_on_page)
  df0 <- withColumn(df0,  "min_on_page", round(df0$sec_on_page / 60))
}




#### KPI SITO ----

add_kpi_sito <- function(df) {
  # Creo la concatenazione cookie-visit_num
  df <- withColumn(df, "visit_uniq", concat_ws('-', df$post_visid_concatenated, df$visit_num))
  
  # Aggiugno colonne necessarie ai nuovi kpi
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,
            -- Visite per categoria
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-calcio') THEN visit_uniq ELSE Null END AS visit_Calcio,
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-calcio-estero') THEN visit_uniq ELSE Null END AS visit_CalcioEstero,
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-formula1') THEN visit_uniq ELSE Null END AS visit_Formula1,
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-calciomercato') THEN visit_uniq ELSE Null END AS visit_CalcioMercato,
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-motogp') THEN visit_uniq ELSE Null END AS visit_MotoGp,
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-nba') THEN visit_uniq ELSE Null END AS visit_Nba,

            CASE WHEN (terzo_livello_post_prop LIKE 'sport-serie-a') THEN visit_uniq ELSE Null END AS visit_Calcio_serieA,
            CASE WHEN (terzo_livello_post_prop LIKE 'sport-champions-league') THEN visit_uniq ELSE Null END AS visit_Calcio_championsLeague,
            
            -- Secondi per categoria
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-calcio') THEN sec_on_page ELSE Null END AS sec_Calcio,
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-calcio-estero') THEN sec_on_page ELSE Null END AS sec_CalcioEstero,
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-formula1') THEN sec_on_page ELSE Null END AS sec_Formula1,
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-calciomercato') THEN sec_on_page ELSE Null END AS sec_CalcioMercato,
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-motogp') THEN sec_on_page ELSE Null END AS sec_MotoGp,
            CASE WHEN (secondo_livello_post_prop LIKE 'sport-nba') THEN sec_on_page ELSE Null END AS sec_Nba,

            CASE WHEN (terzo_livello_post_prop LIKE 'sport-serie-a') THEN sec_on_page ELSE Null END AS sec_Calcio_serieA,
            CASE WHEN (terzo_livello_post_prop LIKE 'sport-champions-league') THEN sec_on_page ELSE Null END AS sec_Calcio_championsLeague

            FROM df"
  )
  return(df)
}

group_kpi_sito <- function(df) {
  
  df <- add_kpi_sito(df)
  
  df_kpi <- agg(
    groupBy(df, "COD_CLIENTE_CIFRATO"),
    # visite
    visite_totali = countDistinct(df$visit_uniq), 
    visite_Calcio = countDistinct(df$visit_Calcio),
    visite_CalcioEstero = countDistinct(df$visit_CalcioEstero),
    visite_Formula1= countDistinct(df$visit_Formula1),
    visite_CalcioMercato = countDistinct(df$visit_CalcioMercato),
    visite_MotoGp = countDistinct(df$visit_MotoGp),
    visite_Nba = countDistinct(df$visit_Nba),
    
    visite_Calcio_serieA = countDistinct(df$visit_Calcio_serieA),
    visite_Calcio_CL = countDistinct(df$visit_Calcio_championsLeague),

    # secondi
    secondi_totali = sum(df$sec_on_page, na.rm = T), 
    secondi_Calcio = sum(df$sec_Calcio, na.rm = T),
    secondi_CalcioEstero = sum(df$sec_CalcioEstero, na.rm = T),
    secondi_Formula1= sum(df$sec_Formula1, na.rm = T),
    secondi_CalcioMercato = sum(df$sec_CalcioMercato, na.rm = T),
    secondi_MotoGp = sum(df$sec_MotoGp, na.rm = T),
    secondi_Nba = sum(df$sec_Nba, na.rm = T),
    
    secondi_Calcio_serieA = sum(df$sec_Calcio_serieA),
    secondi_Calcio_CL = sum(df$sec_Calcio_championsLeague)
  )
  return(df_kpi)
}


#### KPI APP ----

add_kpi_app <- function(df) {
  # Aggiugno colonne necessarie ai nuovi kpi
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,
            -- Visite per categoria
            CASE WHEN site_subsection_post_evar LIKE 'video' THEN visit_num ELSE Null END AS visit_Video,
            CASE WHEN site_subsection_post_evar LIKE 'sport%' THEN visit_num ELSE Null END AS visit_Sports,
            CASE WHEN site_subsection_post_evar LIKE 'skysport24' THEN visit_num ELSE Null END AS visit_SkySport24,

            CASE WHEN site_subsection_post_evar LIKE 'calcio' THEN visit_num ELSE Null END AS visit_Sports_Calcio,

            -- Secondi per categoria
            CASE WHEN site_subsection_post_evar LIKE 'video' THEN sec_on_page ELSE Null END AS sec_Video,
            CASE WHEN site_subsection_post_evar LIKE 'sport%' THEN sec_on_page ELSE Null END AS sec_Sports,
            CASE WHEN site_subsection_post_evar LIKE 'skysport24' THEN sec_on_page ELSE Null END AS sec_SkySport24,

            CASE WHEN site_subsection_post_evar LIKE 'calcio' THEN sec_on_page ELSE Null END AS sec_Sports_Calcio

            FROM df"
  )
  return(df)
}

group_kpi_app <- function(df) {
  
  df <- add_kpi_app(df)
  
  df_kpi <- agg(
    groupBy(df, "COD_CLIENTE_CIFRATO"),
    # visite
    visite_totali = countDistinct(df$visit_num),
    visite_Video = countDistinct(df$visit_Video),
    visite_Sports = countDistinct(df$visit_Sports),
    visite_SkySport24 = countDistinct(df$visit_SkySport24),
    visite_Sports_Calcio = countDistinct(df$visit_Sports_Calcio),
    
    secondi_totali = sum(df$sec_on_page, na.rm = T),
    secondi_Video = sum(df$sec_Video, na.rm = T),
    secondi_Sports = sum(df$sec_Sports, na.rm = T),
    secondi_SkySport24 = sum(df$sec_SkySport24, na.rm = T),
    secondi_Sports_Calcio = sum(df$sec_Sports_Calcio, na.rm = T) 
  )
  return(df_kpi)
}

