## LAUNCH SPARK 
source("connection_R.R")

## OPTIONS
options(scipen = 10000)

## LIBRERIE
library(magrittr)
library(stringr)


## PULISCO CACHE
clearCache()

## PULISCO WS
rm(list = ls())

## SEED
seed = 27

## FUNZIONE PRINT HEAD
H <- function(df) {
  View(head(df, 1000))
}

## FUNZIONE RENAME LIST
rename_with_list <- function(df, suffix){
  nomi_vecchi <- colnames(df)
  nomi_nuovi <- unlist(lapply(nomi_vecchi, paste0, suffix))
  for(i in 1:length(nomi_vecchi)){
    df <- withColumnRenamed(df, nomi_vecchi[i], nomi_nuovi[i])
  }
  
  return(df)
}


## CUSTOM JOIN PER EVITARE COLONNE DUPLICATE
join_sky_id <- function(df1, df2){
  df2 <- withColumnRenamed(df2, "SKY_ID", "SKY_ID_y")
  df1 <- merge(df1, df2, all.x = T, by.x = "SKY_ID", by.y = "SKY_ID_y")
  df1 <- drop(df1, "SKY_ID_y")
  
  return(df1)
}

join_post_visid_concatenated <- function(df1, df2){
  df2 <- withColumnRenamed(df2, "post_visid_concatenated", "post_visid_concatenated_y")
  df1 <- merge(df1, df2, all.x = T, by.x = "post_visid_concatenated", by.y = "post_visid_concatenated_y")
  df1 <- drop(df1, "post_visid_concatenated_y")
  return(df1)
}

join_COD_CLIENTE_CIFRATO <- function(df1, df2){
  df2 <- withColumnRenamed(df2, "COD_CLIENTE_CIFRATO", "COD_CLIENTE_CIFRATO_y")
  df1 <- merge(df1, df2, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_y")
  df1 <- drop(df1, "COD_CLIENTE_CIFRATO_y")
  return(df1)
}

## CUSTOM FILTER ON LIST
filter_on_count <- function(df1, df2, n){
  df2 <- withColumnRenamed(df2, "COD_CLIENTE_CIFRATO", "COD_CLIENTE_CIFRATO_y")
  df1 <- merge(df1, df2, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_y")
  df1 <- drop(df1, "COD_CLIENTE_CIFRATO_y")
  df1 <- filter(df1, df1$`count(1)` <= n)
  
  return(df1)
}

#### KPI SITO ----

start_kpi_sito <- function(df, clienti){
  
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *, datediff(TARGET_dt, date_time_dt) as diff_gg_view from df")
  
  # Tengo solo i > 7gg
  df <- filter(df, "diff_gg_view > 7")
  
  # Calcolo i vari df per i kpi
  df_1w <- filter(df, "diff_gg_view <= 14")
  df_2w <- filter(df, "diff_gg_view <= 21")
  df_1m <- filter(df, "diff_gg_view <= 35")
  df_2m <- filter(df, "diff_gg_view <= 63")
  
  # KPI
  df_1w_kpi <- group_kpi_sito(df_1w)
  df_1w_kpi <- rename_with_list(df_1w_kpi, "_sito_1w")
  df_kpi <- merge(clienti, df_1w_kpi, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_sito_1w")
  
  df_2w_kpi <- group_kpi_sito(df_2w)
  df_2w_kpi <- rename_with_list(df_2w_kpi, "_sito_2w")
  df_kpi <- merge(df_kpi, df_2w_kpi, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_sito_2w")
  
  df_1m_kpi <- group_kpi_sito(df_1m)
  df_1m_kpi <- rename_with_list(df_1m_kpi, "_sito_1m")
  df_kpi <- merge(df_kpi, df_1m_kpi, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_sito_1m")
  
  df_2m_kpi <- group_kpi_sito(df_2m)
  df_2m_kpi <- rename_with_list(df_2m_kpi, "_sito_2m")
  df_kpi <- merge(df_kpi, df_2m_kpi, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_sito_2m")
  
  df_kpi <- drop(df_kpi, c("COD_CLIENTE_CIFRATO_sito_1w", "COD_CLIENTE_CIFRATO_sito_2w", "COD_CLIENTE_CIFRATO_sito_1m", "COD_CLIENTE_CIFRATO_sito_2m"))
  
  ## LAST CLEANS
  df_kpi <- fillna(df_kpi, 0)
  
  return(df_kpi)
  
}
add_kpi_sito <- function(df) {
  
  
  # Aggiugno colonne necessarie ai nuovi kpi
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,
            -- Visite per categoria
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate fatture pagamenti') THEN visit_num ELSE Null END AS visit_Fatture,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate extra') THEN visit_num ELSE Null END AS visit_FaiDaTeExtra,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate arricchisci abbonamento') THEN visit_num ELSE Null END AS visit_ArricchischiAbbonamento,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate gestisci dati servizi') THEN visit_num ELSE Null END AS visit_Gestione,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate webtracking') THEN visit_num ELSE Null END AS visit_Comunicazione,
            CASE WHEN (terzo_livello_post_prop LIKE 'contatta') THEN visit_num ELSE Null END AS visit_Contatti,
            CASE WHEN (terzo_livello_post_prop LIKE 'conosci' OR 
            terzo_livello_post_prop LIKE 'home' OR
            terzo_livello_post_prop LIKE 'gestisci' OR 
            terzo_livello_post_prop LIKE 'ricerca' OR 
            terzo_livello_post_prop LIKE 'risolvi') THEN visit_num ELSE Null END AS visit_Assistenza,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky') THEN visit_num ELSE Null END AS visit_Trasloco,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasparenza tariffaria') THEN visit_num ELSE Null END AS visit_TrasparenzaTariffaria,
            CASE WHEN (secondo_livello_post_prop LIKE '%guidatv%' OR secondo_livello_post_prop LIKE '%Guidatv%') THEN visit_num ELSE Null END AS visit_GuidaTv,
            CASE WHEN secondo_livello_post_prop LIKE 'pagine di servizio' THEN visit_num ELSE Null END AS visit_PaginaDiServizio,
            CASE WHEN secondo_livello_post_prop LIKE 'extra' THEN visit_num ELSE Null END AS visit_Extra,
            CASE WHEN secondo_livello_post_prop LIKE 'tecnologia' THEN visit_num ELSE Null END AS visit_Tecnologia,
            CASE WHEN secondo_livello_post_prop LIKE 'pacchetti-offerte' THEN visit_num ELSE Null END AS visit_PacchettiOfferte,
            CASE WHEN terzo_livello_post_prop LIKE 'info disdetta' THEN visit_num ELSE Null END AS visit_InfoDisdetta,
            CASE WHEN terzo_livello_post_prop LIKE 'trova sky service' THEN visit_num ELSE Null END AS visit_TrovaSkyService,
            CASE WHEN (page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%') THEN visit_num ELSE Null END AS visit_SearchDisdetta,
            CASE WHEN terzo_livello_post_prop LIKE 'sky expert' THEN visit_num ELSE Null END AS visit_SkyExpert,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky' OR 
            terzo_livello_post_prop LIKE 'trasparenza tariffaria' OR
            terzo_livello_post_prop LIKE 'trova sky service' OR
            page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%' OR
            terzo_livello_post_prop LIKE 'sky expert') THEN visit_num ELSE Null END AS visit_Trigger,
            
            -- Secondi per categoria
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate fatture pagamenti') THEN sec_on_page ELSE Null END AS sec_Fatture,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate extra') THEN sec_on_page ELSE Null END AS sec_FaiDaTeExtra,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate arricchisci abbonamento') THEN sec_on_page ELSE Null END AS sec_ArricchischiAbbonamento,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate gestisci dati servizi') THEN sec_on_page ELSE Null END AS sec_Gestione,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate webtracking') THEN sec_on_page ELSE Null END AS sec_Comunicazione,
            CASE WHEN (terzo_livello_post_prop LIKE 'contatta') THEN sec_on_page ELSE Null END AS sec_Contatti,
            CASE WHEN (terzo_livello_post_prop LIKE 'conosci' OR 
            terzo_livello_post_prop LIKE 'home' OR
            terzo_livello_post_prop LIKE 'gestisci' OR 
            terzo_livello_post_prop LIKE 'ricerca' OR 
            terzo_livello_post_prop LIKE 'risolvi') THEN sec_on_page ELSE Null END AS sec_Assistenza,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky') THEN sec_on_page ELSE Null END AS sec_Trasloco,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasparenza tariffaria') THEN sec_on_page ELSE Null END AS sec_TrasparenzaTariffaria,
            CASE WHEN (secondo_livello_post_prop LIKE '%guidatv%' OR secondo_livello_post_prop LIKE '%Guidatv%') THEN sec_on_page ELSE Null END AS sec_GuidaTv,
            CASE WHEN secondo_livello_post_prop LIKE 'pagine di servizio' THEN sec_on_page ELSE Null END AS sec_PaginaDiServizio,
            CASE WHEN secondo_livello_post_prop LIKE 'extra' THEN sec_on_page ELSE Null END AS sec_Extra,
            CASE WHEN secondo_livello_post_prop LIKE 'tecnologia' THEN sec_on_page ELSE Null END AS sec_Tecnologia,
            CASE WHEN secondo_livello_post_prop LIKE 'pacchetti-offerte' THEN sec_on_page ELSE Null END AS sec_PacchettiOfferte,
            CASE WHEN terzo_livello_post_prop LIKE 'info disdetta' THEN sec_on_page ELSE Null END AS sec_InfoDisdetta,
            CASE WHEN terzo_livello_post_prop LIKE 'trova sky service' THEN sec_on_page ELSE Null END AS sec_TrovaSkyService,
            CASE WHEN (page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%') THEN sec_on_page ELSE Null END AS sec_SearchDisdetta,
            CASE WHEN terzo_livello_post_prop LIKE 'sky expert' THEN sec_on_page ELSE Null END AS sec_SkyExpert,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky' OR 
            terzo_livello_post_prop LIKE 'trasparenza tariffaria' OR
            terzo_livello_post_prop LIKE 'trova sky service' OR
            page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%' OR
            terzo_livello_post_prop LIKE 'sky expert') THEN sec_on_page ELSE Null END AS sec_Trigger
            FROM df"
  )
  
  return(df)
}
group_kpi_sito <- function(df) {
  
  df <- add_kpi_sito(df)
  
  df_kpi <- agg(
    groupBy(df, "COD_CLIENTE_CIFRATO"),
    # visite
    visite_totali = max(df$visit_num, na.rm = T) - min(df$visit_num, na.rm = T) + 1,
    visite_Fatture = max(df$visit_Fatture, na.rm = T) - min(df$visit_Fatture, na.rm = T) + 1,
    visite_FaiDaTeExtra = max(df$visit_FaiDaTeExtra, na.rm = T) - min(df$visit_FaiDaTeExtra, na.rm = T) + 1,
    visite_ArricchisciAbbonamento = max(df$visit_ArricchischiAbbonamento, na.rm = T) - min(df$visit_ArricchischiAbbonamento, na.rm = T) + 1,
    visite_Gestione = max(df$visit_Gestione, na.rm = T) - min(df$visit_Gestione, na.rm = T) + 1,
    visite_Contatti = max(df$visit_Contatti, na.rm = T) - min(df$visit_Contatti, na.rm = T) + 1,
    visite_Comunicazione = max(df$visit_Comunicazione, na.rm = T) - min(df$visit_Comunicazione, na.rm = T) + 1,
    visite_Assistenza = max(df$visit_Assistenza, na.rm = T) - min(df$visit_Assistenza, na.rm = T) + 1,
    visite_Trasloco = max(df$visit_Trasloco, na.rm = T) - min(df$visit_Trasloco, na.rm = T) + 1,
    visite_TrasparenzaTariffaria = max(df$visit_TrasparenzaTariffaria, na.rm = T) - min(df$visit_TrasparenzaTariffaria, na.rm = T) + 1,
    visite_GuidaTv = max(df$visit_GuidaTv, na.rm = T) - min(df$visit_GuidaTv, na.rm = T) + 1,
    visite_PaginaDiServizio = max(df$visit_PaginaDiServizio, na.rm = T) - min(df$visit_PaginaDiServizio, na.rm = T) + 1,
    visite_Extra = max(df$visit_Extra, na.rm = T) - min(df$visit_Extra, na.rm = T) + 1,
    visite_Tecnologia = max(df$visit_Tecnologia, na.rm = T) - min(df$visit_Tecnologia, na.rm = T) + 1,
    visite_PacchettiOfferte = max(df$visit_PacchettiOfferte, na.rm = T) - min(df$visit_PacchettiOfferte, na.rm = T) + 1,
    visite_InfoDisdetta = max(df$visit_InfoDisdetta, na.rm = T) - min(df$visit_InfoDisdetta, na.rm = T) + 1,
    visite_TrovaSkyService = max(df$visit_TrovaSkyService, na.rm = T) - min(df$visit_TrovaSkyService, na.rm = T) + 1,
    visite_SearchDisdetta = max(df$visit_SearchDisdetta, na.rm = T) - min(df$visit_SearchDisdetta, na.rm = T) + 1,
    visite_SkyExpert = max(df$visit_SkyExpert, na.rm = T) - min(df$visit_SkyExpert, na.rm = T) + 1,
    visite_Trigger = max(df$visit_Trigger, na.rm = T) - min(df$visit_Trigger, na.rm = T) + 1,
    
    # secondi
    secondi_totali = sum(df$sec_on_page, na.rm = T),
    secondi_Fatture = sum(df$sec_Fatture, na.rm = T),
    secondi_FaiDaTeExtra = sum(df$sec_FaiDaTeExtra, na.rm = T),
    secondi_ArricchisciAbbonamento = sum(df$sec_ArricchischiAbbonamento, na.rm = T),
    secondi_Gestione = sum(df$sec_Gestione, na.rm = T),
    secondi_Contatti = sum(df$sec_Contatti, na.rm = T),
    secondi_Comunicazione = sum(df$sec_Comunicazione, na.rm = T),
    secondi_Assistenza = sum(df$sec_Assistenza, na.rm = T),
    secondi_Trasloco = sum(df$sec_Trasloco, na.rm = T),
    secondi_TrasparenzaTariffaria = sum(df$sec_TrasparenzaTariffaria, na.rm = T),
    secondi_GuidaTv = sum(df$sec_GuidaTv, na.rm = T),
    secondi_PaginaDiServizio = sum(df$sec_PaginaDiServizio, na.rm = T),
    secondi_Extra = sum(df$sec_Extra, na.rm = T),
    secondi_Tecnologia = sum(df$sec_Tecnologia, na.rm = T),
    secondi_PacchettiOfferte = sum(df$sec_PacchettiOfferte, na.rm = T),
    secondi_InfoDisdetta = sum(df$sec_InfoDisdetta, na.rm = T),
    secondi_TrovaSkyService = sum(df$sec_TrovaSkyService, na.rm = T),
    secondi_SearchDisdetta = sum(df$sec_SearchDisdetta, na.rm = T),
    secondi_SkyExpert = sum(df$sec_SkyExpert, na.rm = T),
    secondi_Trigger = sum(df$sec_Trigger, na.rm = T)
  )
  
  return(df_kpi)
}

#### KPI APP ----
start_kpi_app <- function(df, clienti){
  
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *, datediff(TARGET_dt, date_time_dt) as diff_gg_view from df")
  
  # Tengo solo i > 7gg
  df <- filter(df, "diff_gg_view > 7")
  
  # Calcolo i vari df per i kpi
  df_1w <- filter(df, "diff_gg_view <= 14")
  df_2w <- filter(df, "diff_gg_view <= 21")
  df_1m <- filter(df, "diff_gg_view <= 35")
  df_2m <- filter(df, "diff_gg_view <= 63")
  
  # KPI
  df_1w_kpi <- group_kpi_app(df_1w)
  df_1w_kpi <- rename_with_list(df_1w_kpi, "_app_1w")
  df_kpi <- merge(clienti, df_1w_kpi, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_app_1w")
  
  df_2w_kpi <- group_kpi_app(df_2w)
  df_2w_kpi <- rename_with_list(df_2w_kpi, "_app_2w")
  df_kpi <- merge(df_kpi, df_2w_kpi, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_app_2w")
  
  df_1m_kpi <- group_kpi_app(df_1m)
  df_1m_kpi <- rename_with_list(df_1m_kpi, "_app_1m")
  df_kpi <- merge(df_kpi, df_1m_kpi, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_app_1m")
  
  df_2m_kpi <- group_kpi_app(df_2m)
  df_2m_kpi <- rename_with_list(df_2m_kpi, "_app_2m")
  df_kpi <- merge(df_kpi, df_2m_kpi, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_app_2m")
  
  df_kpi <- drop(df_kpi, c("COD_CLIENTE_CIFRATO_app_1w", "COD_CLIENTE_CIFRATO_app_2w", "COD_CLIENTE_CIFRATO_app_1m", "COD_CLIENTE_CIFRATO_app_2m"))
  
  ## LAST CLEANS
  df_kpi <- fillna(df_kpi, 0)
  
  return(df_kpi)
  
}

add_kpi_app <- function(df) {
  
  
  # Aggiugno colonne necessarie ai nuovi kpi
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,
            -- Visite per categoria
            CASE WHEN (site_section LIKE 'fatture' OR site_section LIKE 'dati e fatture' OR site_section LIKE 'widget') THEN visit_num ELSE Null END AS visit_fatture,
            CASE WHEN site_section LIKE 'extra' THEN visit_num ELSE Null END AS visit_extra,
            CASE WHEN site_section LIKE 'arricchisci abbonamento' THEN visit_num ELSE Null END AS visit_arricchisci_abbonamento,
            CASE WHEN (site_section LIKE 'stato abbonamento' OR site_section LIKE 'gestione servizi' OR site_section LIKE 'i miei dati') THEN visit_num ELSE Null END AS visit_stato_abbonamento,
            CASE WHEN site_section LIKE 'contatta sky' THEN visit_num ELSE Null END AS visit_contatta_sky,
            CASE WHEN site_section LIKE 'assistenza e supporto' THEN visit_num ELSE Null END AS visit_assistenza_e_supporto,
            CASE WHEN site_section LIKE 'comunicazioni' THEN visit_num ELSE Null END AS visit_comunicazioni,
            CASE WHEN site_section LIKE 'gestisci servizi' THEN visit_num ELSE Null END AS visit_gestisci_servizi,
            CASE WHEN site_section LIKE 'il mio abbonamento' THEN visit_num ELSE Null END AS visit_mio_abbonamento,
            CASE WHEN site_section LIKE 'info' THEN visit_num ELSE Null END AS visit_info,
            -- Secondi per categoria
            CASE WHEN (site_section LIKE 'fatture' OR site_section LIKE 'dati e fatture' OR site_section LIKE 'widget') THEN sec_on_page ELSE Null END AS sec_fatture,
            CASE WHEN site_section LIKE 'extra' THEN sec_on_page ELSE Null END AS sec_extra,
            CASE WHEN site_section LIKE 'arricchisci abbonamento' THEN sec_on_page ELSE Null END AS sec_arricchisci_abbonamento,
            CASE WHEN (site_section LIKE 'stato abbonamento' OR site_section LIKE 'gestione servizi' OR site_section LIKE 'i miei dati') THEN sec_on_page ELSE Null END AS sec_stato_abbonamento,
            CASE WHEN site_section LIKE 'contatta sky' THEN sec_on_page ELSE Null END AS sec_contatta_sky,
            CASE WHEN site_section LIKE 'assistenza e supporto' THEN sec_on_page ELSE Null END AS sec_assistenza_e_supporto,
            CASE WHEN site_section LIKE 'comunicazioni' THEN sec_on_page ELSE Null END AS sec_comunicazioni,
            CASE WHEN site_section LIKE 'gestisci servizi' THEN sec_on_page ELSE Null END AS sec_gestisci_servizi,
            CASE WHEN site_section LIKE 'il mio abbonamento' THEN sec_on_page ELSE Null END AS sec_mio_abbonamento,
            CASE WHEN site_section LIKE 'info' THEN sec_on_page ELSE Null END AS sec_info
            FROM df"
  )
  
  return(df)
}
group_kpi_app <- function(df) {
  
  df <- add_kpi_app(df)
  
  df_kpi <- agg(
    groupBy(df, "COD_CLIENTE_CIFRATO"),
    # visite
    visite_totali = max(df$visit_num, na.rm = T) - min(df$visit_num, na.rm = T) + 1,
    visite_fatture = max(df$visit_fatture, na.rm = T) - min(df$visit_fatture, na.rm = T) + 1,
    visite_extra = max(df$visit_extra, na.rm = T) - min(df$visit_extra, na.rm = T) + 1,
    visite_arricchisci_abbonamento = max(df$visit_arricchisci_abbonamento, na.rm = T) - min(df$visit_arricchisci_abbonamento, na.rm = T) + 1,
    visite_stato_abbonamento = max(df$visit_stato_abbonamento, na.rm = T) - min(df$visit_stato_abbonamento, na.rm = T) + 1,
    visite_contatta_sky = max(df$visit_contatta_sky, na.rm = T) - min(df$visit_contatta_sky, na.rm = T) + 1,
    visite_assistenza_e_supporto = max(df$visit_assistenza_e_supporto, na.rm = T) - min(df$visit_assistenza_e_supporto, na.rm = T) + 1,
    visite_comunicazioni = max(df$visit_comunicazioni, na.rm = T) - min(df$visit_comunicazioni, na.rm = T) + 1,
    visite_gestisci_servizi = max(df$visit_gestisci_servizi, na.rm = T) - min(df$visit_gestisci_servizi, na.rm = T) + 1,
    visite_mio_abbonamento = max(df$visit_mio_abbonamento, na.rm = T) - min(df$visit_mio_abbonamento, na.rm = T) + 1,
    visite_info = max(df$visit_info, na.rm = T) - min(df$visit_info, na.rm = T) + 1,
    
    secondi_totali = sum(df$sec_on_page, na.rm = T),
    secondi_fatture = sum(df$sec_fatture, na.rm = T),
    secondi_extra = sum(df$sec_extra, na.rm = T) ,
    secondi_arricchisci_abbonamento = sum(df$sec_arricchisci_abbonamento, na.rm = T),
    secondi_stato_abbonamento = sum(df$sec_stato_abbonamento, na.rm = T),
    secondi_contatta_sky = sum(df$sec_contatta_sky, na.rm = T),
    secondi_assistenza_e_supporto = sum(df$sec_assistenza_e_supporto, na.rm = T),
    secondi_comunicazioni = sum(df$sec_comunicazioni, na.rm = T),
    secondi_gestisci_servizi = sum(df$sec_gestisci_servizi, na.rm = T),
    secondi_mio_abbonamento = sum(df$sec_mio_abbonamento, na.rm = T),
    secondi_info = sum(df$sec_info, na.rm = T)   
  )
  
  return(df_kpi)
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


melt_kpi <- function(df0){
  
  span_time <- c("_1w", "_2w", "_1m", "_2m")
  
  for (i in 1:length(span_time)){
    
    df0 <- withColumn(df0, paste0("visite_totali", span_time[[i]]), lit( df0[[ paste0("visite_totali_sito", span_time[[i]]) ]] + df0[[ paste0("visite_totali_app", span_time[[i]]) ]] ))
    df0 <- withColumn(df0, paste0("visite_fatture", span_time[[i]]), lit( df0[[ paste0("visite_fatture_sito", span_time[[i]]) ]] + df0[[ paste0("visite_fatture_app", span_time[[i]]) ]]  ))
    df0 <- withColumn(df0, paste0("visite_extra", span_time[[i]]), lit(df0[[ paste0("visite_extra_sito", span_time[[i]]) ]] + df0[[ paste0("visite_extra_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("visite_arricchisciAbbonamento", span_time[[i]]), lit(df0[[ paste0("visite_ArricchisciAbbonamento_sito", span_time[[i]]) ]] + df0[[ paste0("visite_arricchisci_abbonamento_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("visite_gestione", span_time[[i]]), lit(df0[[ paste0("visite_Gestione_sito", span_time[[i]]) ]] + df0[[ paste0("visite_gestisci_servizi_app", span_time[[i]]) ]] ))
    df0 <- withColumn(df0, paste0("visite_contatti", span_time[[i]]), lit(df0[[ paste0("visite_Contatti_sito", span_time[[i]]) ]] + df0[[ paste0("visite_contatta_sky_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("visite_comunicazione", span_time[[i]]), lit(df0[[ paste0("visite_Comunicazione_sito", span_time[[i]]) ]] + df0[[ paste0("visite_comunicazioni_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("visite_assistenza", span_time[[i]]), lit(df0[[ paste0("visite_Assistenza_sito", span_time[[i]]) ]] + df0[[ paste0("visite_assistenza_e_supporto_app", span_time[[i]]) ]]))
    
  }
  
  for (i in 1:length(span_time)){
    
    df0 <- withColumn(df0, paste0("secondi_totali", span_time[[i]]), lit( df0[[ paste0("secondi_totali_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_totali_app", span_time[[i]]) ]] ))
    df0 <- withColumn(df0, paste0("secondi_fatture", span_time[[i]]), lit( df0[[ paste0("secondi_fatture_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_fatture_app", span_time[[i]]) ]]  ))
    df0 <- withColumn(df0, paste0("secondi_extra", span_time[[i]]), lit(df0[[ paste0("secondi_extra_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_extra_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("secondi_arricchisciAbbonamento", span_time[[i]]), lit(df0[[ paste0("secondi_ArricchisciAbbonamento_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_arricchisci_abbonamento_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("secondi_gestione", span_time[[i]]), lit(df0[[ paste0("secondi_Gestione_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_gestisci_servizi_app", span_time[[i]]) ]] ))
    df0 <- withColumn(df0, paste0("secondi_contatti", span_time[[i]]), lit(df0[[ paste0("secondi_Contatti_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_contatta_sky_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("secondi_comunicazione", span_time[[i]]), lit(df0[[ paste0("secondi_Comunicazione_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_comunicazioni_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("secondi_assistenza", span_time[[i]]), lit(df0[[ paste0("secondi_Assistenza_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_assistenza_e_supporto_app", span_time[[i]]) ]]))
    
  }
  
  return(df0)
}


differential_kpi <- function(df0, vars){
  
  for (i in 1:length(vars)){
    
    w1 <- paste0(vars[i], "_sito_1w")
    w2 <- paste0(vars[i], "_sito_2w")
    m1 <- paste0(vars[i], "_sito_1m")
    m2 <- paste0(vars[i], "_sito_2m")
    
    df0 <- withColumn(df0, paste(vars[i], "_incr_perc_2w", sep = ""), lit((df0[[w1]]/df0[[w2]] - 1) * 100))
    df0 <- withColumn(df0, paste(vars[i], "_incr_perc_1m", sep = ""), lit((df0[[w1]]/df0[[m1]] - 1) * 100))
    df0 <- withColumn(df0, paste(vars[i], "_incr_perc_2m", sep = ""), lit((df0[[w1]]/df0[[m2]] - 1) * 100))
    
  }
  
  df0 <- fillna(df0, -1)
  
  return(df0)
  
}


# cookie_sito_pdisc <- cookie_sito_pdisc_17
# cookie_sito_pdisc <- orderBy(cookie_sito_pdisc, "post_visid_concatenated", "date_time_ts")
# window2 <- orderBy(windowPartitionBy(cookie_sito_pdisc$post_visid_concatenated), desc(cookie_sito_pdisc$date_time_dt))
# cookie_sito_pdisc <- withColumn(cookie_sito_pdisc, "SKY_ID", over(last(cookie_sito_pdisc$external_id_post_evar, TRUE), window2))
# H(cookie_sito_pdisc)

## FUNZIONE PER CALCOLARE L'ACCURACY
score_accuracy <- function(tp, tn, fp, fn){
  accuracy <- round((tp + tn)/(tp + tn + fp + fn) * 100, 1)
  return(accuracy)
}

## FUNZIONE PER CALCOLARE LA PRECISION
score_precision <- function(tp, fp){
  precision <- round(tp/(tp + fp) * 100, 1)
  return(precision)
}

## FUNZIONE PER CALCOLARE LA RECALL
score_recall <- function(tp, fn){
  recall <- round(tp/(tp + fn) * 100, 1)
  return(recall)
}

test_cleaning <- function(df0){
  df0 <- drop(df0, "COD_CLIENTE_CIFRATO")
  df0 <- drop(df0, "DAT_PRIMA_ATTIVAZIONE_dt")
  df0 <- drop(df0, "INGR_PDISC_dt")
  df0 <- drop(df0, "month_pdisc")
  df0$meno1M_FASCIA_SDM_PDISC_M1 <- cast(df0$meno1M_FASCIA_SDM_PDISC_M1, "integer")
  df0$meno1M_FASCIA_SDM_PDISC_M4 <- cast(df0$meno1M_FASCIA_SDM_PDISC_M4, "integer")
  df0 <- dropna(df0, how = "any", cols = c("meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4"))
  
  return(df0)
}

denseVectorToArray <- function(dv) {
  SparkR:::callJMethod(dv, "toArray")
}


print_performance <- function(pred, threshold){
  pred <- select(pred, c("CHURN", "label", "prediction"))
  pred <- withColumn(pred, "Pred", lit("TRUE"))
  pred$Pred <- ifelse(pred$prediction > threshold, "TRUE", "FALSE")
  createOrReplaceTempView(pred, "prediction")
  cm <- sql("SELECT CHURN as Real, Pred, count(*) as Freq FROM prediction GROUP BY 1,2")
  cm <- as.data.frame(cm)
  
  true_positive <- cm[cm$Real == "TRUE" & cm$Pred == "TRUE",]$Freq
  true_negative <- cm[cm$Real == "FALSE" & cm$Pred == "FALSE",]$Freq
  false_positive <- cm[cm$Real == "FALSE" & cm$Pred == "TRUE",]$Freq
  false_negative <- cm[cm$Real == "TRUE" & cm$Pred == "FALSE",]$Freq
  
  accuracy <- score_accuracy(true_positive, true_negative, false_positive, false_negative)
  precision <- score_precision(true_positive, false_positive)
  recall <- score_recall(true_positive, false_negative)
  
  View(cm)
  print("--- PERFORMANCE ---")
  print(paste0("ACCURACY --> ", accuracy))
  print(paste0("PRECISION --> ", precision))
  print(paste0("RECALL --> ", recall))
  
  
  return(pred)
}

test_cleaning <- function(df0){
  df0 <- drop(df0, "COD_CLIENTE_CIFRATO")
  df0 <- drop(df0, "DAT_PRIMA_ATTIVAZIONE_dt")
  df0 <- drop(df0, "INGR_PDISC_dt")
  df0 <- drop(df0, "month_pdisc")
  df0$meno1M_FASCIA_SDM_PDISC_M1 <- cast(df0$meno1M_FASCIA_SDM_PDISC_M1, "integer")
  df0$meno1M_FASCIA_SDM_PDISC_M4 <- cast(df0$meno1M_FASCIA_SDM_PDISC_M4, "integer")
  df0 <- dropna(df0, how = "any", cols = c("meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4"))
  
  return(df0)
}

retain <- c("visite_totali_1w",
            "visite_fatture_1w",
            "visite_extra_1w",
            "visite_arricchisciAbbonamento_1w",
            "visite_gestione_1w",
            "visite_contatti_1w",
            "visite_comunicazione_1w",
            "visite_assistenza_1w",
            "visite_GuidaTv_sito_1w",
            "visite_PaginaDiServizio_sito_1w",
            "visite_FaiDaTeExtra_sito_1w",
            "visite_Tecnologia_sito_1w",
            "visite_InfoDisdetta_sito_1w",
            "visite_TrovaSkyService_sito_1w",
            "visite_Trasloco_sito_1w",
            "visite_TrasparenzaTariffaria_sito_1w",
            "visite_SkyExpert_sito_1w",
            "visite_Trigger_sito_1w",    
            "visite_PacchettiOfferte_sito_1w",
            "visite_SearchDisdetta_sito_1w",
            
            "secondi_totali_1w",
            "secondi_fatture_1w",
            "secondi_extra_1w",
            "secondi_arricchisciAbbonamento_1w",
            "secondi_gestione_1w",
            "secondi_contatti_1w",
            "secondi_comunicazione_1w",
            "secondi_assistenza_1w",
            "secondi_GuidaTv_sito_1w",
            "secondi_PaginaDiServizio_sito_1w",
            "secondi_FaiDaTeExtra_sito_1w",
            "secondi_Tecnologia_sito_1w",
            "secondi_InfoDisdetta_sito_1w",
            "secondi_TrovaSkyService_sito_1w",
            "secondi_Trasloco_sito_1w",
            "secondi_TrasparenzaTariffaria_sito_1w",
            "secondi_SkyExpert_sito_1w",
            "secondi_Trigger_sito_1w",    
            "secondi_PacchettiOfferte_sito_1w",
            "secondi_SearchDisdetta_sito_1w",
            
            "visite_totali_2w",
            "visite_fatture_2w",
            "visite_extra_2w",
            "visite_arricchisciAbbonamento_2w",
            "visite_gestione_2w",
            "visite_contatti_2w",
            "visite_comunicazione_2w",
            "visite_assistenza_2w",
            "visite_GuidaTv_sito_2w",
            "visite_PaginaDiServizio_sito_2w",
            "visite_FaiDaTeExtra_sito_2w",
            "visite_Tecnologia_sito_2w",
            "visite_InfoDisdetta_sito_2w",
            "visite_TrovaSkyService_sito_2w",
            "visite_Trasloco_sito_2w",
            "visite_TrasparenzaTariffaria_sito_2w",
            "visite_SkyExpert_sito_2w",
            "visite_Trigger_sito_2w",    
            "visite_PacchettiOfferte_sito_2w",
            "visite_SearchDisdetta_sito_2w",
            
            "secondi_totali_2w",
            "secondi_fatture_2w",
            "secondi_extra_1w",
            "secondi_arricchisciAbbonamento_2w",
            "secondi_gestione_2w",
            "secondi_contatti_2w",
            "secondi_comunicazione_2w",
            "secondi_assistenza_2w",
            "secondi_GuidaTv_sito_2w",
            "secondi_PaginaDiServizio_sito_2w",
            "secondi_FaiDaTeExtra_sito_2w",
            "secondi_Tecnologia_sito_2w",
            "secondi_InfoDisdetta_sito_2w",
            "secondi_TrovaSkyService_sito_2w",
            "secondi_Trasloco_sito_2w",
            "secondi_TrasparenzaTariffaria_sito_2w",
            "secondi_SkyExpert_sito_2w",
            "secondi_Trigger_sito_2w",    
            "secondi_PacchettiOfferte_sito_2w",
            "secondi_SearchDisdetta_sito_2w",
            
            "visite_totali_1m",
            "visite_fatture_1m",
            "visite_extra_1m",
            "visite_arricchisciAbbonamento_1m",
            "visite_gestione_1m",
            "visite_contatti_1m",
            "visite_comunicazione_1m",
            "visite_assistenza_1m",
            "visite_GuidaTv_sito_1m",
            "visite_PaginaDiServizio_sito_1m",
            "visite_FaiDaTeExtra_sito_1m",
            "visite_Tecnologia_sito_1m",
            "visite_InfoDisdetta_sito_1m",
            "visite_TrovaSkyService_sito_1m",
            "visite_Trasloco_sito_1m",
            "visite_TrasparenzaTariffaria_sito_1m",
            "visite_SkyExpert_sito_1m",
            "visite_Trigger_sito_1m",    
            "visite_PacchettiOfferte_sito_1m",
            "visite_SearchDisdetta_sito_1m",
            
            "secondi_totali_1m",
            "secondi_fatture_1m",
            "secondi_extra_1m",
            "secondi_arricchisciAbbonamento_1m",
            "secondi_gestione_1m",
            "secondi_contatti_1m",
            "secondi_comunicazione_1m",
            "secondi_assistenza_1m",
            "secondi_GuidaTv_sito_1m",
            "secondi_PaginaDiServizio_sito_1m",
            "secondi_FaiDaTeExtra_sito_1m",
            "secondi_Tecnologia_sito_1m",
            "secondi_InfoDisdetta_sito_1m",
            "secondi_TrovaSkyService_sito_1m",
            "secondi_Trasloco_sito_1m",
            "secondi_TrasparenzaTariffaria_sito_1m",
            "secondi_SkyExpert_sito_1m",
            "secondi_Trigger_sito_1m",    
            "secondi_PacchettiOfferte_sito_1m",
            "secondi_SearchDisdetta_sito_1m",
            
            "visite_totali_2m",
            "visite_fatture_2m",
            "visite_extra_2m",
            "visite_arricchisciAbbonamento_2m",
            "visite_gestione_2m",
            "visite_contatti_2m",
            "visite_comunicazione_2m",
            "visite_assistenza_2m",
            "visite_GuidaTv_sito_2m",
            "visite_PaginaDiServizio_sito_2m",
            "visite_FaiDaTeExtra_sito_2m",
            "visite_Tecnologia_sito_2m",
            "visite_InfoDisdetta_sito_2m",
            "visite_TrovaSkyService_sito_2m",
            "visite_Trasloco_sito_2m",
            "visite_TrasparenzaTariffaria_sito_2m",
            "visite_SkyExpert_sito_2m",
            "visite_Trigger_sito_2m",    
            "visite_PacchettiOfferte_sito_2m",
            "visite_SearchDisdetta_sito_2m",
            
            "secondi_totali_2m",
            "secondi_fatture_2m",
            "secondi_extra_2m",
            "secondi_arricchisciAbbonamento_2m",
            "secondi_gestione_2m",
            "secondi_contatti_2m",
            "secondi_comunicazione_2m",
            "secondi_assistenza_2m",
            "secondi_GuidaTv_sito_2m",
            "secondi_PaginaDiServizio_sito_2m",
            "secondi_FaiDaTeExtra_sito_2m",
            "secondi_Tecnologia_sito_2m",
            "secondi_InfoDisdetta_sito_2m",
            "secondi_TrovaSkyService_sito_2m",
            "secondi_Trasloco_sito_2m",
            "secondi_TrasparenzaTariffaria_sito_2m",
            "secondi_SkyExpert_sito_2m",
            "secondi_Trigger_sito_2m",    
            "secondi_PacchettiOfferte_sito_2m",
            "secondi_SearchDisdetta_sito_2m",
            
            "meno1M_FASCIA_SDM_PDISC_M1",
            "meno1M_FASCIA_SDM_PDISC_M4",
            "CHURN"
)
