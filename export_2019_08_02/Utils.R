## LAUNCH SPARK 
source("connection_R.R")

## OPTIONS
options(scipen = 10000)

## LIBRERIE
library(magrittr)
library(stringr)


## PULISCO CACHE
# clearCache()

## PULISCO WS
# rm(list = ls())


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

join_post_visid_concatenated <- function(df1, df2){
  df2 <- withColumnRenamed(df2, "post_visid_concatenated", "post_visid_concatenated_y")
  df1 <- merge(df1, df2, all.x = T, by.x = "post_visid_concatenated", by.y = "post_visid_concatenated_y")
  df1 <- drop(df1, "post_visid_concatenated_y")
  return(df1)
}

join_external_id_post_evar <- function(df1, df2){
  df2 <- withColumnRenamed(df2, "external_id_post_evar", "external_id_post_evar_y")
  df1 <- merge(df1, df2, all.x = T, by.x = "external_id_post_evar", by.y = "external_id_post_evar_y")
  df1 <- drop(df1, "external_id_post_evar_y")
  return(df1)
}

## CUSTOM FILTER ON LIST
# filter_on_count <- function(df1, df2, n){
#   df2 <- withColumnRenamed(df2, "COD_CLIENTE_CIFRATO", "COD_CLIENTE_CIFRATO_y")
#   df1 <- merge(df1, df2, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_y")
#   df1 <- drop(df1, "COD_CLIENTE_CIFRATO_y")
#   df1 <- filter(df1, df1$`count(1)` <= n)
#   
#   return(df1)
# }


#### KPI SITO ----

add_kpi_sito <- function(df) {
  # Creo la concatenazione cookie-visit_num
  df <- withColumn(df, "visit_uniq", concat_ws('-', df$post_visid_concatenated, df$visit_num))
  
  # Aggiugno colonne necessarie ai nuovi kpi
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,
            -- Visite per categoria
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate fatture pagamenti') THEN visit_uniq ELSE Null END AS visit_Fatture,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate extra') THEN visit_uniq ELSE Null END AS visit_FaiDaTeExtra,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate arricchisci abbonamento') THEN visit_uniq ELSE Null END AS visit_ArricchischiAbbonamento,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate gestisci dati servizi') THEN visit_uniq ELSE Null END AS visit_Gestione,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate webtracking') THEN visit_uniq ELSE Null END AS visit_Comunicazione,
            CASE WHEN (terzo_livello_post_prop LIKE 'contatta') THEN visit_uniq ELSE Null END AS visit_Contatti,
            CASE WHEN (terzo_livello_post_prop LIKE 'conosci' OR 
            terzo_livello_post_prop LIKE 'home' OR
            terzo_livello_post_prop LIKE 'gestisci' OR 
            terzo_livello_post_prop LIKE 'ricerca' OR 
            terzo_livello_post_prop LIKE 'risolvi') THEN visit_uniq ELSE Null END AS visit_Assistenza,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky') THEN visit_uniq ELSE Null END AS visit_Trasloco,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasparenza tariffaria') THEN visit_uniq ELSE Null END AS visit_TrasparenzaTariffaria,
            CASE WHEN (secondo_livello_post_prop LIKE '%guidatv%' OR secondo_livello_post_prop LIKE '%Guidatv%') THEN visit_uniq ELSE Null END AS visit_GuidaTv,
            CASE WHEN secondo_livello_post_prop LIKE 'pagine di servizio' THEN visit_uniq ELSE Null END AS visit_PaginaDiServizio,
            CASE WHEN secondo_livello_post_prop LIKE 'extra' THEN visit_uniq ELSE Null END AS visit_Extra,
            CASE WHEN secondo_livello_post_prop LIKE 'tecnologia' THEN visit_uniq ELSE Null END AS visit_Tecnologia,
            CASE WHEN secondo_livello_post_prop LIKE 'pacchetti-offerte' THEN visit_uniq ELSE Null END AS visit_PacchettiOfferte,
            CASE WHEN terzo_livello_post_prop LIKE 'info disdetta' THEN visit_uniq ELSE Null END AS visit_InfoDisdetta,
            CASE WHEN terzo_livello_post_prop LIKE 'trova sky service' THEN visit_uniq ELSE Null END AS visit_TrovaSkyService,
            CASE WHEN (page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%') THEN visit_uniq ELSE Null END AS visit_SearchDisdetta,
            CASE WHEN terzo_livello_post_prop LIKE 'sky expert' THEN visit_uniq ELSE Null END AS visit_SkyExpert,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky' OR 
            terzo_livello_post_prop LIKE 'trasparenza tariffaria' OR
            terzo_livello_post_prop LIKE 'trova sky service' OR
            page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%' OR
            terzo_livello_post_prop LIKE 'sky expert') THEN visit_uniq ELSE Null END AS visit_Trigger,
            CASE WHEN page_url_post_prop LIKE '%sky.it/pacchetti-offerte/sky-calcio/dazn%' OR 
                                        page_url_post_prop LIKE '%sky.it/assistenza/conosci/programmazione%' OR 
                                        page_url_post_prop LIKE '%sky.it/assistenza/conosci/supporto-dazn%' THEN visit_num ELSE Null END AS visit_infoDAZN,
            
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
    groupBy(df, "external_id_post_evar"),
    # visite
    visite_totali = countDistinct(df$visit_uniq), 
    visite_Fatture = countDistinct(df$visit_Fatture),
    visite_FaiDaTeExtra = countDistinct(df$visit_FaiDaTeExtra),
    visite_ArricchisciAbbonamento = countDistinct(df$visit_ArricchischiAbbonamento),
    visite_Gestione = countDistinct(df$visit_Gestione),
    visite_Contatti = countDistinct(df$visit_Contatti),
    visite_Comunicazione = countDistinct(df$visit_Comunicazione),
    visite_Assistenza = countDistinct(df$visit_Assistenza),
    visite_Trasloco = countDistinct(df$visit_Trasloco),
    visite_TrasparenzaTariffaria = countDistinct(df$visit_TrasparenzaTariffaria),
    visite_GuidaTv = countDistinct(df$visit_GuidaTv),
    visite_PaginaDiServizio = countDistinct(df$visit_PaginaDiServizio),
    visite_Extra = countDistinct(df$visit_Extra),
    visite_Tecnologia = countDistinct(df$visit_Tecnologia),
    visite_PacchettiOfferte = countDistinct(df$visit_PacchettiOfferte),
    visite_InfoDisdetta = countDistinct(df$visit_InfoDisdetta),
    visite_TrovaSkyService = countDistinct(df$visit_TrovaSkyService),
    visite_SearchDisdetta = countDistinct(df$visit_SearchDisdetta),
    visite_SkyExpert = countDistinct(df$visit_SkyExpert),
    visite_Trigger = countDistinct(df$visit_Trigger),
    visite_indoDAZN = countDistinct(df$visit_infoDAZN),
    
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
            CASE WHEN page_name_post_evar LIKE 'arricchisci abbonamento:lista voucher' THEN visit_num ELSE Null END AS visit_InfoDAZN,
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
    groupBy(df, "external_id_post_evar"),
    # visite
    visite_totali = countDistinct(df$visit_num),
    visite_fatture = countDistinct(df$visit_fatture),
    visite_extra = countDistinct(df$visit_extra),
    visite_arricchisci_abbonamento = countDistinct(df$visit_arricchisci_abbonamento),
    visite_stato_abbonamento = countDistinct(df$visit_stato_abbonamento),
    visite_contatta_sky = countDistinct(df$visit_contatta_sky),
    visite_assistenza_e_supporto = countDistinct(df$visit_assistenza_e_supporto),
    visite_comunicazioni = countDistinct(df$visit_comunicazioni),
    visite_gestisci_servizi = countDistinct(df$visit_gestisci_servizi),
    visite_mio_abbonamento = countDistinct(df$visit_mio_abbonamento),
    visite_info = countDistinct(df$visit_info),
    visite_InfoDAZN = countDistinct(df$visit_InfoDAZN),
    
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
  df0 <- orderBy(df0, "external_id_post_evar", "date_time_ts")
  
  ## AGGIUNGO TIMESTAMP DELLA VISITA ALLA PAGINA SUCCESSIVA
  mywindow = orderBy(windowPartitionBy(df0$external_id_post_evar), desc(df0$date_time_ts))
  df0 <- withColumn(df0, "nextvisit", over(lag(df0$date_time_ts), mywindow))
  df0 <- orderBy(df0, asc(df0$external_id_post_evar), asc(df0$date_time_ts))
  
  ## CALCOLO LA DURATA DI VIEW DI UNA PAGINA
  createOrReplaceTempView(df0, "df0")
  df0 <- sql("SELECT *, CAST(nextvisit AS LONG) - CAST(date_time_ts AS LONG) as sec_on_page from df0")
  df0 <- filter(df0, isNull(df0$sec_on_page) | df0$sec_on_page >= duration)
  df0$sec_on_page <- ifelse(df0$sec_on_page >= 3600, NA, df0$sec_on_page)
  df0 <- withColumn(df0,  "min_on_page", round(df0$sec_on_page / 60))
}


melt_kpi <- function(df0){
  
    df0 <- withColumn(df0, "visite_totali", lit( df0[["visite_totali_sito"]] + df0[["visite_totali_app"]] ))
    df0 <- withColumn(df0, "visite_fatture", lit( df0[["visite_fatture_sito"]] + df0[["visite_fatture_app"]]  ))
    df0 <- withColumn(df0, "visite_extra", lit(df0[["visite_extra_sito"]] + df0[["visite_extra_app"]]))
    df0 <- withColumn(df0, "visite_arricchisciAbbonamento", lit(df0[["visite_ArricchisciAbbonamento_sito"]] + df0[["visite_arricchisci_abbonamento_app"]]))
    df0 <- withColumn(df0, "visite_gestione", lit(df0[["visite_Gestione_sito"]] + df0[["visite_gestisci_servizi_app"]] ))
    df0 <- withColumn(df0, "visite_contatti", lit(df0[["visite_Contatti_sito"]] + df0[["visite_contatta_sky_app"]]))
    df0 <- withColumn(df0, "visite_comunicazione", lit(df0[["visite_Comunicazione_sito"]] + df0[["visite_comunicazioni_app"]]))
    df0 <- withColumn(df0, "visite_assistenza", lit(df0[["visite_Assistenza_sito"]] + df0[["visite_assistenza_e_supporto_app"]]))
    
    df0 <- withColumn(df0,"secondi_totali", lit( df0[["secondi_totali_sito"]] + df0[["secondi_totali_app"]] ))
    df0 <- withColumn(df0, "secondi_fatture", lit( df0[["secondi_fatture_sito"]] + df0[["secondi_fatture_app"]]  ))
    df0 <- withColumn(df0, "secondi_extra", lit(df0[["secondi_extra_sito"]] + df0[["secondi_extra_app"]]))
    df0 <- withColumn(df0, "secondi_arricchisciAbbonamento", lit(df0[["secondi_ArricchisciAbbonamento_sito"]] + df0[["secondi_arricchisci_abbonamento_app"]]))
    df0 <- withColumn(df0, "secondi_gestione", lit(df0[["secondi_Gestione_sito"]] + df0[["secondi_gestisci_servizi_app"]] ))
    df0 <- withColumn(df0, "secondi_contatti", lit(df0[["secondi_Contatti_sito"]] + df0[["secondi_contatta_sky_app"]]))
    df0 <- withColumn(df0, "secondi_comunicazione", lit(df0[["secondi_Comunicazione_sito"]] + df0[["secondi_comunicazioni_app"]]))
    df0 <- withColumn(df0, "secondi_assistenza", lit(df0[["secondi_Assistenza_sito"]] + df0[["secondi_assistenza_e_supporto_app"]]))
    
  return(df0)
}


retain <- c("external_id_post_evar",
            
            "visite_totali_sito",
            "visite_Fatture_sito",
            "visite_FaiDaTeExtra_sito",
            "visite_ArricchisciAbbonamento_sito",
            "visite_Gestione_sito",
            "visite_Contatti_sito",
            "visite_Comunicazione_sito",
            "visite_Assistenza_sito",
            "visite_Trasloco_sito",
            "visite_TrasparenzaTariffaria_sito",
            "visite_GuidaTv_sito",
            "visite_PaginaDiServizio_sito",
            "visite_Extra_sito",
            "visite_Tecnologia_sito",
            "visite_PacchettiOfferte_sito",
            "visite_InfoDisdetta_sito",
            "visite_TrovaSkyService_sito",
            "visite_SearchDisdetta_sito",
            "visite_SkyExpert_sito",
            "visite_Trigger_sito",
            "visite_indoDAZN_sito",
            
            # "secondi_totali_sito",
            # "secondi_Fatture_sito",
            # "secondi_FaiDaTeExtra_sito",
            # "secondi_ArricchisciAbbonamento_sito",
            # "secondi_Gestione_sito",
            # "secondi_Contatti_sito",
            # "secondi_Comunicazione_sito",
            # "secondi_Assistenza_sito",
            # "secondi_Trasloco_sito",
            # "secondi_TrasparenzaTariffaria_sito",
            # "secondi_GuidaTv_sito",
            # "secondi_PaginaDiServizio_sito",
            # "secondi_Extra_sito",
            # "secondi_Tecnologia_sito",
            # "secondi_PacchettiOfferte_sito",
            # "secondi_InfoDisdetta_sito",
            # "secondi_TrovaSkyService_sito",
            # "secondi_SearchDisdetta_sito",
            # "secondi_SkyExpert_sito",
            # "secondi_Trigger_sito",
            
            "visite_totali_app",
            "visite_fatture_app",
            "visite_extra_app",
            "visite_arricchisci_abbonamento_app",
            "visite_stato_abbonamento_app",
            "visite_contatta_sky_app",
            "visite_assistenza_e_supporto_app",
            "visite_comunicazioni_app",
            "visite_gestisci_servizi_app",
            "visite_mio_abbonamento_app",
            "visite_info_app",
            "visite_InfoDAZN_app",
            
            # "secondi_totali_app",
            # "secondi_fatture_app",
            # "secondi_extra_app",
            # "secondi_arricchisci_abbonamento_app",
            # "secondi_stato_abbonamento_app",
            # "secondi_contatta_sky_app",
            # "secondi_assistenza_e_supporto_app",
            # "secondi_comunicazioni_app",
            # "secondi_gestisci_servizi_app",
            # "secondi_mio_abbonamento_app",
            # "secondi_info_app",
            
            "visite_totali",
            "visite_fatture",
            "visite_extra",
            "visite_arricchisciAbbonamento",
            "visite_gestione",
            "visite_contatti",
            "visite_comunicazione",
            "visite_assistenza" #,
            
            # "secondi_totali",
            # "secondi_fatture",
            # "secondi_extra",
            # "secondi_arricchisciAbbonamento",
            # "secondi_gestione",
            # "secondi_contatti",
            # "secondi_comunicazione",
            # "secondi_assistenza"
)
