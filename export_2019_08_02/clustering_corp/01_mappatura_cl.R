
## Mappatura di sito corporate e app wsc ##

source("connection_R.R")
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


join_external_id_post_evar <- function(df1, df2){
  df2 <- withColumnRenamed(df2, "external_id_post_evar", "external_id_post_evar_y")
  df1 <- merge(df1, df2, all.x = T, by.x = "external_id_post_evar", by.y = "external_id_post_evar_y")
  df1 <- drop(df1, "external_id_post_evar_y")
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


#### KPI SITO ###################################################################################################################################################

start_kpi_sito <- function(df, clienti){
  
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *, datediff(data, date_time_dt) as diff_gg_view from df")
  
  # Calcolo i vari df per i kpi
  df_1w <- filter(df, "diff_gg_view <= 7")
  df_2w <- filter(df, "diff_gg_view <= 14")
  df_1m <- filter(df, "diff_gg_view <= 30")
  df_2m <- filter(df, "diff_gg_view <= 60")
  
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
  # Creo la concatenazione cookie-visit_num
  df <- withColumn(df, "visit_uniq", concat_ws('-', df$post_visid_concatenated, df$visit_num))
  
  # Aggiugno colonne necessarie ai nuovi kpi
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,

            -- Visite per categoria
            CASE WHEN secondo_livello_post_prop LIKE 'fai da te' THEN visit_uniq ELSE Null END AS wsc_tot,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate fatture pagamenti') THEN visit_uniq ELSE Null END AS wsc_fatture_pagamenti,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate extra') THEN visit_uniq ELSE Null END AS wsc_extra,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate arricchisci abbonamento') THEN visit_uniq ELSE Null END AS wsc_arricchischi_abbonamento,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate gestisci dati servizi') THEN visit_uniq ELSE Null END AS wsc_gestisci_dati_servizi,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate webtracking') THEN visit_uniq ELSE Null END AS wsc_webtracking_comunicazione,

            CASE WHEN secondo_livello_post_prop LIKE 'assistenza' THEN visit_uniq ELSE Null END AS assistenza_tot,
            CASE WHEN terzo_livello_post_prop LIKE 'contatta' THEN visit_uniq ELSE Null END AS assistenza_contatta,
            CASE WHEN terzo_livello_post_prop LIKE 'conosci' THEN visit_uniq ELSE Null END AS assistenza_conosci,
            CASE WHEN terzo_livello_post_prop LIKE 'home' THEN visit_uniq ELSE Null END AS assistenza_home,
            CASE WHEN terzo_livello_post_prop LIKE 'gestisci' THEN visit_uniq ELSE Null END AS assistenza_gestisci,
            CASE WHEN terzo_livello_post_prop LIKE 'ricerca' THEN visit_uniq ELSE Null END AS assistenza_ricerca,
            CASE WHEN terzo_livello_post_prop LIKE 'risolvi' THEN visit_uniq ELSE Null END AS assistenza_risolvi,

            -- CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky') THEN visit_uniq ELSE Null END AS visit_Trasloco,
            -- CASE WHEN (terzo_livello_post_prop LIKE 'trasparenza tariffaria') THEN visit_uniq ELSE Null END AS visit_TrasparenzaTariffaria,
            CASE WHEN (secondo_livello_post_prop LIKE '%guidatv%' OR secondo_livello_post_prop LIKE '%Guidatv%') THEN visit_uniq ELSE Null END AS guida_tv,
            CASE WHEN secondo_livello_post_prop LIKE 'pagine di servizio' THEN visit_uniq ELSE Null END AS pagine_di_servizio,
            CASE WHEN secondo_livello_post_prop LIKE 'extra' THEN visit_uniq ELSE Null END AS extra,
            CASE WHEN secondo_livello_post_prop LIKE 'tecnologia' THEN visit_uniq ELSE Null END AS tecnologia,
            CASE WHEN secondo_livello_post_prop LIKE 'pacchetti-offerte' THEN visit_uniq ELSE Null END AS pacchetti_offerte,
            CASE WHEN terzo_livello_post_prop LIKE 'info disdetta' THEN visit_uniq ELSE Null END AS visit_InfoDisdetta,
            -- CASE WHEN terzo_livello_post_prop LIKE 'trova sky service' THEN visit_uniq ELSE Null END AS visit_TrovaSkyService,
            CASE WHEN (page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%') THEN visit_uniq ELSE Null END AS visit_SearchDisdetta,
            -- CASE WHEN terzo_livello_post_prop LIKE 'sky expert' THEN visit_uniq ELSE Null END AS visit_SkyExpert,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky' OR
                terzo_livello_post_prop LIKE 'trasparenza tariffaria' OR
                terzo_livello_post_prop LIKE 'trova sky service' OR
                page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%' OR
                terzo_livello_post_prop LIKE 'sky expert') THEN visit_uniq ELSE Null END AS visit_Trigger,
            -- CASE WHEN (page_url_post_prop LIKE '%sky.it/pacchetti-offerte/sky-calcio/dazn%' OR 
                -- page_url_post_prop LIKE '%sky.it/assistenza/conosci/programmazione%' OR 
                -- page_url_post_prop LIKE '%sky.it/assistenza/conosci/supporto-dazn%') THEN visit_uniq ELSE Null END AS visit_info_DAZN,

            -- Secondi per categoria
            CASE WHEN secondo_livello_post_prop LIKE 'faidate' THEN sec_on_page ELSE Null END AS sec_wsc_tot,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate fatture pagamenti') THEN sec_on_page ELSE Null END AS sec_wsc_fatture_pagamenti,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate extra') THEN sec_on_page ELSE Null END AS sec_wsc_extra,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate arricchisci abbonamento') THEN sec_on_page ELSE Null END AS sec_wsc_arricchischi_abbonamento,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate gestisci dati servizi') THEN sec_on_page ELSE Null END AS sec_wsc_gestisci_dati_servizi,
            CASE WHEN (terzo_livello_post_prop LIKE 'faidate webtracking') THEN sec_on_page ELSE Null END AS sec_wsc_webtracking_comunicazione,

            CASE WHEN secondo_livello_post_prop LIKE 'assistenza' THEN sec_on_page ELSE Null END AS sec_assistenza_tot,
            CASE WHEN terzo_livello_post_prop LIKE 'contatta' THEN sec_on_page ELSE Null END AS sec_assistenza_contatta,
            CASE WHEN terzo_livello_post_prop LIKE 'conosci' THEN sec_on_page ELSE Null END AS sec_assistenza_conosci,
            CASE WHEN terzo_livello_post_prop LIKE 'home' THEN sec_on_page ELSE Null END AS sec_assistenza_home,
            CASE WHEN terzo_livello_post_prop LIKE 'gestisci' THEN sec_on_page ELSE Null END AS sec_assistenza_gestisci,
            CASE WHEN terzo_livello_post_prop LIKE 'ricerca' THEN sec_on_page ELSE Null END AS sec_assistenza_ricerca,
            CASE WHEN terzo_livello_post_prop LIKE 'risolvi' THEN sec_on_page ELSE Null END AS sec_assistenza_risolvi,
            -- CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky') THEN sec_on_page ELSE Null END AS sec_visit_Trasloco,
            -- CASE WHEN (terzo_livello_post_prop LIKE 'trasparenza tariffaria') THEN sec_on_page ELSE Null END AS sec_visit_TrasparenzaTariffaria,

            CASE WHEN (secondo_livello_post_prop LIKE '%guidatv%' OR secondo_livello_post_prop LIKE '%Guidatv%') THEN sec_on_page ELSE Null END AS sec_guida_tv,
            CASE WHEN secondo_livello_post_prop LIKE 'pagine di servizio' THEN sec_on_page ELSE Null END AS sec_pagine_di_servizio,
            CASE WHEN secondo_livello_post_prop LIKE 'extra' THEN sec_on_page ELSE Null END AS sec_extra,
            CASE WHEN secondo_livello_post_prop LIKE 'tecnologia' THEN sec_on_page ELSE Null END AS sec_tecnologia,
            CASE WHEN secondo_livello_post_prop LIKE 'pacchetti-offerte' THEN sec_on_page ELSE Null END AS sec_pacchetti_offerte,
            CASE WHEN terzo_livello_post_prop LIKE 'info disdetta' THEN sec_on_page ELSE Null END AS sec_visit_InfoDisdetta,
            -- CASE WHEN terzo_livello_post_prop LIKE 'trova sky service' THEN sec_on_page ELSE Null END AS sec_visit_TrovaSkyService,
            CASE WHEN (page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%') THEN sec_on_page ELSE Null END AS sec_visit_SearchDisdetta,
            -- CASE WHEN terzo_livello_post_prop LIKE 'sky expert' THEN sec_on_page ELSE Null END AS sec_visit_SkyExpert,
            CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky' OR
                terzo_livello_post_prop LIKE 'trasparenza tariffaria' OR
                terzo_livello_post_prop LIKE 'trova sky service' OR
                page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%' OR
                terzo_livello_post_prop LIKE 'sky expert') THEN sec_on_page ELSE Null END AS sec_visit_Trigger
            -- CASE WHEN (page_url_post_prop LIKE '%sky.it/pacchetti-offerte/sky-calcio/dazn%' OR 
                -- page_url_post_prop LIKE '%sky.it/assistenza/conosci/programmazione%' OR 
                -- page_url_post_prop LIKE '%sky.it/assistenza/conosci/supporto-dazn%') THEN sec_on_page ELSE Null END AS sec_visit_info_DAZN

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
    
    visite_FaiDaTe = countDistinct(df$wsc_tot),
    visite_Fatture = countDistinct(df$wsc_fatture_pagamenti),
    visite_FaiDaTeExtra = countDistinct(df$wsc_extra),
    visite_ArricchisciAbbonamento = countDistinct(df$wsc_arricchischi_abbonamento),
    visite_Gestione = countDistinct(df$wsc_gestisci_dati_servizi),
    visite_Comunicazione = countDistinct(df$wsc_webtracking_comunicazione),
    
    visite_Assistenza = countDistinct(df$assistenza_tot),
    visite_Assitenza_Contatta = countDistinct(df$assistenza_contatta),
    visite_Assitenza_Conosci = countDistinct(df$assistenza_conosci),
    visite_Assitenza_Home = countDistinct(df$assistenza_home),
    visite_Assitenza_Gestisci = countDistinct(df$assistenza_gestisci),
    visite_Assitenza_Ricerca = countDistinct(df$assistenza_ricerca),
    visite_Assitenza_Risolvi = countDistinct(df$assistenza_risolvi),
    
    # visite_Trasloco = countDistinct(df$visit_Trasloco),
    # visite_TrasparenzaTariffaria = countDistinct(df$visit_TrasparenzaTariffaria),
    visite_GuidaTv = countDistinct(df$guida_tv),
    visite_PaginaDiServizio = countDistinct(df$pagine_di_servizio),
    visite_Extra = countDistinct(df$extra),
    visite_Tecnologia = countDistinct(df$tecnologia),
    visite_PacchettiOfferte = countDistinct(df$pacchetti_offerte),
    visite_InfoDisdetta = countDistinct(df$visit_InfoDisdetta),
    # visite_TrovaSkyService = countDistinct(df$visit_TrovaSkyService),
    visite_SearchDisdetta = countDistinct(df$visit_SearchDisdetta),
    # visite_SkyExpert = countDistinct(df$visit_SkyExpert),
    visite_Trigger = countDistinct(df$visit_Trigger),
    # visite_infoDAZN = countDistinct(df$visit_info_DAZN),
    
    # secondi
    secondi_totali = sum(df$sec_on_page, na.rm = T),
    
    secondi_FaiDaTe = sum(df$sec_wsc_tot, na.rm = T),
    secondi_Fatture = sum(df$sec_wsc_fatture_pagamenti, na.rm = T),
    secondi_FaiDaTeExtra = sum(df$sec_wsc_extra, na.rm = T),
    secondi_ArricchisciAbbonamento = sum(df$sec_wsc_arricchischi_abbonamento, na.rm = T),
    secondi_Gestione = sum(df$sec_wsc_gestisci_dati_servizi, na.rm = T),
    secondi_Comunicazione = sum(df$wsc_webtracking_comunicazione, na.rm = T),
    
    secondi_Assistenza = sum(df$sec_assistenza_tot, na.rm = T),
    secondi_Assitenza_Contatta = sum(df$sec_assistenza_contatta, na.rm = T),
    secondi_Assitenza_Conosci = sum(df$sec_assistenza_conosci, na.rm = T),
    secondi_Assitenza_Home = sum(df$sec_assistenza_home, na.rm = T),
    secondi_Assitenza_Gestisci = sum(df$sec_assistenza_gestisci, na.rm = T),
    secondi_Assitenza_Ricerca = sum(df$sec_assistenza_ricerca, na.rm = T),
    secondi_Assitenza_Risolvi = sum(df$sec_assistenza_risolvi, na.rm = T),
    
    # secondi_Trasloco = sum(df$sec_Trasloco, na.rm = T),
    # secondi_TrasparenzaTariffaria = sum(df$sec_TrasparenzaTariffaria, na.rm = T),
    secondi_GuidaTv = sum(df$sec_guida_tv, na.rm = T),
    secondi_PaginaDiServizio = sum(df$sec_pagine_di_servizio, na.rm = T),
    secondi_Extra = sum(df$sec_extra, na.rm = T),
    secondi_Tecnologia = sum(df$sec_tecnologia, na.rm = T),
    secondi_PacchettiOfferte = sum(df$sec_pacchetti_offerte, na.rm = T),
    secondi_InfoDisdetta = sum(df$sec_visit_InfoDisdetta, na.rm = T),
    # secondi_TrovaSkyService = sum(df$sec_visit_TrovaSkyService, na.rm = T),
    secondi_SearchDisdetta = sum(df$sec_visit_SearchDisdetta, na.rm = T),
    # secondi_SkyExpert = sum(df$sec_visit_SkyExpert, na.rm = T),
    secondi_Trigger = sum(df$sec_visit_Trigger, na.rm = T)
    # secondi_infoDAZN = sum(df$sec_visit_info_DAZN)
  )
  
  return(df_kpi)
}


#### KPI APP###################################################################################################################################################

start_kpi_app <- function(df, clienti){
  
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *, datediff(data, date_time_dt) as diff_gg_view from df")
  
  # Calcolo i vari df per i kpi
  df_1w <- filter(df, "diff_gg_view <= 7")
  df_2w <- filter(df, "diff_gg_view <= 14")
  df_1m <- filter(df, "diff_gg_view <= 30")
  df_2m <- filter(df, "diff_gg_view <= 60")
  
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
            CASE WHEN (site_section LIKE 'fatture' OR site_section LIKE 'dati e fatture') THEN visit_num ELSE Null END AS app_dati_fatture,
            CASE WHEN site_section LIKE 'widget' THEN visit_num ELSE Null END AS app_widget,
              CASE WHEN page_name_post_prop LIKE 'widget:dispositivi%' THEN visit_num ELSE Null END AS app_widget_dispositivi,
              CASE WHEN page_name_post_prop LIKE 'widget:ultimafattura%' THEN visit_num ELSE Null END AS app_widget_ultima_fattura,
              CASE WHEN page_name_post_prop LIKE 'widget:contatta sky%' THEN visit_num ELSE Null END AS app_widget_contatta,
              CASE WHEN page_name_post_prop LIKE 'widget:gestisci%' THEN visit_num ELSE Null END AS app_widget_gestisci,
            CASE WHEN site_section LIKE '%assistenza%' THEN visit_num ELSE Null END AS app_assistenza,
              CASE WHEN page_name_post_prop LIKE 'assistenza:home%' THEN visit_num ELSE Null END AS app_assistenza_home,
              CASE WHEN page_name_post_prop LIKE 'assistenza:contatta%' THEN visit_num ELSE Null END AS app_assistenza_contatta,
              CASE WHEN page_name_post_prop LIKE 'assistenza:conosci%' THEN visit_num ELSE Null END AS app_assistenza_conosci,
              CASE WHEN page_name_post_prop LIKE 'assistenza:ricerca%' THEN visit_num ELSE Null END AS app_assistenza_ricerca,
              CASE WHEN page_name_post_prop LIKE 'assistenza:gestisci%' THEN visit_num ELSE Null END AS app_assistenza_gestisci,
              CASE WHEN page_name_post_prop LIKE 'assistenza:risolvi%' THEN visit_num ELSE Null END AS app_assistenza_risolvi,
            CASE WHEN site_section LIKE '%extra%' THEN visit_num ELSE Null END AS app_extra,
            CASE WHEN site_section LIKE 'arricchisci abbonamento' THEN visit_num ELSE Null END AS app_arricchisci_abbonamento,
            CASE WHEN (site_section LIKE 'stato abbonamento' OR site_section LIKE 'gestione servizi' OR site_section LIKE 'i miei dati' OR
                      site_section LIKE 'il mio abbonamento') THEN visit_num ELSE Null END AS app_stato_abbonamento,
            -- ATTENZIONE!!! RICORDARSI DI VERIFICARE LE SEZIONI !!!!
            CASE WHEN site_section LIKE 'contatta sky' THEN visit_num ELSE Null END AS app_contatta_sky,
            CASE WHEN site_section LIKE 'impostazioni' THEN visit_num ELSE Null END AS app_impostazioni,
            CASE WHEN site_section LIKE 'comunicazioni' THEN visit_num ELSE Null END AS app_comunicazioni,
            CASE WHEN site_section LIKE 'gestisci servizi' THEN visit_num ELSE Null END AS app_gestisci_servizi,
            -- CASE WHEN site_section LIKE '%home%' THEN visit_num ELSE Null END AS app_home,
            -- CASE WHEN site_section LIKE 'sky service' THEN visit_num ELSE Null END AS app_sky_service,
            -- CASE WHEN site_section LIKE 'intro' THEN visit_num ELSE Null END AS app_intro,
            -- CASE WHEN site_section LIKE '%primafila%' THEN visit_num ELSE Null END AS app_prima_fila,
            CASE WHEN site_section LIKE 'info' THEN visit_num ELSE Null END AS app_info,
            -- CASE WHEN page_name_post_evar LIKE 'arricchisci abbonamento:lista voucher' THEN visit_num ELSE Null END AS visit_Info_DAZN,

            -- Secondi per categoria
            CASE WHEN (site_section LIKE 'fatture' OR site_section LIKE 'dati e fatture') THEN sec_on_page ELSE Null END AS sec_app_dati_fatture,
            CASE WHEN site_section LIKE 'widget' THEN sec_on_page ELSE Null END AS sec_app_widget,
            CASE WHEN page_name_post_prop LIKE 'widget:dispositivi%' THEN sec_on_page ELSE Null END AS sec_app_widget_dispositivi,
            CASE WHEN page_name_post_prop LIKE 'widget:ultimafattura%' THEN sec_on_page ELSE Null END AS sec_app_widget_ultima_fattura,
            CASE WHEN page_name_post_prop LIKE 'widget:contatta sky%' THEN sec_on_page ELSE Null END AS sec_app_widget_contatta,
            CASE WHEN page_name_post_prop LIKE 'widget:gestisci%' THEN sec_on_page ELSE Null END AS sec_app_widget_gestisci,
            CASE WHEN site_section LIKE '%assistenza%' THEN sec_on_page ELSE Null END AS sec_app_assistenza,
            CASE WHEN page_name_post_prop LIKE 'assistenza:home%' THEN sec_on_page ELSE Null END AS sec_app_assistenza_home,
            CASE WHEN page_name_post_prop LIKE 'assistenza:contatta%' THEN sec_on_page ELSE Null END AS sec_app_assistenza_contatta,
            CASE WHEN page_name_post_prop LIKE 'assistenza:conosci%' THEN sec_on_page ELSE Null END AS sec_app_assistenza_conosci,
            CASE WHEN page_name_post_prop LIKE 'assistenza:ricerca%' THEN sec_on_page ELSE Null END AS sec_app_assistenza_ricerca,
            CASE WHEN page_name_post_prop LIKE 'assistenza:gestisci%' THEN sec_on_page ELSE Null END AS sec_app_assistenza_gestisci,
            CASE WHEN page_name_post_prop LIKE 'assistenza:risolvi%' THEN sec_on_page ELSE Null END AS sec_app_assistenza_risolvi,
            CASE WHEN site_section LIKE '%extra%' THEN sec_on_page ELSE Null END AS sec_app_extra,
            CASE WHEN site_section LIKE 'arricchisci abbonamento' THEN sec_on_page ELSE Null END AS sec_app_arricchisci_abbonamento,
            CASE WHEN (site_section LIKE 'stato abbonamento' OR site_section LIKE 'gestione servizi' OR site_section LIKE 'i miei dati' OR 
                      site_section LIKE 'il mio abbonamento') THEN sec_on_page ELSE Null END AS sec_app_stato_abbonamento,
            -- ATTENZIONE!!! RICORDARSI DI VERIFICARE LE SEZIONI !!!!
            CASE WHEN site_section LIKE 'contatta sky' THEN sec_on_page ELSE Null END AS sec_app_contatta_sky,
            CASE WHEN site_section LIKE 'impostazioni' THEN sec_on_page ELSE Null END AS sec_app_impostazioni,
            CASE WHEN site_section LIKE 'comunicazioni' THEN sec_on_page ELSE Null END AS sec_app_comunicazioni,
            CASE WHEN site_section LIKE 'gestisci servizi' THEN sec_on_page ELSE Null END AS sec_app_gestisci_servizi,
            -- CASE WHEN site_section LIKE '%home%' THEN sec_on_page ELSE Null END AS sec_app_home,
            -- CASE WHEN site_section LIKE 'sky service' THEN sec_on_page ELSE Null END AS sec_app_sky_service,
            -- CASE WHEN site_section LIKE 'intro' THEN sec_on_page ELSE Null END AS sec_app_intro,
            -- CASE WHEN site_section LIKE '%primafila%' THEN sec_on_page ELSE Null END AS sec_app_prima_fila,
            CASE WHEN site_section LIKE 'info' THEN sec_on_page ELSE Null END AS sec_app_info
            -- CASE WHEN page_name_post_evar LIKE 'arricchisci abbonamento:lista voucher' THEN sec_on_page ELSE Null END AS sec_visit_Info_DAZN

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
    visite_fatture = countDistinct(df$app_dati_fatture),
    
    visite_widget = countDistinct(df$app_widget),
    visite_widget_dispositivi = countDistinct(df$app_widget_dispositivi),
    visite_widget_ultima_fattura = countDistinct(df$app_widget_ultima_fattura),
    visite_widget_contatta = countDistinct(df$app_widget_contatta),
    visite_widget_gestisci = countDistinct(df$app_widget_gestisci),
    
    visite_assistenza_e_supporto = countDistinct(df$app_assistenza),
    visite_assistenza_home = countDistinct(df$app_assistenza_home),
    visite_assistenza_contatta = countDistinct(df$app_assistenza_contatta),
    visite_assistenza_conosci = countDistinct(df$app_assistenza_conosci),
    visite_assistenza_ricerca = countDistinct(df$app_assistenza_ricerca),
    visite_assistenza_gestisci = countDistinct(df$app_assistenza_gestisci),
    visite_assistenza_risolvi = countDistinct(df$app_assistenza_risolvi),
    
    visite_extra = countDistinct(df$app_extra),
    visite_arricchisci_abbonamento = countDistinct(df$app_arricchisci_abbonamento),
    visite_stato_abbonamento = countDistinct(df$app_stato_abbonamento),
    visite_contatta_sky = countDistinct(df$app_contatta_sky),
    visite_impostazioni = countDistinct(df$app_impostazioni),
    visite_comunicazioni = countDistinct(df$app_comunicazioni),
    visite_gestisci_servizi = countDistinct(df$app_gestisci_servizi),
    # visite_home = countDistinct(df$app_home),
    # visite_sky_service = countDistinct(df$app_sky_service),
    # visite_intro = countDistinct(df$app_intro),
    # visite_prima_fila = countDistinct(df$app_prima_fila),
    visite_info = countDistinct(df$app_info),
    # visite_Info_DAZN = countDistinct(df$visit_Info_DAZN),
    
    # secondi
    secondi_totali = sum(df$sec_on_page, na.rm = T),
    secondi_fatture = sum(df$sec_app_dati_fatture, na.rm = T),
    
    secondi_widget = sum(df$sec_app_widget, na.rm = T),
    secondi_widget_dispositivi = sum(df$sec_app_widget_dispositivi, na.rm = T),
    secondi_widget_ultima_fattura = sum(df$sec_app_widget_ultima_fattura, na.rm = T),
    secondi_widget_contatta = sum(df$sec_app_widget_contatta, na.rm = T),
    secondi_widget_gestisci = sum(df$sec_app_widget_gestisci, na.rm = T),
    
    secondi_assistenza_e_supporto = sum(df$sec_app_assistenza, na.rm = T),
    secondi_assistenza_home = sum(df$sec_app_assistenza_home, na.rm = T),
    secondi_assistenza_contatta = sum(df$sec_app_assistenza_contatta, na.rm = T),
    secondi_assistenza_conosci = sum(df$sec_app_assistenza_conosci, na.rm = T),
    secondi_assistenza_ricerca = sum(df$sec_app_assistenza_ricerca, na.rm = T),
    secondi_assistenza_gestisci = sum(df$sec_app_assistenza_gestisci, na.rm = T),
    secondi_assistenza_risolvi = sum(df$sec_app_assistenza_risolvi, na.rm = T),
    
    secondi_extra = sum(df$sec_app_extra, na.rm = T),
    secondi_arricchisci_abbonamento = sum(df$sec_app_arricchisci_abbonamento, na.rm = T),
    secondi_stato_abbonamento = sum(df$sec_app_stato_abbonamento, na.rm = T),
    secondi_contatta_sky = sum(df$sec_app_contatta_sky, na.rm = T),
    secondi_impostazioni = sum(df$sec_app_impostazioni, na.rm = T),
    secondi_comunicazioni = sum(df$sec_app_comunicazioni, na.rm = T),
    secondi_gestisci_servizi = sum(df$sec_app_gestisci_servizi, na.rm = T),
    # secondi_home = sum(df$sec_app_home, na.rm = T),
    # secondi_sky_service = sum(df$sec_app_sky_service, na.rm = T),
    # secondi_intro = sum(df$sec_app_intro, na.rm = T),
    # secondi_prima_fila = sum(df$sec_app_prima_fila, na.rm = T),
    secondi_info = sum(df$sec_app_info, na.rm = T)
    # secondi_Info_DAZN = sum(df$sec_visit_Info_DAZN, na.rm = T),
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
    df0 <- withColumn(df0, paste0("visite_contatti", span_time[[i]]), lit(df0[[ paste0("visite_Assitenza_Contatta_sito", span_time[[i]]) ]] + df0[[ paste0("visite_contatta_sky_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("visite_comunicazione", span_time[[i]]), lit(df0[[ paste0("visite_Comunicazione_sito", span_time[[i]]) ]] + df0[[ paste0("visite_comunicazioni_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("visite_assistenza", span_time[[i]]), lit(df0[[ paste0("visite_Assistenza_sito", span_time[[i]]) ]] + df0[[ paste0("visite_assistenza_e_supporto_app", span_time[[i]]) ]]))
    
  }
  
  for (i in 1:length(span_time)){
    
    df0 <- withColumn(df0, paste0("secondi_totali", span_time[[i]]), lit( df0[[ paste0("secondi_totali_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_totali_app", span_time[[i]]) ]] ))
    df0 <- withColumn(df0, paste0("secondi_fatture", span_time[[i]]), lit( df0[[ paste0("secondi_fatture_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_fatture_app", span_time[[i]]) ]]  ))
    df0 <- withColumn(df0, paste0("secondi_extra", span_time[[i]]), lit(df0[[ paste0("secondi_extra_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_extra_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("secondi_arricchisciAbbonamento", span_time[[i]]), lit(df0[[ paste0("secondi_ArricchisciAbbonamento_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_arricchisci_abbonamento_app", span_time[[i]]) ]]))
    df0 <- withColumn(df0, paste0("secondi_gestione", span_time[[i]]), lit(df0[[ paste0("secondi_Gestione_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_gestisci_servizi_app", span_time[[i]]) ]] ))
    df0 <- withColumn(df0, paste0("secondi_contatti", span_time[[i]]), lit(df0[[ paste0("secondi_Assitenza_Contatta_sito", span_time[[i]]) ]] + df0[[ paste0("secondi_contatta_sky_app", span_time[[i]]) ]]))
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





## ALTRE elaborazioni ###########################################################################################################################################

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


denseVectorToArray <- function(dv) {
  SparkR:::callJMethod(dv, "toArray")
}


print_performance <- function(pred, threshold){
  pred <- select(pred, c("COD_CLIENTE_CIFRATO", "DAT_PRIMA_ATTIVAZIONE_dt", "meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4", "prediction"))
  pred <- withColumn(pred, "Pred", lit("TRUE"))
  pred$Pred <- ifelse(pred$prediction > threshold, "TRUE", "FALSE")
  display <- arrange(pred, desc(pred$prediction))
  
  View(head(display,1000))
  
  return(pred)
}

test_cleaning <- function(df0){
  # df0 <- drop(df0, "COD_CLIENTE_CIFRATO")
  # df0 <- drop(df0, "DAT_PRIMA_ATTIVAZIONE_dt")
  df0$meno1M_FASCIA_SDM_PDISC_M1 <- cast(df0$meno1M_FASCIA_SDM_PDISC_M1, "integer")
  df0$meno1M_FASCIA_SDM_PDISC_M4 <- cast(df0$meno1M_FASCIA_SDM_PDISC_M4, "integer")
  df0 <- dropna(df0, how = "any", cols = c("meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4"))
  
  return(df0)
}





            