

## utils estrazione per Pierpaolo


## LIBRERIE
library(magrittr)
library(stringr)

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



#### KPI SITO ###################################################################################################################################################

start_kpi_sito <- function(df, clienti){
  
  df <- withColumn(df, "TARGET_dt", lit("2018-04-30"))
  #df <- withColumn(df, "TARGET_dt", cast(df$TARGET, 'date'))
  
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *, datediff(TARGET_dt, date_time_dt) as diff_gg_view from df")
  
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


#### KPI APP ###################################################################################################################################################

start_kpi_app <- function(df, clienti){
  
  df <- withColumn(df, "TARGET_dt", lit("2018-04-30"))
  #df <- withColumn(df, "TARGET_dt", cast(df$TARGET, 'date'))

  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *, datediff(TARGET_dt, date_time_dt) as diff_gg_view from df")
  
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
            "secondi_PacchettiOfferte_sito_2m",
            "secondi_SearchDisdetta_sito_2m"
            
)
