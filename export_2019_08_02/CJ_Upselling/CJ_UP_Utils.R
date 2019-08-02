

## CJ_UP_Utils.R


## Funzione per rimuovere hit minori ai 10 secondi ###########################################################################################################

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



## Funzione per mappare sezioni sito ############################################################################################################################

add_kpi_sito <- function(df) {
  
  # Mappare le sezioni
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,
            CASE WHEN terzo_livello_prop LIKE 'faidate fatture pagamenti' THEN 'wsc_fatture'
              WHEN terzo_livello_prop LIKE 'faidate extra' THEN 'wsc_extra'
              WHEN terzo_livello_prop LIKE 'faidate arricchisci abbonamento' THEN 'wsc_arricchisciabbonamento'
              WHEN terzo_livello_prop LIKE 'faidate gestisci dati servizi' THEN 'wsc_gestisci'
              WHEN terzo_livello_prop LIKE 'faidate webtracking' THEN 'wsc_webtracking'
              WHEN terzo_livello_prop LIKE 'faidate home' THEN 'wsc_home'
              WHEN secondo_livello_prop LIKE 'assistenza' -- AND 
                          -- (terzo_livello_prop LIKE 'contatta' OR 
                          -- terzo_livello_prop LIKE 'conosci' OR 
                          -- terzo_livello_prop LIKE 'home' OR
                          -- terzo_livello_prop LIKE 'gestisci' OR 
                          -- terzo_livello_prop LIKE 'ricerca' OR 
                          -- terzo_livello_prop LIKE 'risolvi' OR
                          -- terzo_livello_prop LIKE 'trova sky service' OR
                          -- terzo_livello_prop LIKE 'trasloca sky' OR
                          -- terzo_livello_prop LIKE 'info disdetta') 
                          THEN 'sito_assistenza'
              WHEN (secondo_livello_prop LIKE '%guidatv%' OR secondo_livello_prop LIKE '%Guidatv%') THEN 'sito_guidatv'
              WHEN secondo_livello_prop LIKE 'extra' THEN 'sito_extra'
              WHEN secondo_livello_prop LIKE 'tecnologia' THEN 'sito_tecnologia'
              WHEN secondo_livello_prop LIKE 'pacchetti-offerte' THEN 'sito_pacchettieofferte'
              WHEN page_url_pulita_post_prop LIKE '%sky.it/acquista%' or page_url_pulita_post_prop LIKE '%abbonamento.sky.it/aol%' THEN 'sito_acquista'
              WHEN secondo_livello_prop LIKE 'landing' THEN 'sito_landing'
            ELSE Null END AS evento_digital
            FROM df"
  )
  
  return(df)
}



## Funzione per mappare sezioni app ############################################################################################################################

add_kpi_app <- function(df) {
  
  # mappare le sezioni
  createOrReplaceTempView(df, "df")
  df <- sql("SELECT *,
            CASE WHEN (site_section_prop LIKE 'fatture' OR site_section_prop LIKE 'dati e fatture') THEN 'wsc_fatture'
              WHEN site_section_prop LIKE 'extra' THEN 'wsc_extra'
              WHEN site_section_prop LIKE 'arricchisci abbonamento' THEN 'wsc_arricchisciabbonamento'
              WHEN (site_section_prop LIKE 'stato abbonamento' OR site_section_prop LIKE '%servizi%' OR site_section_prop LIKE 'i miei dati') THEN 'wsc_gestisci'
              WHEN site_section_prop LIKE 'assistenza' THEN 'wsc_assistenza'
              WHEN site_section_prop LIKE 'widget' THEN 'wsc_widget'
              WHEN (site_section_prop LIKE 'home' OR site_section_prop LIKE 'il mio abbonamento' OR site_section_prop LIKE 'info') THEN 'wsc_home'
              WHEN (site_section_prop LIKE 'contatta sky' OR site_section_prop LIKE 'comunicazioni') THEN 'wsc_comunicazioni'
            ELSE Null END AS evento_digital
            FROM df"
  )
  
  return(df)
}


## Funzione per join veloce ###################################################################################################################################

join_COD_CLIENTE_CIFRATO <- function(df1, df2){
  df2 <- withColumnRenamed(df2, "COD_CLIENTE_CIFRATO", "COD_CLIENTE_CIFRATO_y")
  df1 <- merge(df1, df2, all.x = T, by.x = "COD_CLIENTE_CIFRATO", by.y = "COD_CLIENTE_CIFRATO_y")
  df1 <- drop(df1, "COD_CLIENTE_CIFRATO_y")
  return(df1)
}


