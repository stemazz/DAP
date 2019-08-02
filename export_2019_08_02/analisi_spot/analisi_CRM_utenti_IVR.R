
## Analisi richiesta da CRM su utenti IVR


source("connection_R.R")
options(scipen = 10000)


path_navigazione_sito <- "/user/stefano.mazzucca/CRM_info_utenti_IVR/nav_sito.parquet"
path_navigazione_app <- "/user/stefano.mazzucca/CRM_info_utenti_IVR/nav_app.parquet"

path_kpi_sito <- "/user/stefano.mazzucca/CRM_info_utenti_IVR/kpi_sito.parquet"
path_kpi_app <- "/user/stefano.mazzucca/CRM_info_utenti_IVR/kpi_app.parquet"


path_kpi_sito_bis <- "/user/stefano.mazzucca/CRM_info_utenti_IVR/kpi_sito_with_date.parquet"
path_kpi_app_bis <- "/user/stefano.mazzucca/CRM_info_utenti_IVR/kpi_app_with_date.parquet"



## Navigazioni sito
skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev/table=hitdata/hitdata.parquet")
skyit_1 <- select(skyitdev, "external_id_post_evar",
                  "post_visid_high",
                  "post_visid_low",
                  "post_visid_concatenated",
                  "visit_num",
                  "date_time",
                  "post_channel", 
                  "page_name_post_evar",
                  "page_url_post_evar",
                  "secondo_livello_post_prop",
                  "terzo_livello_post_prop",
                  "hit_source",
                  "exclude_hit",
                  "post_page_event_value"
)
skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))
skyit_4 <- filter(skyit_3, "post_channel = 'corporate' or post_channel = 'Guidatv'")

# filtro temporale
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt >= as.Date("2018-12-29")) 
skyit_6 <- filter(skyit_5, skyit_5$date_time_dt <= as.Date("2019-03-29"))


write.parquet(skyit_6, path_navigazione_sito)



## Naviagazioni APP
skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod/table=hitdata/hitdata.parquet")
skyappwsc_1 <- select(skyappwsc, "external_id_post_evar",
                      "post_visid_high",
                      "post_visid_low",
                      "post_visid_concatenated",
                      "visit_num",
                      "date_time",
                      "post_channel", 
                      "page_name_post_evar",
                      "page_name_post_prop", 
                      "page_url_post_prop",
                      "site_section",
                      "hit_source",
                      "exclude_hit",
                      "post_page_event_value"
)
skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_ts", cast(skyappwsc_2$date_time, "timestamp"))
skyappwsc_4 <- filter(skyappwsc_3, "external_id_post_evar is not NULL")

# filtro temporale
skyappwsc_5 <- filter(skyappwsc_4, skyappwsc_4$date_time_dt >= as.Date("2018-12-29")) 
skyappwsc_6 <- filter(skyappwsc_5, skyappwsc_5$date_time_dt <= as.Date("2019-03-29"))


write.parquet(skyappwsc_6, path_navigazione_app)



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
            
            CASE WHEN terzo_livello_post_prop LIKE 'info disdetta' THEN visit_uniq ELSE Null END AS visit_InfoDisdetta,
            
            CASE WHEN (terzo_livello_post_prop LIKE 'trasloca sky' OR 
            terzo_livello_post_prop LIKE 'trasparenza tariffaria' OR
            terzo_livello_post_prop LIKE 'trova sky service' OR
            page_url_post_evar LIKE '%query=disdett%' OR page_url_post_evar LIKE '%query=recess%' OR
            terzo_livello_post_prop LIKE 'sky expert') THEN visit_uniq ELSE Null END AS visit_Trigger

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
    
    visite_InfoDisdetta = countDistinct(df$visit_InfoDisdetta),
    
    visite_Trigger = countDistinct(df$visit_Trigger)
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
            
            CASE WHEN (site_section LIKE 'stato abbonamento' OR site_section LIKE 'gestione servizi' OR site_section LIKE 'i miei dati') THEN visit_num ELSE Null END AS visit_stato_abbonamento

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

    visite_stato_abbonamento = countDistinct(df$visit_stato_abbonamento)  
  )
  return(df_kpi)
}



## Elaborazioni richieste sui dati

nav_sito <- read.parquet(path_navigazione_sito)
View(head(nav_sito,100))
nrow(nav_sito)
# 59.674.605
nav_app <- read.parquet(path_navigazione_app)
nrow(nav_app)
# 25.947.622


kpi_sito <- group_kpi_sito(nav_sito)
write.parquet(kpi_sito, path_kpi_sito)

kpi_app <- group_kpi_app(nav_app)
write.parquet(kpi_app, path_kpi_app)


##### Prendi i filtri ##################################################################################

kpi_sito <- read.parquet(path_kpi_sito)
View(head(kpi_sito,100))
nrow(kpi_sito)
# 1.620.521

kpi_app <- read.parquet(path_kpi_app)
View(head(kpi_app,100))
nrow(kpi_app)
# 741.392


## Clienti che hanno visualizzato la pagina web di disdetta https://www.sky.it/assistenza/info-disdetta/sky.html

info_disdetta <- summarize(groupBy(kpi_sito, kpi_sito$external_id_post_evar), 
                           count_info_disdetta = max(kpi_sito$visite_InfoDisdetta))
info_disdetta <- arrange(info_disdetta, desc(info_disdetta$count_info_disdetta))
View(head(info_disdetta,100))

info_disdetta <- filter(info_disdetta, info_disdetta$external_id_post_evar != "null" &
                          info_disdetta$external_id_post_evar != "n-a" &
                          isNotNull(info_disdetta$external_id_post_evar))
View(head(info_disdetta,100))
nrow(info_disdetta)
# 1.620.518

count_ext_id_info_disdetta <- filter(info_disdetta, info_disdetta$count_info_disdetta != 0)
View(head(count_ext_id_info_disdetta,100))
nrow(count_ext_id_info_disdetta)
# 79.833


# createOrReplaceTempView(info_disdetta, "info_disdetta")
# quartili <- sql("select percentile_approx(count_info_disdetta, 0.25) as q1,
#                         percentile_approx(count_info_disdetta, 0.50) as q2, -- q2 = mediana
#                         percentile_approx(count_info_disdetta, 0.75) as q3,
#                         percentile_approx(count_info_disdetta, 1) as q4,
#                         percentile_approx(count_info_disdetta, 0.99) as 99percentile
#                 from info_disdetta 
#                 ")
# View(head(quartili,100)) 


## Clienti che hanno visualizzato la sezione fattura e pagamenti su WSC 

wsc_fatt <- summarize(groupBy(kpi_sito, kpi_sito$external_id_post_evar), 
                           count_wsc_fatt = max(kpi_sito$visite_Fatture))
wsc_fatt <- arrange(wsc_fatt, desc(wsc_fatt$count_wsc_fatt))
View(head(wsc_fatt,100))

wsc_fatt <- filter(wsc_fatt, wsc_fatt$external_id_post_evar != "null" &
                     wsc_fatt$external_id_post_evar != "n-a" &
                     isNotNull(wsc_fatt$external_id_post_evar))
View(head(wsc_fatt,100))
nrow(wsc_fatt)
# 1.620.518

count_ext_id_wsc_fatt <- filter(wsc_fatt, wsc_fatt$count_wsc_fatt != 0)
View(head(count_ext_id_wsc_fatt,100))
nrow(count_ext_id_wsc_fatt)
# 412.369


## Clienti che hanno visualizzato la sezione pagamenti (vedi le fatture) sull???APP ???Fai da te???

asc_fatt <- summarize(groupBy(kpi_app, kpi_app$external_id_post_evar), 
                      count_asc_fatt = max(kpi_app$visite_fatture))
asc_fatt <- arrange(asc_fatt, desc(asc_fatt$count_asc_fatt))
View(head(asc_fatt,100))

asc_fatt <- filter(asc_fatt, asc_fatt$external_id_post_evar != "null" &
                     asc_fatt$external_id_post_evar != "n-a" &
                     isNotNull(asc_fatt$external_id_post_evar))
View(head(asc_fatt,100))
nrow(asc_fatt)
# 741.390

count_ext_id_asc_fatt <- filter(asc_fatt, asc_fatt$count_asc_fatt != 0)
View(head(count_ext_id_asc_fatt,100))
nrow(count_ext_id_asc_fatt)
# 390.712


createOrReplaceTempView(count_ext_id_info_disdetta, "count_ext_id_info_disdetta")
createOrReplaceTempView(count_ext_id_wsc_fatt, "count_ext_id_wsc_fatt")
createOrReplaceTempView(count_ext_id_asc_fatt, "count_ext_id_asc_fatt")

join_CRM <- sql("select distinct t1.external_id_post_evar, t2.external_id_post_evar as ext_id_2,
                                t1.count_info_disdetta, t2.count_wsc_fatt
                from count_ext_id_info_disdetta t1
                full outer join count_ext_id_wsc_fatt t2
                on t1.external_id_post_evar = t2.external_id_post_evar")
View(head(join_CRM,100))
nrow(join_CRM)
# 454.705

# join_CRM_2 <- join(count_ext_id_info_disdetta, count_ext_id_wsc_fatt, 
#                    count_ext_id_info_disdetta$external_id_post_evar == count_ext_id_wsc_fatt$external_id_post_evar, 
#                    joinType = "outer")
# View(head(join_CRM_2,100))
# nrow(join_CRM_2)
# # 454.705

join_CRM$external_id_supp <- ifelse(isNull(join_CRM$external_id_post_evar), join_CRM$ext_id_2, 
                                    join_CRM$external_id_post_evar)
join_CRM$external_id_post_evar <- NULL
join_CRM$ext_id_2 <- NULL
View(head(join_CRM,100))
nrow(join_CRM)
# 454.705


createOrReplaceTempView(join_CRM, "join_CRM")
join_CRM_2 <- sql("select distinct t1.external_id_supp, t2.external_id_post_evar,
                                t1.count_info_disdetta, t1.count_wsc_fatt, t2.count_asc_fatt
                from join_CRM t1
                full outer join count_ext_id_asc_fatt t2
                on t1.external_id_supp = t2.external_id_post_evar")
View(head(join_CRM_2,100))
nrow(join_CRM_2)
# 771.391

join_CRM_2$external_id <- ifelse(isNull(join_CRM_2$external_id_supp), join_CRM_2$external_id_post_evar, 
                                 join_CRM_2$external_id_supp)
join_CRM_2$external_id_supp <- NULL
join_CRM_2$external_id_post_evar <- NULL
View(head(join_CRM_2,100))
nrow(join_CRM_2)
# 771.391

join_CRM_2 <- select(join_CRM_2, "external_id", "count_info_disdetta", "count_wsc_fatt", "count_asc_fatt")



write.df(repartition( join_CRM_2, 1), path = "/user/stefano.mazzucca/CRM_info_utenti_IVR/richiesta_CRM.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





#### OPPURE ###########################################################################################

kpi_sito_2 <- add_kpi_sito(nav_sito)
kpi_sito_2 <- filter(kpi_sito_2, isNotNull(kpi_sito_2$external_id_post_evar))
# View(head(kpi_sito_2,100))

kpi_sito_3 <- agg(
  groupBy(kpi_sito_2, "date_time_dt", "external_id_post_evar"),
  # external_id_post_evar = first(kpi_sito_2$external_id_post_evar),
  visite_Fatture = countDistinct(kpi_sito_2$visit_Fatture),
  visite_InfoDisdetta = countDistinct(kpi_sito_2$visit_InfoDisdetta)
)
kpi_sito_3 <- arrange(kpi_sito_3, kpi_sito_3$external_id_post_evar, asc(kpi_sito_3$date_time_dt))
kpi_sito_4 <- filter(kpi_sito_3, kpi_sito_3$visite_Fatture != 0 | kpi_sito_3$visite_InfoDisdetta != 0)
kpi_sito_4 <- select(kpi_sito_4, kpi_sito_4$external_id_post_evar, kpi_sito_4$visite_Fatture, kpi_sito_4$visite_InfoDisdetta, 
                     kpi_sito_4$date_time_dt)
# View(head(kpi_sito_4,100))

write.parquet(kpi_sito_4, path_kpi_sito_bis)



kpi_app_2 <- add_kpi_app(nav_app)
kpi_app_2 <- filter(kpi_app_2, isNotNull(kpi_app_2$external_id_post_evar))
View(head(kpi_app_2,100))

kpi_app_3 <- agg(
  groupBy(kpi_app_2, "date_time_dt", "external_id_post_evar"),
  # external_id_post_evar = first(kpi_app_2$external_id_post_evar),
  visite_Fatture_app = countDistinct(kpi_app_2$visit_fatture)
)
kpi_app_3 <- arrange(kpi_app_3, kpi_app_3$external_id_post_evar, asc(kpi_app_3$date_time_dt))
kpi_app_4 <- filter(kpi_app_3, kpi_app_3$visite_Fatture_app != 0)
kpi_app_4 <- select(kpi_app_4, kpi_app_4$external_id_post_evar, kpi_app_4$visite_Fatture_app, kpi_app_4$date_time_dt)
# View(head(kpi_app_4,100))

write.parquet(kpi_app_4, path_kpi_app_bis)



kpi_sito <- read.parquet(path_kpi_sito_bis)
View(head(kpi_sito,100))
nrow(kpi_sito)
# 846.543
kpi_sito <- withColumn(kpi_sito, "visite_Fatture_app", lit(NULL))

kpi_app <- read.parquet(path_kpi_app_bis)
View(head(kpi_app,100))
nrow(kpi_app)
# 1.175.940
kpi_app <- withColumn(kpi_app, "visite_Fatture", lit(NULL))
kpi_app <- withColumn(kpi_app, "visite_InfoDisdetta", lit(NULL))

kpi_sito <- select(kpi_sito, "external_id_post_evar", "visite_Fatture", "visite_InfoDisdetta", "visite_Fatture_app", "date_time_dt")
kpi_app <- select(kpi_app, "external_id_post_evar", "visite_Fatture", "visite_InfoDisdetta", "visite_Fatture_app", "date_time_dt")

kpi_tot <- union(kpi_sito, kpi_app)
kpi_tot <- arrange(kpi_tot, kpi_tot$external_id_post_evar, asc(kpi_tot$date_time_dt))

kpi_tot <- withColumnRenamed(kpi_tot, "visite_InfoDisdetta", "count_info_disdetta")
kpi_tot <- withColumnRenamed(kpi_tot, "visite_Fatture", "count_wsc_fatt")
kpi_tot <- withColumnRenamed(kpi_tot, "visite_Fatture_app", "count_asc_fatt")

kpi_tot <- fillna(kpi_tot, 0)

View(head(kpi_tot,100))

write.df(repartition( kpi_tot, 1), path = "/user/stefano.mazzucca/CRM_info_utenti_IVR/richiesta_CRM_with_date.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



#chiudi sessione
sparkR.stop()
