
## Ananlisi churn DTT 2 (fosuc su skysport)


source("connection_R.R")
options(scipen = 10000)



path_tier2 <- "/user/stefano.mazzucca/churn_dtt/14NOV18_basegeo.csv"
path_dtt <- "/user/stefano.mazzucca/churn_dtt/14NOV18_BASEDTT.csv"


base_tier2 <- read.df(path_tier2, source = "csv", header = "true", delimiter = ";")
base_tier2 <- withColumn(base_tier2, "dat_cessazione_promo_dt", cast(cast(unix_timestamp(base_tier2$DAT_CESSAZIONE_PROMO, 'dd/MM/yyyy HH:mm'), 'timestamp'),'date'))
View(head(base_tier2,100))
nrow(base_tier2)
# 19.814

# filt_active <- filter(base_tier2, base_tier2$DES_STATO_BUSINESS == 'Active')
# nrow(filt_active)
# # 10.067



base_dtt <- read.df(path_dtt, source = "csv", header = "true", delimiter = ";")
base_dtt <- filter(base_dtt, isNotNull(base_dtt$COD_CLIENTE_CIFRATO))
# base_dtt <- filter(base_dtt, base_dtt$DES_STATO_BUSINESS != 'Disconnected')
View(head(base_dtt,100))
nrow(base_dtt)
# 453.367  # 430.533


################################################################################################################################################################
## DTT ##
################################################################################################################################################################

path_mappatura_cookie_skyid <- "/user/stefano.mazzucca/churn_dtt/dizionario_dtt.parquet"
path_navigazione_sito <- "/user/stefano.mazzucca/churn_dtt/nav_sito_sport_dtt.parquet"
path_navigazione_app <- "/user/stefano.mazzucca/churn_dtt/nav_app_sport_dtt.parquet"

path_kpi_sito <- "/user/stefano.mazzucca/churn_dtt/kpi_sito_sport_dtt.parquet"
path_kpi_app <- "/user/stefano.mazzucca/churn_dtt/kpi_app_sport_dtt.parquet"
path_kpi_finale <- "/user/stefano.mazzucca/churn_dtt/kpi_finale_sport_dtt.parquet"

data <- as.Date("2018-11-13")
gg_analizzati <- as.integer(90)


full <- base_dtt


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
skyit_4 <- filter(skyit_3, "post_channel = 'sport'")

# filtro temporale
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt >= (data - gg_analizzati)) 
skyit_6 <- filter(skyit_5, skyit_5$date_time_dt <= data)


diz <- read.parquet(path_mappatura_cookie_skyid)


## join tra full_CB e chiavi_cookie
createOrReplaceTempView(full, "full")
createOrReplaceTempView(diz, "diz")

full_coo <- sql("select distinct t1.*, t2.post_visid_concatenated
                from full t1
                inner join diz t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

## join tra full e navigazioni (skyit_6)
createOrReplaceTempView(full_coo,"full_coo")
createOrReplaceTempView(skyit_6,"skyit_6")

nav_sito <- sql("SELECT DISTINCT t1.*, 
                t2.visit_num, t2.post_channel,
                t2.date_time, t2.date_time_dt, t2.date_time_ts,
                t2.page_name_post_evar, t2.page_url_post_evar, t2.secondo_livello_post_prop, t2.terzo_livello_post_prop,
                t2.hit_source, t2.exclude_hit
                FROM full_coo t1
                INNER JOIN skyit_6 t2
                ON t1.post_visid_concatenated = t2.post_visid_concatenated")

write.parquet(nav_sito, path_navigazione_sito)


## Navigazioni app
skyappsport <- read.parquet("/STAGE/adobe/reportsuite=skyappsport.prod")
View(head(skyappsport_1,100))

skyappsport_1 <- select(skyappsport, "external_id_post_evar",
                      "post_visid_high",
                      "post_visid_low",
                      "post_visid_concatenated",
                      "visit_num",
                      "date_time",
                      "post_channel", 
                      "page_name_post_evar",
                      "post_page_url",
                      "site_section_post_evar",
                      "site_subsection_post_evar",
                      "hit_source",
                      "exclude_hit",
                      "post_page_event_value"
)
skyappsport_2 <- withColumn(skyappsport_1, "date_time_dt", cast(skyappsport_1$date_time, "date"))
skyappsport_3 <- withColumn(skyappsport_2, "date_time_ts", cast(skyappsport_2$date_time, "timestamp"))
skyappsport_4 <- filter(skyappsport_3, "external_id_post_evar is not NULL")

# filtro temporale
skyappsport_5 <- filter(skyappsport_4, skyappsport_4$date_time_dt >= (data - gg_analizzati)) 
skyappsport_6 <- filter(skyappsport_5, skyappsport_5$date_time_dt <= data)

## join tra full e navigazioni (skyappsport_6)
createOrReplaceTempView(full, "full")
createOrReplaceTempView(skyappsport_6, "skyappsport_6")

nav_app_sport <- sql("SELECT DISTINCT t1.*, 
                   t2.visit_num, t2.post_channel,
                   t2.date_time, t2.date_time_dt, t2.date_time_ts,
                   t2.page_name_post_evar, t2.post_page_url, t2.site_section_post_evar, t2.site_subsection_post_evar,
                   t2.hit_source, t2.exclude_hit, t2.post_page_event_value
                   FROM full t1
                   INNER JOIN skyappsport_6 t2
                   ON t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(nav_app_sport, path_navigazione_app, mode = "overwrite")





nav_sito_sport <- read.parquet(path_navigazione_sito)
View(head(nav_sito_sport,100))
nrow(nav_sito_sport)
# 1.274.670

# createOrReplaceTempView(nav_sito_sport, "nav_sito_sport")
# valdist_2_livello <- sql("select secondo_livello_post_prop, count(*) as count
#                          from nav_sito_sport
#                          group by secondo_livello_post_prop")
# valdist_2_livello <- arrange(valdist_2_livello, desc(valdist_2_livello$count))
# View(head(valdist_2_livello,100))
# 
# valdist_3_livello <- sql("select terzo_livello_post_prop, count(*) as count
#                          from nav_sito_sport
#                          group by terzo_livello_post_prop")
# valdist_3_livello <- arrange(valdist_3_livello, desc(valdist_3_livello$count))
# View(head(valdist_3_livello,100))


nav_app_sport <- read.parquet(path_navigazione_app)
View(head(nav_app_sport,100))
nrow(nav_app_sport)
# 132.855

# createOrReplaceTempView(nav_app_sport, "nav_app_sport")
# valdist_site_section <- sql("select site_section_post_evar, count(*) as count
#                          from nav_app_sport
#                          group by site_section_post_evar")
# valdist_site_section <- arrange(valdist_site_section, desc(valdist_site_section$count))
# View(head(valdist_site_section,100))
# 
# valdist_site_subsection <- sql("select site_subsection_post_evar, count(*) as count
#                          from nav_app_sport
#                          group by site_subsection_post_evar")
# valdist_site_subsection <- arrange(valdist_site_subsection, desc(valdist_site_subsection$count))
# View(head(valdist_site_subsection,100))


source("analisi_spot/churn_DTT_Utils.R")

nav_sito_sport <- filter(nav_sito_sport, nav_sito_sport$date_time_dt >= (data - gg_analizzati))
nav_sito_sport <- filter(nav_sito_sport, nav_sito_sport$date_time_dt <= data)

nav_sito_sport <- remove_too_short(nav_sito_sport, 10)

df_kpi_sito_sport <- group_kpi_sito(nav_sito_sport)


write.parquet(df_kpi_sito_sport, path_kpi_sito)

# View(head(df_kpi_sito_sport,100))



nav_app_sport <- filter(nav_app_sport, nav_app_sport$date_time_dt >= (data - gg_analizzati))
nav_app_sport <- filter(nav_app_sport, nav_app_sport$date_time_dt <= data)

nav_app_sport <- remove_too_short(nav_app_sport, 4)

df_kpi_app_sport <- group_kpi_app(nav_app_sport)


write.parquet(df_kpi_app_sport, path_kpi_app)

# View(head(df_kpi_app_sport,100))



df_kpi_sito_sport <- read.parquet(path_kpi_sito)
df_kpi_app_sport <- read.parquet(path_kpi_app)

df_kpi <- join_COD_CLIENTE_CIFRATO(df_kpi_sito_sport, df_kpi_app_sport) 


write.parquet(df_kpi, path_kpi_finale)


df_kpi_dtt_sport <- read.parquet(path_kpi_finale)
View(head(df_kpi_dtt_sport,100))
nrow(df_kpi_dtt_sport)
# 78.985

write.df(repartition( df_kpi_dtt_sport, 1), path = "/user/stefano.mazzucca/churn_dtt/df_kpi_dtt_sport.csv", "csv", sep=";", mode = "overwrite", header=TRUE)




################################################################################################################################################################
## Tier2 ##
################################################################################################################################################################

path_mappatura_cookie_skyid <- "/user/stefano.mazzucca/churn_dtt/dizionario_tier2.parquet"
path_navigazione_sito <- "/user/stefano.mazzucca/churn_dtt/nav_sito_sport_tier2.parquet"
path_navigazione_app <- "/user/stefano.mazzucca/churn_dtt/nav_app_sport_tier2.parquet"

path_kpi_sito <- "/user/stefano.mazzucca/churn_dtt/kpi_sito_sport_tier2.parquet"
path_kpi_app <- "/user/stefano.mazzucca/churn_dtt/kpi_app_sport_tier2.parquet"
path_kpi_finale <- "/user/stefano.mazzucca/churn_dtt/kpi_finale_sport_tier2.parquet"

gg_analizzati <- as.integer(90)




full <- base_tier2


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
skyit_4 <- filter(skyit_3, "post_channel = 'sport'")


diz <- read.parquet(path_mappatura_cookie_skyid)


## join tra full_CB e chiavi_cookie
createOrReplaceTempView(full, "full")
createOrReplaceTempView(diz, "diz")

full_coo <- sql("select distinct t1.*, t2.post_visid_concatenated
                from full t1
                inner join diz t2
                on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")

## join tra full e navigazioni (skyit_6)
createOrReplaceTempView(full_coo,"full_coo")
createOrReplaceTempView(skyit_6,"skyit_6")

nav_sito <- sql("SELECT DISTINCT t1.*, 
                t2.visit_num, t2.post_channel,
                t2.date_time, t2.date_time_dt, t2.date_time_ts,
                t2.page_name_post_evar, t2.page_url_post_evar, t2.secondo_livello_post_prop, t2.terzo_livello_post_prop,
                t2.hit_source, t2.exclude_hit
                FROM full_coo t1
                INNER JOIN skyit_6 t2
                ON t1.post_visid_concatenated = t2.post_visid_concatenated")

write.parquet(nav_sito, path_navigazione_sito)



## Navigazioni app
skyappsport <- read.parquet("/STAGE/adobe/reportsuite=skyappsport.prod")
View(head(skyappsport_1,100))

skyappsport_1 <- select(skyappsport, "external_id_post_evar",
                        "post_visid_high",
                        "post_visid_low",
                        "post_visid_concatenated",
                        "visit_num",
                        "date_time",
                        "post_channel", 
                        "page_name_post_evar",
                        "post_page_url",
                        "site_section_post_evar",
                        "site_subsection_post_evar",
                        "hit_source",
                        "exclude_hit",
                        "post_page_event_value"
)
skyappsport_2 <- withColumn(skyappsport_1, "date_time_dt", cast(skyappsport_1$date_time, "date"))
skyappsport_3 <- withColumn(skyappsport_2, "date_time_ts", cast(skyappsport_2$date_time, "timestamp"))
skyappsport_4 <- filter(skyappsport_3, "external_id_post_evar is not NULL")

## join tra full e navigazioni (skyappsport_6)
createOrReplaceTempView(full, "full")
createOrReplaceTempView(skyappsport_6, "skyappsport_6")

nav_app_sport <- sql("SELECT DISTINCT t1.*, 
                   t2.visit_num, t2.post_channel,
                   t2.date_time, t2.date_time_dt, t2.date_time_ts,
                   t2.page_name_post_evar, t2.post_page_url, t2.site_section_post_evar, t2.site_subsection_post_evar,
                   t2.hit_source, t2.exclude_hit, t2.post_page_event_value
                   FROM full t1
                   INNER JOIN skyappsport_6 t2
                   ON t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")

write.parquet(nav_app_sport, path_navigazione_app, mode = "overwrite")




nav_sito_sport <- read.parquet(path_navigazione_sito)
View(head(nav_sito_sport,100))
nrow(nav_sito_sport)
# 59.640

# createOrReplaceTempView(nav_sito_sport, "nav_sito_sport")
# valdist_2_livello <- sql("select secondo_livello_post_prop, count(*) as count
#                          from nav_sito_sport
#                          group by secondo_livello_post_prop")
# valdist_2_livello <- arrange(valdist_2_livello, desc(valdist_2_livello$count))
# View(head(valdist_2_livello,100))
# 
# valdist_3_livello <- sql("select terzo_livello_post_prop, count(*) as count
#                          from nav_sito_sport
#                          group by terzo_livello_post_prop")
# valdist_3_livello <- arrange(valdist_3_livello, desc(valdist_3_livello$count))
# View(head(valdist_3_livello,100))



nav_app_sport <- read.parquet(path_navigazione_app)
View(head(nav_app_sport,100))
nrow(nav_app_sport)
# 19.279

# createOrReplaceTempView(nav_app_sport, "nav_app_sport")
# valdist_site_section <- sql("select site_section_post_evar, count(*) as count
#                          from nav_app_sport
#                          group by site_section_post_evar")
# valdist_site_section <- arrange(valdist_site_section, desc(valdist_site_section$count))
# View(head(valdist_site_section,100))
# 
# valdist_site_subsection <- sql("select site_subsection_post_evar, count(*) as count
#                          from nav_app_sport
#                          group by site_subsection_post_evar")
# valdist_site_subsection <- arrange(valdist_site_subsection, desc(valdist_site_subsection$count))
# View(head(valdist_site_subsection,100))


source("analisi_spot/churn_DTT_Utils.R")

nav_sito_sport <- filter(nav_sito_sport, nav_sito_sport$date_time_dt <= nav_sito_sport$dat_cessazione_promo_dt)
nav_sito_sport <- filter(nav_sito_sport, nav_sito_sport$date_time_dt >= (date_sub(nav_sito_sport$dat_cessazione_promo_dt, gg_analizzati))) 

nav_sito_sport <- remove_too_short(nav_sito_sport, 10)

df_kpi_sito_sport <- group_kpi_sito(nav_sito_sport)


write.parquet(df_kpi_sito_sport, path_kpi_sito)

# View(head(df_kpi_sito_sport,100))



nav_app_sport <- filter(nav_app_sport, nav_app_sport$date_time_dt <= nav_app_sport$dat_cessazione_promo_dt)
nav_app_sport <- filter(nav_app_sport, nav_app_sport$date_time_dt >= (date_sub(nav_app_sport$dat_cessazione_promo_dt, gg_analizzati)))

nav_app_sport <- remove_too_short(nav_app_sport, 4)

df_kpi_app_sport <- group_kpi_app(nav_app_sport)


write.parquet(df_kpi_app_sport, path_kpi_app)

# View(head(df_kpi_app_sport,100))



df_kpi_sito_sport <- read.parquet(path_kpi_sito)
df_kpi_app_sport <- read.parquet(path_kpi_app)

df_kpi <- join_COD_CLIENTE_CIFRATO(df_kpi_sito_sport, df_kpi_app_sport) 


write.parquet(df_kpi, path_kpi_finale)


df_kpi_tier2_sport <- read.parquet(path_kpi_finale)
View(head(df_kpi_tier2_sport,100))
nrow(df_kpi_tier2_sport)
# 2.530

write.df(repartition( df_kpi_tier2_sport, 1), path = "/user/stefano.mazzucca/churn_dtt/df_kpi_tier2_sport.csv", "csv", sep=";", mode = "overwrite", header=TRUE)









#chiudi sessione
sparkR.stop()
