#Script per digitalizzazione

source("connection_R.R")
options(scipen = 10000)

# Scarico Corporate da SKYITDEV:



data_inizio <- as.Date("2018-01-01") # data inizio perimetro
data_fine <- as.Date("2018-09-30") # data fine perimetro

path_scarico_skyitdev <- '/user/matteo.ballerinipuviani/Digitalizzazione_scarico_skyitdev_gen_set_2018.parquet' # inserire qua il path scarico 
path_scarico_skyappwsc <- '/user/matteo.ballerinipuviani/Digitalizzazione_scarico_appwsc_gen_set_2018.parquet'
path_scarico_skyappgtv <- '/user/matteo.ballerinipuviani/Digitalizzazione_scarico_appgtv_gen_set_2018.parquet'
path_scarico_skyappsport <- '/user/matteo.ballerinipuviani/Digitalizzazione_scarico_appsport_gen_set_2018.parquet'
path_scarico_skyappxfactor <- '/user/matteo.ballerinipuviani/Digitalizzazione_scarico_appxfactor_gen_set_2018.parquet'



path_scarico_corporate <- '/user/matteo.ballerinipuviani/Digitalizzazione_scarico_corporate_guidatv_gen_set_2018.parquet' # inserire qua il path scarico corporate

path_scarico_tg24 <- '/user/matteo.ballerinipuviani/Digitalizzazione_scarico_tg24_gen_set_2018.parquet'

path_scarico_sport <- '/user/matteo.ballerinipuviani/Digitalizzazione_scarico_sport_gen_set_2018.parquet'




path_diz_appgtv <- "/user/matteo.ballerinipuviani/Dizionario_scarico_appgtv_gen_set_2018.parquet"
path_diz_appsport <- "/user/matteo.ballerinipuviani/Dizionario_scarico_appsport_gen_set_2018.parquet"
path_diz_appxfactor <- "/user/matteo.ballerinipuviani/Dizionario_scarico_appxfactor_gen_set_2018.parquet"




path_skyitdev <- '/STAGE/adobe/reportsuite=skyitdev' # reportsuite skyitdev

skyitdev <- read.parquet(path_skyitdev) # read reportsuite

# variabili utilizzate per lo scarico

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
                  "post_page_event_value",
                  "post_mobiledevice",
                  "post_campaign",
                  "va_closer_id", 
                  "mapped_connection_type_value",
                  "mapped_browser_type_value",
                  "mapped_country_value",
                  "mapped_os_value",
                  "mapped_resolution_value",
                  "mapped_browser_value"
)

skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date")) # cast date_time in formato date
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))  # cast date_time in formato timestamp

# filtro temporale

skyit_4 <- filter(skyit_3, skyit_3$date_time_dt >= data_inizio) 
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt <= data_fine)

# traduzione last touch label con dizionario 

skyit_5$last_touch_label <- ifelse(skyit_5$va_closer_id == 1, "editorial_recommendation",
                                   ifelse(skyit_5$va_closer_id == 2, "search_engine_organic",
                                          ifelse(skyit_5$va_closer_id == 3, "internal_referrer_w",
                                                 ifelse(skyit_5$va_closer_id == 4, "social_network",
                                                        ifelse(skyit_5$va_closer_id == 5, "direct_bookmark",
                                                               ifelse(skyit_5$va_closer_id == 6, "referring_domains",
                                                                      ifelse(skyit_5$va_closer_id == 7, "paid_display",
                                                                             ifelse(skyit_5$va_closer_id == 8, "paid_search",
                                                                                    ifelse(skyit_5$va_closer_id == 9, "paid_other",
                                                                                           ifelse(skyit_5$va_closer_id == 10, "retargeting",
                                                                                                  ifelse(skyit_5$va_closer_id == 11, "social_player",
                                                                                                         ifelse(skyit_5$va_closer_id == 12, "direct_email_marketing",
                                                                                                                ifelse(skyit_5$va_closer_id == 13, "paid_social",
                                                                                                                       ifelse(skyit_5$va_closer_id == 14, "newsletter",
                                                                                                                              ifelse(skyit_5$va_closer_id == 15, "acquisition",
                                                                                                                                     ifelse(skyit_5$va_closer_id == 16, "AMP_w",
                                                                                                                                            ifelse(skyit_5$va_closer_id == 17, "web_push_notification",
                                                                                                                                                   ifelse(skyit_5$va_closer_id == 18, "DEM_w",
                                                                                                                                                          ifelse(skyit_5$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                 ifelse(skyit_5$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                        ifelse(skyit_5$va_closer_id == 21, "internal",
                                                                                                                                                                               ifelse(skyit_5$va_closer_id == 22, "affiliation",
                                                                                                                                                                                      ifelse(skyit_5$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                             ifelse(skyit_5$va_closer_id == 24, "short_url",
                                                                                                                                                                                                    ifelse(skyit_5$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

write.parquet(skyit_5, path_scarico_skyitdev) # write scarico skyitdev

scarico <- read.parquet(path_scarico_skyitdev) # read scarico skyitdev

# Esplorazione dati: hits, external_id, cookie unici, visite uniche

exteranl_id_unici <- arrange(summarize(groupBy(scarico, "external_id_post_evar"), count = countDistinct(scarico$post_visid_concatenated)), "count", decreasing = T) # conto cookie distinti per external id

cookie_unici <- arrange(summarize(groupBy(scarico, "post_visid_concatenated"), count = countDistinct(scarico$visit_num)), "count", decreasing = T) # conto visite distinte per cookie

visite_uniche <- summarize(cookie_unici, sum = sum(cookie_unici$count)) # somma visite distinte calcolate sopra


###### CORPORATE ######

corporate <- filter(scarico, 'post_channel=="corporate" OR post_channel=="Guidatv"') # filtro primo livello corporate e guida tv

write.parquet(corporate, path_scarico_corporate) # write scarico corporate

corporate <- read.parquet(path_scarico_corporate) # read scarico corporate


# Esplorazione dati corporate: hits, external_id, cookie unici, visite uniche, page views, video views

external_id_unici_corporate <- arrange(summarize(groupBy(corporate, "external_id_post_evar"), count = countDistinct(corporate$post_visid_concatenated)), "count", decreasing = T) # conto cookie distinti per external id Corporate

cookie_unici_corporate <- arrange(summarize(groupBy(corporate, "post_visid_concatenated"), count = countDistinct(corporate$visit_num)), "count", decreasing = T) # conto visite distinte per cookie Corporate

visite_uniche_corporate <- summarize(cookie_unici_corporate, sum = sum(cookie_unici_corporate$count)) # somma visite distinte corporate calcolate sopra

page_view_corporate <- filter(corporate, 'post_page_event_value == "page_view"') # filtro page views di corporate

video_view_corporate <- filter(corporate, 'post_page_event_value like "%media%"') # filtro video views di corporate

# # cookie per external id con percentili corporate
# 
# createOrReplaceTempView(external_id_unici_corporate, "external_id_unici_corporate") # tempview conto cookie distinti per external id Corporate
# 
# percentili_cookie_corporate <- sql("select percentile_approx(count, 0.25) as p25,
#                                    percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                    percentile_approx(count, 0.75) as p75,
#                                    percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                    percentile_approx(count, 1) as max
#                                    from external_id_unici_corporate
#                                    where count is NOT NULL")

# # percentili visite uniche per ext id
# 
# visite_uniche_corporate <- select(corporate,
#                                   "external_id_post_evar",
#                                   "post_visid_concatenated",
#                                   "visit_num")
# 
# visite_uniche_corporate_conc <- arrange(summarize(groupBy(visite_uniche_corporate, "external_id_post_evar"), 
#                                         count = countDistinct(concat(visite_uniche_corporate$post_visid_concatenated, visite_uniche_corporate$visit_num))), 
#                                         "count", decreasing = T) # concatenazione visit num e cookie per ottenere numero visite e aggregazione per external_id


# createOrReplaceTempView(visite_uniche_corporate_conc, "visite_uniche_corporate_conc")
# 
# percentili_visite_corporate <- sql("select percentile_approx(count, 0.25) as q25,
#                                    percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                    percentile_approx(count, 0.75) as q75,
#                                    percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                    percentile_approx(count, 1) as max
#                                    from visite_uniche_corporate_conc
#                                    where count is NOT NULL")

page_view_per_external_id_corporate <- summarize(groupBy(page_view_corporate, "external_id_post_evar"), 
                                                 count = count(page_view_corporate$post_page_event_value)) # page views per external_id corporate

# createOrReplaceTempView(page_view_per_external_id_corporate, "page_view_per_external_id_corporate")
# 
# percentili_page_view_corporate <- sql("select percentile_approx(count, 0.25) as q25,
#                                       percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                       percentile_approx(count, 0.75) as q75,
#                                       percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                       max(count) as max,
#                                       mean(count) as mean
#                                       from page_view_per_external_id_corporate
#                                       where count is NOT NULL")

# video per external id con percentili:

video_per_external_id_corporate <- summarize(groupBy(video_view_corporate, "external_id_post_evar"), count = count(video_view_corporate$post_page_event_value)) # video views per external_id corporate

# createOrReplaceTempView(video_per_external_id_corporate, "video_per_external_id_corporate")
# 
# percentili_video_corporate <- sql("select percentile_approx(count, 0.25) as q25,
#                                   percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                   percentile_approx(count, 0.75) as q75,
#                                   percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                   max(count) as max
#                                   from video_per_external_id_corporate
#                                   where count is NOT NULL")


month_corporate <- withColumn(corporate, "month", month(corporate$date_time_dt)) # Aggiungo il mese a ciascun osservazione

visite_uniche_month_corporate <- summarize(groupBy(month_corporate, "month"), count = countDistinct(concat(month_corporate$post_visid_concatenated, month_corporate$visit_num))) # visite uniche per mese

external_id_month_corporate <- summarize(groupBy(month_corporate, "month"), count = countDistinct(month_corporate$external_id_post_evar)) # external id per mese al fine di calcolare la media visite per ogni mese (fatto su excel per comodit?? prendendo i risultati della tabella precedente e dividendoli per questa)

month_corporate2 <- withColumn(month_corporate, "unique_visit", concat(month_corporate$post_visid_concatenated, month_corporate$visit_num)) # aggiungo la colonna visite uniche al dataset month_corporate

visite_uniche_month_external_id_corporate <- summarize(groupBy(month_corporate2, "month", "external_id_post_evar"), count = countDistinct(month_corporate2$unique_visit)) # calcolo le visite uniche per external id per mese al fine di calcolare la mediana di visite per ogni mese

#va cambiato il numero '1' e runnare per ogni mese e.g: '1' per gennaio, '2' per febbraio, etc.
# visite_uniche_month_external_id_corporate_2 <- filter(visite_uniche_month_external_id_corporate, "month like '1'") 

# createOrReplaceTempView(visite_uniche_month_external_id_corporate_2, "visite_uniche_month_external_id_corporate_2")
# 
# percentili_month_external_id_corporate <- sql("select percentile_approx(count, 0.25) as q25,
#                                               percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                               percentile_approx(count, 0.75) as q75,
#                                               percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                               percentile_approx(count, 1) as max
#                                               from visite_uniche_month_external_id_corporate_2
#                                               where count is NOT NULL")
# 
# View(head(percentili_month_external_id_corporate, 10))
# 
# # mobile_external_id_corporate <- arrange(summarize(groupBy(corporate, "post_mobiledevice"), count = countDistinct(corporate$external_id_post_evar)), "count", decreasing = T) # conto quanti "post_mobiledevice" vengono utilizzati da quanti external id
# # 
# # View(head(mobile_external_id_corporate, 100)) NON UTILE

os_external_id_corporate <- arrange(summarize(groupBy(corporate, "mapped_os_value"), count = countDistinct(corporate$external_id_post_evar)), "count", decreasing = T) # conto quanti diversi sistemi operativi vengono utilizzati da quanti external id

# View(head(os_external_id_corporate, 1000))

# riconduco i vari tipi di device a mobile o desktop

createOrReplaceTempView(os_external_id_corporate,'os_external_id_corporate')

mobile_desktop_corporate <- sql('SELECT *,
                                CASE WHEN (mapped_os_value LIKE "Linux%" OR mapped_os_value LIKE "Window%" OR mapped_os_value LIKE "OS%" OR 
                                          mapped_os_value LIKE "Macintosh") THEN "desktop"
                                WHEN (mapped_os_value LIKE "Firefox%" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Chrome%" OR 
                                      mapped_os_value LIKE "Tizen%" OR mapped_os_value LIKE "%Mobile%" 
                                      OR mapped_os_value LIKE "Android%") THEN "mobile" ELSE "other" END AS device_type
                                FROM os_external_id_corporate')

device_type_corporate <- arrange(summarize(groupBy(mobile_desktop_corporate,"device_type"), sum = sum(mobile_desktop_corporate$count)), "sum", decreasing=T) # conto tipo device totale corporate

# riconduco i vari tipi di sistema operativo a linux, windows, apple, other

createOrReplaceTempView(os_external_id_corporate,'os_external_id_corporate')

os_corporate <- sql('SELECT *,
                    CASE WHEN (mapped_os_value LIKE "Linux%") THEN "linux"
                    WHEN (mapped_os_value LIKE "Window%") THEN "windows"
                    WHEN (mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh" OR mapped_os_value LIKE "%iPhone%" 
                          OR mapped_os_value LIKE "Mobile iOS%") THEN "apple"
                    WHEN (mapped_os_value LIKE "Android%") THEN "android" ELSE "other" END AS os
                    FROM os_external_id_corporate')

os_type_corporate <- arrange(summarize(groupBy(os_corporate, "os"),sum=sum(os_corporate$count)), "sum", decreasing=T)

last_touch_corporate <- arrange(summarize(groupBy(corporate, 'last_touch_label'), count=countDistinct(corporate$external_id_post_evar)),"count", decreasing=T) # calcolo lista last touch point per identificare chi proviene da social network


############################## TG24 ###########################################

tg24 <- filter(scarico, 'post_channel == "tg24"')

write.parquet(tg24, path_scarico_tg24)

tg24 <- read.parquet(path_scarico_tg24)

external_id_unici_tg24 <- arrange(summarize(groupBy(tg24, "external_id_post_evar"), count = countDistinct(tg24$post_visid_concatenated)), "count", decreasing = T)

cookie_unici_tg24 <- arrange(summarize(groupBy(tg24, "post_visid_concatenated"), count = countDistinct(tg24$visit_num)), "count", decreasing = T)

visite_uniche_tg24 <- summarize(cookie_unici_tg24, sum = sum(cookie_unici_tg24$count))

page_view_tg24 <- filter(tg24,'post_page_event_value == "page_view"')

video_view_tg24 <- filter(tg24, 'post_page_event_value like "%media%"')

# # cookie per external id con percentili
# 
# createOrReplaceTempView(external_id_unici_tg24, "external_id_unici_tg24") 
# 
# percentili_cookie_tg24 <- sql("select percentile_approx(count, 0.25) as q25,
#                               percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                               percentile_approx(count, 0.75) as q75,
#                               percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                               percentile_approx(count, 1) as max
#                               from external_id_unici_tg24
#                               where count is NOT NULL")

# percentili visite uniche per ext id

visite_uniche_tg24 <- select(tg24,
                             "external_id_post_evar",
                             "post_visid_concatenated",
                             "visit_num")

visite_uniche_tg24_conc <- arrange(summarize(groupBy(visite_uniche_tg24, "external_id_post_evar"), count = countDistinct(concat(visite_uniche_tg24$post_visid_concatenated, visite_uniche_tg24$visit_num))), "count", decreasing = T) # concatenazione visit num e cookie per ottenere numero visite e aggregazione per external_id

# createOrReplaceTempView(visite_uniche_tg24_conc, "visite_uniche_tg24_conc")
# 
# percentili_visite_tg24 <- sql("select percentile_approx(count, 0.25) as q25,
#                               percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                               percentile_approx(count, 0.75) as q75,
#                               percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                               percentile_approx(count, 1) as max
#                               from visite_uniche_tg24_conc
#                               where count is NOT NULL")

page_view_per_external_id_tg24 <- summarize(groupBy(page_view_tg24, "external_id_post_evar"), count = count(page_view_tg24$post_page_event_value)) # page views per external_id tg24

# createOrReplaceTempView(page_view_per_external_id_tg24, "page_view_per_external_id_tg24")
# 
# percentili_page_view_tg24 <- sql("select percentile_approx(count, 0.25) as q25,
#                                  percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                  percentile_approx(count, 0.75) as q75,
#                                  percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                  max(count) as max,
#                                  mean(count) as mean
#                                  from page_view_per_external_id_tg24
#                                  where count is NOT NULL")

# video per external id con percentili:

video_per_external_id_tg24 <- summarize(groupBy(video_view_tg24, "external_id_post_evar"), count = count(video_view_tg24$post_page_event_value))

# createOrReplaceTempView(video_per_external_id_tg24, "video_per_external_id_tg24")
# 
# percentili_video_tg24 <- sql("select percentile_approx(count, 0.25) as q25,
#                              percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                              percentile_approx(count, 0.75) as q75,
#                              percentile_approx(count, 0.95) as q95, -- 95esimo percentile
#                              max(count) as max
#                              from video_per_external_id_tg24
#                              where count is NOT NULL")

month_tg24 <- withColumn(tg24, "month", month(tg24$date_time_dt)) # Aggiungo il mese a ciascun osservazione

visite_uniche_month_tg24 <- summarize(groupBy(month_tg24, "month"), count = countDistinct(concat(month_tg24$post_visid_concatenated, month_tg24$visit_num))) # visite uniche per mese

external_id_month_tg24 <- summarize(groupBy(month_tg24, "month"), count = countDistinct(month_tg24$external_id_post_evar)) # external id per mese al fine di calcolare la media visite per ogni mese (fatto su excel per comodit?? prendendo i risultati della tabella precedente e dividendoli per questa)

month_tg24_2 <- withColumn(month_tg24, "unique_visit", concat(month_tg24$post_visid_concatenated, month_tg24$visit_num)) # aggiungo la colonna visite uniche al dataset month_tg24

visite_uniche_month_external_id_tg24 <- summarize(groupBy(month_tg24_2, "month", "external_id_post_evar"), count = countDistinct(month_tg24_2$unique_visit)) # calcolo le visite uniche per external id per mese al fine di calcolare la mediana di visite per ogni mese

# #va cambiato il numero '1' e runnare per ogni mese e.g: '1' per gennaio, '2' per febbraio, etc.
# visite_uniche_month_external_id_tg24_2 <- filter(visite_uniche_month_external_id_tg24 , "month like '1'") 
# 
# createOrReplaceTempView(visite_uniche_month_external_id_tg24_2, "visite_uniche_month_external_id_tg24_2")
# 
# percentili_month_external_id_tg24 <- sql("select percentile_approx(count, 0.25) as q25,
#                                          percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                          percentile_approx(count, 0.75) as q75,
#                                          percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                          percentile_approx(count, 1) as max
#                                          from visite_uniche_month_external_id_tg24_2
#                                          where count is NOT NULL")

os_external_id_tg24 <- arrange(summarize(groupBy(tg24, "mapped_os_value"), count = countDistinct(tg24$external_id_post_evar)), "count", decreasing = T) # conto quanti diversi sistemi operativi vengono utilizzati da quanti external id

# riconduco i vari tipi di device a mobile o desktop

createOrReplaceTempView(os_external_id_tg24, 'os_external_id_tg24')

mobile_desktop_tg24 <- sql('SELECT *,
                           CASE WHEN (mapped_os_value LIKE "Linux%" OR mapped_os_value LIKE "Window%" OR mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh") THEN "desktop"
                           WHEN (mapped_os_value LIKE "Firefox%" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Chrome%" OR mapped_os_value LIKE "Tizen%" OR mapped_os_value LIKE "%Mobile%" OR mapped_os_value LIKE "Android%") THEN "mobile" ELSE "other" END AS device_type
                           FROM os_external_id_tg24')

device_type_tg24 <- arrange(summarize(groupBy(mobile_desktop_tg24,"device_type"), sum = sum(mobile_desktop_tg24$count)), "sum", decreasing=T) # conto tipo device totale tg24

# riconduco i vari tipi di sistema operativo a linux, windows, apple, other

createOrReplaceTempView(os_external_id_tg24,'os_external_id_tg24')

os_tg24 <- sql('SELECT *,
               CASE WHEN (mapped_os_value LIKE "Linux%") THEN "linux"
               WHEN (mapped_os_value LIKE "Window%") THEN "windows"
               WHEN (mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh" OR mapped_os_value LIKE "%iPhone%" 
                      OR mapped_os_value LIKE "Mobile iOS%") THEN "apple"
               WHEN (mapped_os_value LIKE "Android%") THEN "android" ELSE "other" END AS os
               FROM os_external_id_tg24')

os_type_tg24 <- arrange(summarize(groupBy(os_tg24, "os"),sum=sum(os_tg24$count)), "sum", decreasing=T)

last_touch_tg24 <- arrange(summarize(groupBy(tg24, 'last_touch_label'), count=countDistinct(tg24$external_id_post_evar)), "count", decreasing=T) # calcolo lista last touch point per identificare chi proviene da social network


############################## SPORT ###########################################

sport <- filter(scarico, 'post_channel == "sport"')

write.parquet(sport, path_scarico_sport)

sport <- read.parquet(path_scarico_sport)

external_id_unici_sport <- arrange(summarize(groupBy(sport, "external_id_post_evar"), count = countDistinct(sport$post_visid_concatenated)), "count", decreasing = T)

cookie_unici_sport <- arrange(summarize(groupBy(sport, "post_visid_concatenated"), count = countDistinct(sport$visit_num)), "count", decreasing = T)

visite_uniche_sport <- summarize(cookie_unici_sport, sum = sum(cookie_unici_sport$count))

page_view_sport <- filter(sport,'post_page_event_value == "page_view"')

video_view_sport <- filter(sport, 'post_page_event_value like "%media%"')

# # cookie per external id con percentili
# 
# createOrReplaceTempView(external_id_unici_sport, "external_id_unici_sport") 
# 
# percentili_cookie_sport <- sql("select percentile_approx(count, 0.25) as q25,
#                                percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                percentile_approx(count, 0.75) as q75,
#                                percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                percentile_approx(count, 1) as max
#                                from external_id_unici_sport
#                                where count is NOT NULL")

# percentili visite uniche per ext id

visite_uniche_sport <- select(sport,
                              "external_id_post_evar",
                              "post_visid_concatenated",
                              "visit_num")

visite_uniche_sport_conc <- arrange(summarize(groupBy(visite_uniche_sport, "external_id_post_evar"), count = countDistinct(concat(visite_uniche_sport$post_visid_concatenated, visite_uniche_sport$visit_num))), "count", decreasing = T) # concatenazione visit num e cookie per ottenere numero visite e aggregazione per external_id

# createOrReplaceTempView(visite_uniche_sport_conc, "visite_uniche_sport_conc")
# 
# percentili_visite_sport <- sql("select percentile_approx(count, 0.25) as q25,
#                                percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                percentile_approx(count, 0.75) as q75,
#                                percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                percentile_approx(count, 1) as max
#                                from visite_uniche_sport_conc
#                                where count is NOT NULL")

page_view_per_external_id_sport <- summarize(groupBy(page_view_sport, "external_id_post_evar"), count = count(page_view_sport$post_page_event_value)) # page views per external_id sport

# createOrReplaceTempView(page_view_per_external_id_sport, "page_view_per_external_id_sport")
# 
# percentili_page_view_sport <- sql("select percentile_approx(count, 0.25) as q25,
#                                   percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                   percentile_approx(count, 0.75) as q75,
#                                   percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                   max(count) as max,
#                                   mean(count) as mean
#                                   from page_view_per_external_id_sport
#                                   where count is NOT NULL")

# video per external id con percentili:

video_per_external_id_sport <- summarize(groupBy(video_view_sport, "external_id_post_evar"), count = count(video_view_sport$post_page_event_value))

# createOrReplaceTempView(video_per_external_id_sport, "video_per_external_id_sport")
# 
# percentili_video_sport <- sql("select percentile_approx(count, 0.25) as q25,
#                               percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                               percentile_approx(count, 0.75) as q75,
#                               percentile_approx(count, 0.95) as q95, -- 95esimo percentile
#                               max(count) as max
#                               from video_per_external_id_sport
#                               where count is NOT NULL")

month_sport <- withColumn(sport, "month", month(sport$date_time_dt)) # Aggiungo il mese a ciascun osservazione

visite_uniche_month_sport <- summarize(groupBy(month_sport, "month"), count = countDistinct(concat(month_sport$post_visid_concatenated, month_sport$visit_num))) # visite uniche per mese

external_id_month_sport <- summarize(groupBy(month_sport, "month"), count = countDistinct(month_sport$external_id_post_evar)) # external id per mese al fine di calcolare la media visite per ogni mese (fatto su excel per comodit?? prendendo i risultati della tabella precedente e dividendoli per questa)

month_sport_2 <- withColumn(month_sport, "unique_visit", concat(month_sport$post_visid_concatenated, month_sport$visit_num)) # aggiungo la colonna visite uniche al dataset month_sport

visite_uniche_month_external_id_sport <- summarize(groupBy(month_sport_2, "month", "external_id_post_evar"), count = countDistinct(month_sport_2$unique_visit)) # calcolo le visite uniche per external id per mese al fine di calcolare la mediana di visite per ogni mese

# #va cambiato il numero '1' e runnare per ogni mese e.g: '1' per gennaio, '2' per febbraio, etc.
# visite_uniche_month_external_id_sport_2 <- filter(visite_uniche_month_external_id_sport , "month like '1'") 

# createOrReplaceTempView(visite_uniche_month_external_id_sport_2, "visite_uniche_month_external_id_sport_2")
# 
# percentili_month_external_id_sport <- sql("select percentile_approx(count, 0.25) as q25,
#                                           percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                           percentile_approx(count, 0.75) as q75,
#                                           percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                           percentile_approx(count, 1) as max
#                                           from visite_uniche_month_external_id_sport_2
#                                           where count is NOT NULL")

mobile_external_id_sport <- arrange(summarize(groupBy(sport, "post_mobiledevice"), count = countDistinct(sport$external_id_post_evar)), "count", decreasing = T) # conto quanti "post_mobiledevice" vengono utilizzati da quanti external id

os_external_id_sport <- arrange(summarize(groupBy(sport, "mapped_os_value"), count = countDistinct(sport$external_id_post_evar)), "count", decreasing = T) # conto quanti diversi sistemi operativi vengono utilizzati da quanti external id

# riconduco i vari tipi di device a mobile o desktop

createOrReplaceTempView(os_external_id_sport, 'os_external_id_sport')

mobile_desktop_sport <- sql('SELECT *,
                            CASE WHEN (mapped_os_value LIKE "Linux%" OR mapped_os_value LIKE "Window%" OR mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh") THEN "desktop"
                            WHEN (mapped_os_value LIKE "Firefox%" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Chrome%" OR mapped_os_value LIKE "Tizen%" OR mapped_os_value LIKE "%Mobile%" OR mapped_os_value LIKE "Android%") THEN "mobile" ELSE "other" END AS device_type
                            FROM os_external_id_sport')

device_type_sport <- arrange(summarize(groupBy(mobile_desktop_sport,"device_type"), sum = sum(mobile_desktop_sport$count)), "sum", decreasing=T) # conto tipo device totale sport

# riconduco i vari tipi di sistema operativo a linux, windows, apple, other

createOrReplaceTempView(os_external_id_sport,'os_external_id_sport')

os_sport <- sql('SELECT *,
                CASE WHEN (mapped_os_value LIKE "Linux%") THEN "linux"
                WHEN (mapped_os_value LIKE "Window%") THEN "windows"
                WHEN (mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Mobile iOS%") THEN "apple"
                WHEN (mapped_os_value LIKE "Android%") THEN "android" ELSE "other" END AS os
                FROM os_external_id_sport')

os_type_sport <- arrange(summarize(groupBy(os_sport, "os"),sum=sum(os_sport$count)), "sum", decreasing=T)

last_touch_sport <- arrange(summarize(groupBy(sport, 'last_touch_label'), count=countDistinct(sport$external_id_post_evar)), "count", decreasing=T) # calcolo lista last touch point per identificare chi proviene da social network


############################## APP WSC ###########################################

skyappwsc <- read.parquet('/STAGE/adobe/reportsuite=skyappwsc.prod')

skyappwsc_1 <- select(skyappwsc, "external_id_post_evar", #external id
                      "post_visid_high", #first part of cookie
                      "post_visid_low", #second part of cookie
                      "post_visid_concatenated", #cookie
                      "visit_num", #?
                      "date_time", #date of the visit
                      "post_channel",
                      "page_name_post_evar",
                      "page_name_post_prop",
                      "site_section",
                      "hit_source",
                      "exclude_hit",
                      "post_page_event_value",
                      "post_mobiledevice",
                      "post_campaign",
                      "va_closer_id",
                      "mapped_connection_type_value",
                      "mapped_browser_type_value",
                      "mapped_country_value",
                      "mapped_language_value",
                      "mapped_os_value",
                      "mapped_resolution_value",
                      "mapped_browser_value",
                      "paid_search"
)

skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))
skyappwsc_3 <- withColumn(skyappwsc_2, "date_time_ts", cast(skyappwsc_2$date_time, "timestamp"))
skyappwsc_4 <- filter(skyappwsc_3, "external_id_post_evar is not NULL")

skyappwsc_5 <- filter(skyappwsc_4, skyappwsc_4$date_time_dt >= data_inizio) 
skyappwsc_6 <- filter(skyappwsc_5, skyappwsc_5$date_time_dt <= data_fine)

#label colonna last_touch_point:
skyappwsc_6$last_touch_label <- ifelse(skyappwsc_6$va_closer_id == 1, "editorial_recommendation",
                                       ifelse(skyappwsc_6$va_closer_id == 2, "search_engine_organic",
                                              ifelse(skyappwsc_6$va_closer_id == 3, "internal_referrer_w",
                                                     ifelse(skyappwsc_6$va_closer_id == 4, "social_network",
                                                            ifelse(skyappwsc_6$va_closer_id == 5, "direct_bookmark",
                                                                   ifelse(skyappwsc_6$va_closer_id == 6, "referring_domains",
                                                                          ifelse(skyappwsc_6$va_closer_id == 7, "paid_display",
                                                                                 ifelse(skyappwsc_6$va_closer_id == 8, "paid_search",
                                                                                        ifelse(skyappwsc_6$va_closer_id == 9, "paid_other",
                                                                                               ifelse(skyappwsc_6$va_closer_id == 10, "retargeting",
                                                                                                      ifelse(skyappwsc_6$va_closer_id == 11, "social_player",
                                                                                                             ifelse(skyappwsc_6$va_closer_id == 12, "direct_email_marketing",
                                                                                                                    ifelse(skyappwsc_6$va_closer_id == 13, "paid_social",
                                                                                                                           ifelse(skyappwsc_6$va_closer_id == 14, "newsletter",
                                                                                                                                  ifelse(skyappwsc_6$va_closer_id == 15, "acquisition",
                                                                                                                                         ifelse(skyappwsc_6$va_closer_id == 16, "AMP_w",
                                                                                                                                                ifelse(skyappwsc_6$va_closer_id == 17, "web_push_notification",
                                                                                                                                                       ifelse(skyappwsc_6$va_closer_id == 18, "DEM_w",
                                                                                                                                                              ifelse(skyappwsc_6$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                     ifelse(skyappwsc_6$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                            ifelse(skyappwsc_6$va_closer_id == 21, "internal",
                                                                                                                                                                                   ifelse(skyappwsc_6$va_closer_id == 22, "affiliation",
                                                                                                                                                                                          ifelse(skyappwsc_6$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                                 ifelse(skyappwsc_6$va_closer_id == 24, "short_url",
                                                                                                                                                                                                        ifelse(skyappwsc_6$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

write.parquet(skyappwsc_6, path_scarico_skyappwsc) # write scarico app wsc

appwsc <- read.parquet(path_scarico_skyappwsc) 

external_id_unici_appwsc <- arrange(summarize(groupBy(appwsc, "external_id_post_evar"), count = countDistinct(appwsc$post_visid_concatenated)), "count", decreasing = T)

cookie_unici_appwsc <- arrange(summarize(groupBy(appwsc, "post_visid_concatenated"), count = countDistinct(appwsc$visit_num)), "count", decreasing = T)

visite_uniche_appwsc<- summarize(cookie_unici_appwsc, sum = sum(cookie_unici_appwsc$count))

page_view_appwsc <- filter(appwsc,'post_page_event_value == "page_view"')

video_view_appwsc <- filter(appwsc, 'post_page_event_value like "%media%"')

# # cookie per external id con percentili
# 
# createOrReplaceTempView(external_id_unici_appwsc, "external_id_unici_appwsc") 
# 
# percentili_cookie_appwsc <- sql("select percentile_approx(count, 0.25) as q25,
#                                 percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                 percentile_approx(count, 0.75) as q75,
#                                 percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                 percentile_approx(count, 1) as max
#                                 from external_id_unici_appwsc
#                                 where count is NOT NULL")

# percentili visite uniche per ext id

visite_uniche_appwsc <- select(appwsc,
                               "external_id_post_evar",
                               "post_visid_concatenated",
                               "visit_num")

visite_uniche_appwsc_conc <- arrange(summarize(groupBy(visite_uniche_appwsc, "external_id_post_evar"), count = countDistinct(concat(visite_uniche_appwsc$post_visid_concatenated, visite_uniche_appwsc$visit_num))), "count", decreasing = T) # concatenazione visit num e cookie per ottenere numero visite e aggregazione per external_id

# createOrReplaceTempView(visite_uniche_appwsc_conc, "visite_uniche_appwsc_conc")
# 
# percentili_visite_appwsc <- sql("select percentile_approx(count, 0.25) as q25,
#                                 percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                 percentile_approx(count, 0.75) as q75,
#                                 percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                 percentile_approx(count, 1) as max
#                                 from visite_uniche_appwsc_conc
#                                 where count is NOT NULL")

page_view_per_external_id_appwsc <- summarize(groupBy(page_view_appwsc, "external_id_post_evar"), count = count(page_view_appwsc$post_page_event_value)) # page views per external_id appwsc

# createOrReplaceTempView(page_view_per_external_id_appwsc, "page_view_per_external_id_appwsc")
# 
# percentili_page_view_appwsc <- sql("select percentile_approx(count, 0.25) as q25,
#                                    percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                    percentile_approx(count, 0.75) as q75,
#                                    percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                    max(count) as max,
#                                    mean(count) as mean
#                                    from page_view_per_external_id_appwsc
#                                    where count is NOT NULL")

# video per external id con percentili:
# 
# video_per_external_id_appwsc <- summarize(groupBy(video_view_appwsc, "external_id_post_evar"), count = count(video_view_appwsc$post_page_event_value))
# 
# createOrReplaceTempView(video_per_external_id_appwsc, "video_per_external_id_appwsc")
# 
# percentili_video_appwsc <- sql("select percentile_approx(count, 0.25) as q25,
#                            percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                            percentile_approx(count, 0.75) as q75,
#                            percentile_approx(count, 0.95) as q95, -- 95esimo percentile
#                            max(count) as max
#                            from video_per_external_id_appwsc
#                            where count is NOT NULL")

month_appwsc <- withColumn(appwsc, "month", month(appwsc$date_time_dt)) # Aggiungo il mese a ciascun osservazione

visite_uniche_month_appwsc <- summarize(groupBy(month_appwsc, "month"), count = countDistinct(concat(month_appwsc$post_visid_concatenated, month_appwsc$visit_num))) # visite uniche per mese

external_id_month_appwsc <- summarize(groupBy(month_appwsc, "month"), count = countDistinct(month_appwsc$external_id_post_evar)) # external id per mese al fine di calcolare la media visite per ogni mese (fatto su excel per comodit?? prendendo i risultati della tabella precedente e dividendoli per questa)

month_appwsc_2 <- withColumn(month_appwsc, "unique_visit", concat(month_appwsc$post_visid_concatenated, month_appwsc$visit_num)) # aggiungo la colonna visite uniche al dataset month_appwsc

visite_uniche_month_external_id_appwsc <- summarize(groupBy(month_appwsc_2, "month", "external_id_post_evar"), count = countDistinct(month_appwsc_2$unique_visit)) # calcolo le visite uniche per external id per mese al fine di calcolare la mediana di visite per ogni mese

# #va cambiato il numero '1' e runnare per ogni mese e.g: '1' per gennaio, '2' per febbraio, etc.
# visite_uniche_month_external_id_appwsc_2 <- filter(visite_uniche_month_external_id_appwsc , "month like '1'") 

# createOrReplaceTempView(visite_uniche_month_external_id_appwsc_2, "visite_uniche_month_external_id_appwsc_2")
# 
# percentili_month_external_id_appwsc <- sql("select percentile_approx(count, 0.25) as q25,
#                                            percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                            percentile_approx(count, 0.75) as q75,
#                                            percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                            percentile_approx(count, 1) as max
#                                            from visite_uniche_month_external_id_appwsc_2
#                                            where count is NOT NULL")

mobile_external_id_appwsc <- arrange(summarize(groupBy(appwsc, "post_mobiledevice"), count = countDistinct(appwsc$external_id_post_evar)), "count", decreasing = T) # conto quanti "post_mobiledevice" vengono utilizzati da quanti external id

os_external_id_appwsc <- arrange(summarize(groupBy(appwsc, "mapped_os_value"), count = countDistinct(appwsc$external_id_post_evar)), "count", decreasing = T) # conto quanti diversi sistemi operativi vengono utilizzati da quanti external id

# riconduco i vari tipi di device a mobile o desktop

createOrReplaceTempView(os_external_id_appwsc, 'os_external_id_appwsc')

mobile_desktop_appwsc <- sql('SELECT *,
                             CASE WHEN (mapped_os_value LIKE "Linux%" OR mapped_os_value LIKE "Window%" OR mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh") THEN "desktop"
                             WHEN (mapped_os_value LIKE "Firefox%" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Chrome%" OR mapped_os_value LIKE "Tizen%" OR mapped_os_value LIKE "%Mobile%" OR mapped_os_value LIKE "Android%") THEN "mobile" ELSE "other" END AS device_type
                             FROM os_external_id_appwsc')

device_type_appwsc <- arrange(summarize(groupBy(mobile_desktop_appwsc,"device_type"), sum = sum(mobile_desktop_appwsc$count)), "sum", decreasing=T) # conto tipo device totale appwsc

# riconduco i vari tipi di sistema operativo a linux, windows, apple, other

createOrReplaceTempView(os_external_id_appwsc,'os_external_id_appwsc')

os_appwsc <- sql('SELECT *,
                 CASE WHEN (mapped_os_value LIKE "Linux%") THEN "linux"
                 WHEN (mapped_os_value LIKE "Window%") THEN "windows"
                 WHEN (mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Mobile iOS%") THEN "apple"
                 WHEN (mapped_os_value LIKE "Android%") THEN "android" ELSE "other" END AS os
                 FROM os_external_id_appwsc')

os_type_appwsc <- arrange(summarize(groupBy(os_appwsc, "os"),sum=sum(os_appwsc$count)), "sum", decreasing=T)

last_touch_appwsc <- arrange(summarize(groupBy(appwsc, 'last_touch_label'), count=countDistinct(appwsc$external_id_post_evar)), "count", decreasing=T) # calcolo lista last touch point per identificare chi proviene da social network


############################# APP GUIDA TV ################################

skyappgtv <- read.parquet("/STAGE/adobe/reportsuite=skyappguidatv.prod")

skyappgtv_1 <- select(skyappgtv, "external_id_post_evar", #external id
                      "post_visid_concatenated", #cookie
                      "visit_num", 
                      "date_time", #date of the visit
                      "post_channel",
                      "site_section_post_evar",
                      "post_page_event_value",
                      "post_mobiledevice",
                      "post_campaign",
                      "va_closer_id",
                      "mapped_connection_type_value",
                      "mapped_browser_type_value",
                      "mapped_country_value",
                      "mapped_language_value",
                      "mapped_os_value",
                      "mapped_resolution_value",
                      "mapped_browser_value",
                      "paid_search"
)

skyappgtv_2 <- withColumn(skyappgtv_1, "date_time_dt", cast(skyappgtv_1$date_time, "date"))
skyappgtv_3 <- withColumn(skyappgtv_2, "date_time_ts", cast(skyappgtv_2$date_time, "timestamp"))
skyappgtv_4 <- filter(skyappgtv_3, "external_id_post_evar is not NULL")

skyappgtv_5 <- filter(skyappgtv_4, skyappgtv_4$date_time_dt >= data_inizio) 
skyappgtv_6 <- filter(skyappgtv_5, skyappgtv_5$date_time_dt <= data_fine)

#label colonna last_touch_point:
skyappgtv_6$last_touch_label <- ifelse(skyappgtv_6$va_closer_id == 1, "editorial_recommendation",
                                       ifelse(skyappgtv_6$va_closer_id == 2, "search_engine_organic",
                                              ifelse(skyappgtv_6$va_closer_id == 3, "internal_referrer_w",
                                                     ifelse(skyappgtv_6$va_closer_id == 4, "social_network",
                                                            ifelse(skyappgtv_6$va_closer_id == 5, "direct_bookmark",
                                                                   ifelse(skyappgtv_6$va_closer_id == 6, "referring_domains",
                                                                          ifelse(skyappgtv_6$va_closer_id == 7, "paid_display",
                                                                                 ifelse(skyappgtv_6$va_closer_id == 8, "paid_search",
                                                                                        ifelse(skyappgtv_6$va_closer_id == 9, "paid_other",
                                                                                               ifelse(skyappgtv_6$va_closer_id == 10, "retargeting",
                                                                                                      ifelse(skyappgtv_6$va_closer_id == 11, "social_player",
                                                                                                             ifelse(skyappgtv_6$va_closer_id == 12, "direct_email_marketing",
                                                                                                                    ifelse(skyappgtv_6$va_closer_id == 13, "paid_social",
                                                                                                                           ifelse(skyappgtv_6$va_closer_id == 14, "newsletter",
                                                                                                                                  ifelse(skyappgtv_6$va_closer_id == 15, "acquisition",
                                                                                                                                         ifelse(skyappgtv_6$va_closer_id == 16, "AMP_w",
                                                                                                                                                ifelse(skyappgtv_6$va_closer_id == 17, "web_push_notification",
                                                                                                                                                       ifelse(skyappgtv_6$va_closer_id == 18, "DEM_w",
                                                                                                                                                              ifelse(skyappgtv_6$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                     ifelse(skyappgtv_6$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                            ifelse(skyappgtv_6$va_closer_id == 21, "internal",
                                                                                                                                                                                   ifelse(skyappgtv_6$va_closer_id == 22, "affiliation",
                                                                                                                                                                                          ifelse(skyappgtv_6$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                                 ifelse(skyappgtv_6$va_closer_id == 24, "short_url",
                                                                                                                                                                                                        ifelse(skyappgtv_6$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

# PER APP GUIDA TV VA FATTO IL DIZIONARIO E IL JOIN

skyappgtv_diz <- distinct(select(skyappgtv, "external_id_post_evar", "post_visid_concatenated"))

skyappgtv_diz2 <- filter(skyappgtv_diz, isNotNull(skyappgtv_diz$external_id_post_evar))
skyappgtv_diz3 <- filter(skyappgtv_diz2, isNotNull(skyappgtv_diz2$post_visid_concatenated))

write.parquet(skyappgtv_diz3, path_diz_appgtv)

skyappgtv_diz5 <- read.parquet(path_diz_appgtv)

skyappgtv_diz6 <- withColumnRenamed(skyappgtv_diz5, "external_id_post_evar", "skyid")
skyappgtv_diz7 <- withColumnRenamed(skyappgtv_diz6, "post_visid_concatenated", "cookieid")

createOrReplaceTempView(skyappgtv_diz7, "skyappgtv_diz7")
createOrReplaceTempView(skyappgtv_6, "skyappgtv_6")

join_skyappgtv_diz <- sql("select skyappgtv_6.*, skyappgtv_diz7.skyid
                          from skyappgtv_6
                          left join skyappgtv_diz7 
                          on skyappgtv_6.post_visid_concatenated = skyappgtv_diz7.cookieid ")

join_skyappgtv_diz_2 <-  drop(join_skyappgtv_diz,  "external_id_post_evar")
join_skyappgtv_diz_2 <-  withColumnRenamed(join_skyappgtv_diz_2, "skyid", "external_id_post_evar")

write.parquet(join_skyappgtv_diz_2, path_scarico_skyappgtv) # write scarico app wsc

appgtv <- read.parquet(path_scarico_skyappgtv) 

external_id_unici_appgtv <- arrange(summarize(groupBy(appgtv, "external_id_post_evar"), count = countDistinct(appgtv$post_visid_concatenated)), "count", decreasing = T)

cookie_unici_appgtv <- arrange(summarize(groupBy(appgtv, "post_visid_concatenated"), count = countDistinct(appgtv$visit_num)), "count", decreasing = T)

visite_uniche_appgtv<- summarize(cookie_unici_appgtv, sum = sum(cookie_unici_appgtv$count))

page_view_appgtv <- filter(appgtv,'post_page_event_value == "page_view"')

video_view_appgtv <- filter(appgtv, 'post_page_event_value like "%media%"')

# cookie per external id con percentili

# createOrReplaceTempView(external_id_unici_appgtv, "external_id_unici_appgtv") 
# 
# percentili_cookie_appgtv <- sql("select percentile_approx(count, 0.25) as q25,
#                                 percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                 percentile_approx(count, 0.75) as q75,
#                                 percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                 percentile_approx(count, 1) as max
#                                 from external_id_unici_appgtv
#                                 where count is NOT NULL")

# percentili visite uniche per ext id

visite_uniche_appgtv <- select(appgtv,
                               "external_id_post_evar",
                               "post_visid_concatenated",
                               "visit_num")

visite_uniche_appgtv_conc <- arrange(summarize(groupBy(visite_uniche_appgtv, "external_id_post_evar"), count = countDistinct(concat(visite_uniche_appgtv$post_visid_concatenated, visite_uniche_appgtv$visit_num))), "count", decreasing = T) # concatenazione visit num e cookie per ottenere numero visite e aggregazione per external_id

# createOrReplaceTempView(visite_uniche_appgtv_conc, "visite_uniche_appgtv_conc")
# 
# percentili_visite_appgtv <- sql("select percentile_approx(count, 0.25) as q25,
#                                 percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                 percentile_approx(count, 0.75) as q75,
#                                 percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                 percentile_approx(count, 1) as max
#                                 from visite_uniche_appgtv_conc
#                                 where count is NOT NULL")

page_view_per_external_id_appgtv <- summarize(groupBy(page_view_appgtv, "external_id_post_evar"), count = count(page_view_appgtv$post_page_event_value)) # page views per external_id appgtv

# createOrReplaceTempView(page_view_per_external_id_appgtv, "page_view_per_external_id_appgtv")
# 
# percentili_page_view_appgtv <- sql("select percentile_approx(count, 0.25) as q25,
#                                    percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                    percentile_approx(count, 0.75) as q75,
#                                    percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                    max(count) as max,
#                                    mean(count) as mean
#                                    from page_view_per_external_id_appgtv
#                                    where count is NOT NULL")

# video per external id con percentili:

video_per_external_id_appgtv <- summarize(groupBy(video_view_appgtv, "external_id_post_evar"), count = count(video_view_appgtv$post_page_event_value))

# createOrReplaceTempView(video_per_external_id_appgtv, "video_per_external_id_appgtv")
# 
# percentili_video_appgtv <- sql("select percentile_approx(count, 0.25) as q25,
#                                percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                percentile_approx(count, 0.75) as q75,
#                                percentile_approx(count, 0.95) as q95, -- 95esimo percentile
#                                max(count) as max
#                                from video_per_external_id_appgtv
#                                where count is NOT NULL")

month_appgtv <- withColumn(appgtv, "month", month(appgtv$date_time_dt)) # Aggiungo il mese a ciascun osservazione

visite_uniche_month_appgtv <- summarize(groupBy(month_appgtv, "month"), count = countDistinct(concat(month_appgtv$post_visid_concatenated, month_appgtv$visit_num))) # visite uniche per mese

external_id_month_appgtv <- summarize(groupBy(month_appgtv, "month"), count = countDistinct(month_appgtv$external_id_post_evar)) # external id per mese al fine di calcolare la media visite per ogni mese (fatto su excel per comodit?? prendendo i risultati della tabella precedente e dividendoli per questa)

month_appgtv_2 <- withColumn(month_appgtv, "unique_visit", concat(month_appgtv$post_visid_concatenated, month_appgtv$visit_num)) # aggiungo la colonna visite uniche al dataset month_appgtv

visite_uniche_month_external_id_appgtv <- summarize(groupBy(month_appgtv_2, "month", "external_id_post_evar"), count = countDistinct(month_appgtv_2$unique_visit)) # calcolo le visite uniche per external id per mese al fine di calcolare la mediana di visite per ogni mese

# #va cambiato il numero '1' e runnare per ogni mese e.g: '1' per gennaio, '2' per febbraio, etc.
# visite_uniche_month_external_id_appgtv_2 <- filter(visite_uniche_month_external_id_appgtv , "month like '1'") 

# createOrReplaceTempView(visite_uniche_month_external_id_appgtv_2, "visite_uniche_month_external_id_appgtv_2")
# 
# percentili_month_external_id_appgtv <- sql("select percentile_approx(count, 0.25) as q25,
#                                            percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                            percentile_approx(count, 0.75) as q75,
#                                            percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                            percentile_approx(count, 1) as max
#                                            from visite_uniche_month_external_id_appgtv_2
#                                            where count is NOT NULL")

mobile_external_id_appgtv <- arrange(summarize(groupBy(appgtv, "post_mobiledevice"), count = countDistinct(appgtv$external_id_post_evar)), "count", decreasing = T) # conto quanti "post_mobiledevice" vengono utilizzati da quanti external id

os_external_id_appgtv <- arrange(summarize(groupBy(appgtv, "mapped_os_value"), count = countDistinct(appgtv$external_id_post_evar)), "count", decreasing = T) # conto quanti diversi sistemi operativi vengono utilizzati da quanti external id

# riconduco i vari tipi di device a mobile o desktop

createOrReplaceTempView(os_external_id_appgtv, 'os_external_id_appgtv')

mobile_desktop_appgtv <- sql('SELECT *,
                             CASE WHEN (mapped_os_value LIKE "Linux%" OR mapped_os_value LIKE "Window%" OR mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh") THEN "desktop"
                             WHEN (mapped_os_value LIKE "Firefox%" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Chrome%" OR mapped_os_value LIKE "Tizen%" OR mapped_os_value LIKE "%Mobile%" OR mapped_os_value LIKE "Android%") THEN "mobile" ELSE "other" END AS device_type
                             FROM os_external_id_appgtv')

device_type_appgtv <- arrange(summarize(groupBy(mobile_desktop_appgtv,"device_type"), sum = sum(mobile_desktop_appgtv$count)), "sum", decreasing=T) # conto tipo device totale appgtv

# riconduco i vari tipi di sistema operativo a linux, windows, apple, other

createOrReplaceTempView(os_external_id_appgtv,'os_external_id_appgtv')

os_appgtv <- sql('SELECT *,
                 CASE WHEN (mapped_os_value LIKE "Linux%") THEN "linux"
                 WHEN (mapped_os_value LIKE "Window%") THEN "windows"
                 WHEN (mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Mobile iOS%") THEN "apple"
                 WHEN (mapped_os_value LIKE "Android%") THEN "android" ELSE "other" END AS os
                 FROM os_external_id_appgtv')

os_type_appgtv <- arrange(summarize(groupBy(os_appgtv, "os"),sum=sum(os_appgtv$count)), "sum", decreasing=T)

last_touch_appgtv <- arrange(summarize(groupBy(appgtv, 'last_touch_label'), count=countDistinct(appgtv$external_id_post_evar)), "count", decreasing=T) # calcolo lista last touch point per identificare chi proviene da social network


############################# APP SPORT ################################

skyappsport <- read.parquet("/STAGE/adobe/reportsuite=skyappsport.prod")

skyappsport_1 <- select(skyappsport, "external_id_post_evar", 
                        "post_visid_concatenated", 
                        "visit_num", 
                        "date_time",
                        "post_channel",
                        "site_section_post_evar",
                        "post_page_event_value",
                        "post_mobiledevice",
                        "post_campaign",
                        "va_closer_id",
                        "mapped_connection_type_value",
                        "mapped_browser_type_value",
                        "mapped_country_value",
                        "mapped_language_value",
                        "mapped_os_value",
                        "mapped_resolution_value",
                        "mapped_browser_value",
                        "paid_search"
)

skyappsport_2 <- withColumn(skyappsport_1, "date_time_dt", cast(skyappsport_1$date_time, "date"))
skyappsport_3 <- withColumn(skyappsport_2, "date_time_ts", cast(skyappsport_2$date_time, "timestamp"))
skyappsport_4 <- filter(skyappsport_3, "external_id_post_evar is not NULL")

# filtro temporale:

skyappsport_5 <- filter(skyappsport_4, skyappsport_4$date_time_dt >= data_inizio) 
skyappsport_6 <- filter(skyappsport_5, skyappsport_5$date_time_dt <= data_fine)

#label colonna last_touch_point:
skyappsport_6$last_touch_label <- ifelse(skyappsport_6$va_closer_id == 1, "editorial_recommendation",
                                         ifelse(skyappsport_6$va_closer_id == 2, "search_engine_organic",
                                                ifelse(skyappsport_6$va_closer_id == 3, "internal_referrer_w",
                                                       ifelse(skyappsport_6$va_closer_id == 4, "social_network",
                                                              ifelse(skyappsport_6$va_closer_id == 5, "direct_bookmark",
                                                                     ifelse(skyappsport_6$va_closer_id == 6, "referring_domains",
                                                                            ifelse(skyappsport_6$va_closer_id == 7, "paid_display",
                                                                                   ifelse(skyappsport_6$va_closer_id == 8, "paid_search",
                                                                                          ifelse(skyappsport_6$va_closer_id == 9, "paid_other",
                                                                                                 ifelse(skyappsport_6$va_closer_id == 10, "retargeting",
                                                                                                        ifelse(skyappsport_6$va_closer_id == 11, "social_player",
                                                                                                               ifelse(skyappsport_6$va_closer_id == 12, "direct_email_marketing",
                                                                                                                      ifelse(skyappsport_6$va_closer_id == 13, "paid_social",
                                                                                                                             ifelse(skyappsport_6$va_closer_id == 14, "newsletter",
                                                                                                                                    ifelse(skyappsport_6$va_closer_id == 15, "acquisition",
                                                                                                                                           ifelse(skyappsport_6$va_closer_id == 16, "AMP_w",
                                                                                                                                                  ifelse(skyappsport_6$va_closer_id == 17, "web_push_notification",
                                                                                                                                                         ifelse(skyappsport_6$va_closer_id == 18, "DEM_w",
                                                                                                                                                                ifelse(skyappsport_6$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                       ifelse(skyappsport_6$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                              ifelse(skyappsport_6$va_closer_id == 21, "internal",
                                                                                                                                                                                     ifelse(skyappsport_6$va_closer_id == 22, "affiliation",
                                                                                                                                                                                            ifelse(skyappsport_6$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                                   ifelse(skyappsport_6$va_closer_id == 24, "short_url",
                                                                                                                                                                                                          ifelse(skyappsport_6$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

# PER APP Sport VA FATTO IL DIZIONARIO E IL JOIN

skyappsport_diz <- distinct(select(skyappsport, "external_id_post_evar", "post_visid_concatenated"))

skyappsport_diz2 <- filter(skyappsport_diz, isNotNull(skyappsport_diz$external_id_post_evar))
skyappsport_diz3 <- filter(skyappsport_diz2, isNotNull(skyappsport_diz2$post_visid_concatenated))

write.parquet(skyappsport_diz3, path_diz_appsport)

skyappsport_diz5 <- read.parquet(path_diz_appsport)
skyappsport_diz6 <- withColumnRenamed(skyappsport_diz5, "external_id_post_evar", "skyid")
skyappsport_diz7 <- withColumnRenamed(skyappsport_diz6, "post_visid_concatenated", "cookieid")

createOrReplaceTempView(skyappsport_diz7, "skyappsport_diz7")
createOrReplaceTempView(skyappsport_6, "skyappsport_6")

join_skyappsport_diz <- sql("select skyappsport_6.*, skyappsport_diz7.skyid
                            from skyappsport_6
                            left join skyappsport_diz7 on skyappsport_6.post_visid_concatenated = skyappsport_diz7.cookieid ")

join_skyappsport_diz_2 <-  drop(join_skyappsport_diz,  "external_id_post_evar")
join_skyappsport_diz_2 <-  withColumnRenamed(join_skyappsport_diz_2, "skyid", "external_id_post_evar")

write.parquet(join_skyappsport_diz_2, path_scarico_skyappsport) # write scarico app wsc

appsport <- read.parquet(path_scarico_skyappsport) 

external_id_unici_appsport <- arrange(summarize(groupBy(appsport, "external_id_post_evar"), count = countDistinct(appsport$post_visid_concatenated)), "count", decreasing = T)

cookie_unici_appsport <- arrange(summarize(groupBy(appsport, "post_visid_concatenated"), count = countDistinct(appsport$visit_num)), "count", decreasing = T)

visite_uniche_appsport<- summarize(cookie_unici_appsport, sum = sum(cookie_unici_appsport$count))

page_view_appsport <- filter(appsport,'post_page_event_value == "page_view"')

video_view_appsport <- filter(appsport, 'post_page_event_value like "%media%"')

# cookie per external id con percentili

# createOrReplaceTempView(external_id_unici_appsport, "external_id_unici_appsport") 
# 
# percentili_cookie_appsport <- sql("select percentile_approx(count, 0.25) as q25,
#                                   percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                   percentile_approx(count, 0.75) as q75,
#                                   percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                   percentile_approx(count, 1) as max
#                                   from external_id_unici_appsport
#                                   where count is NOT NULL")

# percentili visite uniche per ext id

visite_uniche_appsport <- select(appsport,
                                 "external_id_post_evar",
                                 "post_visid_concatenated",
                                 "visit_num")

visite_uniche_appsport_conc <- arrange(summarize(groupBy(visite_uniche_appsport, "external_id_post_evar"), count = countDistinct(concat(visite_uniche_appsport$post_visid_concatenated, visite_uniche_appsport$visit_num))), "count", decreasing = T) # concatenazione visit num e cookie per ottenere numero visite e aggregazione per external_id

# createOrReplaceTempView(visite_uniche_appsport_conc, "visite_uniche_appsport_conc")
# 
# percentili_visite_appsport <- sql("select percentile_approx(count, 0.25) as q25,
#                                   percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                   percentile_approx(count, 0.75) as q75,
#                                   percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                   percentile_approx(count, 1) as max
#                                   from visite_uniche_appsport_conc
#                                   where count is NOT NULL")

page_view_per_external_id_appsport <- summarize(groupBy(page_view_appsport, "external_id_post_evar"), count = count(page_view_appsport$post_page_event_value)) # page views per external_id appsport

# createOrReplaceTempView(page_view_per_external_id_appsport, "page_view_per_external_id_appsport")
# 
# percentili_page_view_appsport <- sql("select percentile_approx(count, 0.25) as q25,
#                                      percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                      percentile_approx(count, 0.75) as q75,
#                                      percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                      max(count) as max,
#                                      mean(count) as mean
#                                      from page_view_per_external_id_appsport
#                                      where count is NOT NULL")

# video per external id con percentili:

video_per_external_id_appsport <- summarize(groupBy(video_view_appsport, "external_id_post_evar"), count = count(video_view_appsport$post_page_event_value))

# createOrReplaceTempView(video_per_external_id_appsport, "video_per_external_id_appsport")
# 
# percentili_video_appsport <- sql("select percentile_approx(count, 0.25) as q25,
#                                  percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                  percentile_approx(count, 0.75) as q75,
#                                  percentile_approx(count, 0.95) as q95, -- 95esimo percentile
#                                  max(count) as max
#                                  from video_per_external_id_appsport
#                                  where count is NOT NULL")

month_appsport <- withColumn(appsport, "month", month(appsport$date_time_dt)) # Aggiungo il mese a ciascun osservazione

visite_uniche_month_appsport <- summarize(groupBy(month_appsport, "month"), count = countDistinct(concat(month_appsport$post_visid_concatenated, month_appsport$visit_num))) # visite uniche per mese

external_id_month_appsport <- summarize(groupBy(month_appsport, "month"), count = countDistinct(month_appsport$external_id_post_evar)) # external id per mese al fine di calcolare la media visite per ogni mese (fatto su excel per comodit?? prendendo i risultati della tabella precedente e dividendoli per questa)

month_appsport_2 <- withColumn(month_appsport, "unique_visit", concat(month_appsport$post_visid_concatenated, month_appsport$visit_num)) # aggiungo la colonna visite uniche al dataset month_appsport

visite_uniche_month_external_id_appsport <- summarize(groupBy(month_appsport_2, "month", "external_id_post_evar"), count = countDistinct(month_appsport_2$unique_visit)) # calcolo le visite uniche per external id per mese al fine di calcolare la mediana di visite per ogni mese

# #va cambiato il numero '1' e runnare per ogni mese e.g: '1' per gennaio, '2' per febbraio, etc.
# visite_uniche_month_external_id_appsport_2 <- filter(visite_uniche_month_external_id_appsport , "month like '1'") 

# createOrReplaceTempView(visite_uniche_month_external_id_appsport_2, "visite_uniche_month_external_id_appsport_2")
# 
# percentili_month_external_id_appsport <- sql("select percentile_approx(count, 0.25) as q25,
#                                              percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                              percentile_approx(count, 0.75) as q75,
#                                              percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                              percentile_approx(count, 1) as max
#                                              from visite_uniche_month_external_id_appsport_2
#                                              where count is NOT NULL")

mobile_external_id_appsport <- arrange(summarize(groupBy(appsport, "post_mobiledevice"), count = countDistinct(appsport$external_id_post_evar)), "count", decreasing = T) # conto quanti "post_mobiledevice" vengono utilizzati da quanti external id

os_external_id_appsport <- arrange(summarize(groupBy(appsport, "mapped_os_value"), count = countDistinct(appsport$external_id_post_evar)), "count", decreasing = T) # conto quanti diversi sistemi operativi vengono utilizzati da quanti external id

# riconduco i vari tipi di device a mobile o desktop

createOrReplaceTempView(os_external_id_appsport, 'os_external_id_appsport')

mobile_desktop_appsport <- sql('SELECT *,
                               CASE WHEN (mapped_os_value LIKE "Linux%" OR mapped_os_value LIKE "Window%" OR mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh") THEN "desktop"
                               WHEN (mapped_os_value LIKE "Firefox%" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Chrome%" OR mapped_os_value LIKE "Tizen%" OR mapped_os_value LIKE "%Mobile%" OR mapped_os_value LIKE "Android%") THEN "mobile" ELSE "other" END AS device_type
                               FROM os_external_id_appsport')

device_type_appsport <- arrange(summarize(groupBy(mobile_desktop_appsport,"device_type"), sum = sum(mobile_desktop_appsport$count)), "sum", decreasing=T) # conto tipo device totale appsport

# riconduco i vari tipi di sistema operativo a linux, windows, apple, other

createOrReplaceTempView(os_external_id_appsport,'os_external_id_appsport')

os_appsport <- sql('SELECT *,
                   CASE WHEN (mapped_os_value LIKE "Linux%") THEN "linux"
                   WHEN (mapped_os_value LIKE "Window%") THEN "windows"
                   WHEN (mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Mobile iOS%") THEN "apple"
                   WHEN (mapped_os_value LIKE "Android%") THEN "android" ELSE "other" END AS os
                   FROM os_external_id_appsport')

os_type_appsport <- arrange(summarize(groupBy(os_appsport, "os"),sum=sum(os_appsport$count)), "sum", decreasing=T)

last_touch_appsport <- arrange(summarize(groupBy(appsport, 'last_touch_label'), count=countDistinct(appsport$external_id_post_evar)), "count", decreasing=T) # calcolo lista last touch point per identificare chi proviene da social network


############################# APP XFACTOR ################################

skyappxfactor <- read.parquet("/STAGE/adobe/reportsuite=skyappxfactor.prod")

skyappxfactor_1 <- select(skyappxfactor, "external_id_post_evar", 
                          "post_visid_concatenated", 
                          "visit_num", 
                          "date_time",
                          "post_channel",
                          "site_section_post_evar",
                          "post_page_event_value",
                          "post_mobiledevice",
                          "post_campaign",
                          "va_closer_id",
                          "mapped_connection_type_value",
                          "mapped_browser_type_value",
                          "mapped_country_value",
                          "mapped_language_value",
                          "mapped_os_value",
                          "mapped_resolution_value",
                          "mapped_browser_value",
                          "paid_search"
)

skyappxfactor_2 <- withColumn(skyappxfactor_1, "date_time_dt", cast(skyappxfactor_1$date_time, "date"))
skyappxfactor_3 <- withColumn(skyappxfactor_2, "date_time_ts", cast(skyappxfactor_2$date_time, "timestamp"))
skyappxfactor_4 <- filter(skyappxfactor_3, "external_id_post_evar is not NULL")

# filtro temporale:

skyappxfactor_5 <- filter(skyappxfactor_4, skyappxfactor_4$date_time_dt >= data_inizio) 
skyappxfactor_6 <- filter(skyappxfactor_5, skyappxfactor_5$date_time_dt <= data_fine)

#label colonna last_touch_point:
skyappxfactor_6$last_touch_label <- ifelse(skyappxfactor_6$va_closer_id == 1, "editorial_recommendation",
                                           ifelse(skyappxfactor_6$va_closer_id == 2, "search_engine_organic",
                                                  ifelse(skyappxfactor_6$va_closer_id == 3, "internal_referrer_w",
                                                         ifelse(skyappxfactor_6$va_closer_id == 4, "social_network",
                                                                ifelse(skyappxfactor_6$va_closer_id == 5, "direct_bookmark",
                                                                       ifelse(skyappxfactor_6$va_closer_id == 6, "referring_domains",
                                                                              ifelse(skyappxfactor_6$va_closer_id == 7, "paid_display",
                                                                                     ifelse(skyappxfactor_6$va_closer_id == 8, "paid_search",
                                                                                            ifelse(skyappxfactor_6$va_closer_id == 9, "paid_other",
                                                                                                   ifelse(skyappxfactor_6$va_closer_id == 10, "retargeting",
                                                                                                          ifelse(skyappxfactor_6$va_closer_id == 11, "social_player",
                                                                                                                 ifelse(skyappxfactor_6$va_closer_id == 12, "direct_email_marketing",
                                                                                                                        ifelse(skyappxfactor_6$va_closer_id == 13, "paid_social",
                                                                                                                               ifelse(skyappxfactor_6$va_closer_id == 14, "newsletter",
                                                                                                                                      ifelse(skyappxfactor_6$va_closer_id == 15, "acquisition",
                                                                                                                                             ifelse(skyappxfactor_6$va_closer_id == 16, "AMP_w",
                                                                                                                                                    ifelse(skyappxfactor_6$va_closer_id == 17, "web_push_notification",
                                                                                                                                                           ifelse(skyappxfactor_6$va_closer_id == 18, "DEM_w",
                                                                                                                                                                  ifelse(skyappxfactor_6$va_closer_id == 19, "mobile_CPC_w",
                                                                                                                                                                         ifelse(skyappxfactor_6$va_closer_id == 20, "real_time_bidding",
                                                                                                                                                                                ifelse(skyappxfactor_6$va_closer_id == 21, "internal",
                                                                                                                                                                                       ifelse(skyappxfactor_6$va_closer_id == 22, "affiliation",
                                                                                                                                                                                              ifelse(skyappxfactor_6$va_closer_id == 23, "unknown_channel",
                                                                                                                                                                                                     ifelse(skyappxfactor_6$va_closer_id == 24, "short_url",
                                                                                                                                                                                                            ifelse(skyappxfactor_6$va_closer_id == 25, "IOL_W", "none")))))))))))))))))))))))))

# PER APP XFACTOR VA FATTO IL DIZIONARIO E IL JOIN

skyappxfactor_diz <- distinct(select(skyappxfactor, "external_id_post_evar", "post_visid_concatenated"))

skyappxfactor_diz2 <- filter(skyappxfactor_diz, isNotNull(skyappxfactor_diz$external_id_post_evar))
skyappxfactor_diz3 <- filter(skyappxfactor_diz2, isNotNull(skyappxfactor_diz2$post_visid_concatenated))

write.parquet(skyappxfactor_diz3, path_diz_appxfactor)


skyappxfactor_diz5 <- read.parquet(path_diz_appxfactor)

skyappxfactor_diz6 <- withColumnRenamed(skyappxfactor_diz5, "external_id_post_evar", "skyid")
skyappxfactor_diz7 <- withColumnRenamed(skyappxfactor_diz6, "post_visid_concatenated", "cookieid")

createOrReplaceTempView(skyappxfactor_diz7, "skyappxfactor_diz7")
createOrReplaceTempView(skyappxfactor_6, "skyappxfactor_6")

join_skyappxfactor_diz <- sql("select skyappxfactor_6.*, skyappxfactor_diz7.skyid
                              from skyappxfactor_6
                              left join skyappxfactor_diz7 on skyappxfactor_6.post_visid_concatenated = skyappxfactor_diz7.cookieid ")

join_skyappxfactor_diz_2 <-  drop(join_skyappxfactor_diz,  "external_id_post_evar")
join_skyappxfactor_diz_2 <-  withColumnRenamed(join_skyappxfactor_diz_2, "skyid", "external_id_post_evar")

write.parquet(join_skyappxfactor_diz_2, path_scarico_skyappxfactor) # write scarico app wsc

appxfactor <- read.parquet(path_scarico_skyappxfactor) 

external_id_unici_appxfactor <- arrange(summarize(groupBy(appxfactor, "external_id_post_evar"), count = countDistinct(appxfactor$post_visid_concatenated)), "count", decreasing = T)

cookie_unici_appxfactor <- arrange(summarize(groupBy(appxfactor, "post_visid_concatenated"), count = countDistinct(appxfactor$visit_num)), "count", decreasing = T)

visite_uniche_appxfactor<- summarize(cookie_unici_appxfactor, sum = sum(cookie_unici_appxfactor$count))

page_view_appxfactor <- filter(appxfactor,'post_page_event_value == "page_view"')

video_view_appxfactor <- filter(appxfactor, 'post_page_event_value like "%media%"')

# cookie per external id con percentili

# createOrReplaceTempView(external_id_unici_appxfactor, "external_id_unici_appxfactor") 
# 
# percentili_cookie_appxfactor <- sql("select percentile_approx(count, 0.25) as q25,
#                                     percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                     percentile_approx(count, 0.75) as q75,
#                                     percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                     percentile_approx(count, 1) as max
#                                     from external_id_unici_appxfactor
#                                     where count is NOT NULL")

# percentili visite uniche per ext id

visite_uniche_appxfactor <- select(appxfactor,
                                   "external_id_post_evar",
                                   "post_visid_concatenated",
                                   "visit_num")

visite_uniche_appxfactor_conc <- arrange(summarize(groupBy(visite_uniche_appxfactor, "external_id_post_evar"), count = countDistinct(concat(visite_uniche_appxfactor$post_visid_concatenated, visite_uniche_appxfactor$visit_num))), "count", decreasing = T) # concatenazione visit num e cookie per ottenere numero visite e aggregazione per external_id

# createOrReplaceTempView(visite_uniche_appxfactor_conc, "visite_uniche_appxfactor_conc")
# 
# percentili_visite_appxfactor <- sql("select percentile_approx(count, 0.25) as q25,
#                                     percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                     percentile_approx(count, 0.75) as q75,
#                                     percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                     percentile_approx(count, 1) as max
#                                     from visite_uniche_appxfactor_conc
#                                     where count is NOT NULL")

page_view_per_external_id_appxfactor <- summarize(groupBy(page_view_appxfactor, "external_id_post_evar"), count = count(page_view_appxfactor$post_page_event_value)) # page views per external_id appxfactor

# createOrReplaceTempView(page_view_per_external_id_appxfactor, "page_view_per_external_id_appxfactor")
# 
# percentili_page_view_appxfactor <- sql("select percentile_approx(count, 0.25) as q25,
#                                        percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                        percentile_approx(count, 0.75) as q75,
#                                        percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                        max(count) as max,
#                                        mean(count) as mean
#                                        from page_view_per_external_id_appxfactor
#                                        where count is NOT NULL")

# video per external id con percentili:

video_per_external_id_appxfactor <- summarize(groupBy(video_view_appxfactor, "external_id_post_evar"), count = count(video_view_appxfactor$post_page_event_value))

# createOrReplaceTempView(video_per_external_id_appxfactor, "video_per_external_id_appxfactor")
# 
# percentili_video_appxfactor <- sql("select percentile_approx(count, 0.25) as q25,
#                                    percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                    percentile_approx(count, 0.75) as q75,
#                                    percentile_approx(count, 0.95) as q95, -- 95esimo percentile
#                                    max(count) as max
#                                    from video_per_external_id_appxfactor
#                                    where count is NOT NULL")

month_appxfactor <- withColumn(appxfactor, "month", month(appxfactor$date_time_dt)) # Aggiungo il mese a ciascun osservazione

visite_uniche_month_appxfactor <- summarize(groupBy(month_appxfactor, "month"), count = countDistinct(concat(month_appxfactor$post_visid_concatenated, month_appxfactor$visit_num))) # visite uniche per mese

external_id_month_appxfactor <- summarize(groupBy(month_appxfactor, "month"), count = countDistinct(month_appxfactor$external_id_post_evar)) # external id per mese al fine di calcolare la media visite per ogni mese (fatto su excel per comodit?? prendendo i risultati della tabella precedente e dividendoli per questa)

month_appxfactor_2 <- withColumn(month_appxfactor, "unique_visit", concat(month_appxfactor$post_visid_concatenated, month_appxfactor$visit_num)) # aggiungo la colonna visite uniche al dataset month_appxfactor

visite_uniche_month_external_id_appxfactor <- summarize(groupBy(month_appxfactor_2, "month", "external_id_post_evar"), count = countDistinct(month_appxfactor_2$unique_visit)) # calcolo le visite uniche per external id per mese al fine di calcolare la mediana di visite per ogni mese

# #va cambiato il numero '1' e runnare per ogni mese e.g: '1' per gennaio, '2' per febbraio, etc.
# visite_uniche_month_external_id_appxfactor_2 <- filter(visite_uniche_month_external_id_appxfactor , "month like '1'") 

# createOrReplaceTempView(visite_uniche_month_external_id_appxfactor_2, "visite_uniche_month_external_id_appxfactor_2")
# 
# percentili_month_external_id_appxfactor <- sql("select percentile_approx(count, 0.25) as q25,
#                                                percentile_approx(count, 0.50) as mediana, -- q2 = mediana
#                                                percentile_approx(count, 0.75) as q75,
#                                                percentile_approx(count, 0.95) as p95, -- 95esimo percentile
#                                                percentile_approx(count, 1) as max
#                                                from visite_uniche_month_external_id_appxfactor_2
#                                                where count is NOT NULL")

mobile_external_id_appxfactor <- arrange(summarize(groupBy(appxfactor, "post_mobiledevice"), count = countDistinct(appxfactor$external_id_post_evar)), "count", decreasing = T) # conto quanti "post_mobiledevice" vengono utilizzati da quanti external id

os_external_id_appxfactor <- arrange(summarize(groupBy(appxfactor, "mapped_os_value"), count = countDistinct(appxfactor$external_id_post_evar)), "count", decreasing = T) # conto quanti diversi sistemi operativi vengono utilizzati da quanti external id

# riconduco i vari tipi di device a mobile o desktop

createOrReplaceTempView(os_external_id_appxfactor, 'os_external_id_appxfactor')

mobile_desktop_appxfactor <- sql('SELECT *,
                                 CASE WHEN (mapped_os_value LIKE "Linux%" OR mapped_os_value LIKE "Window%" OR mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh") THEN "desktop"
                                 WHEN (mapped_os_value LIKE "Firefox%" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Chrome%" OR mapped_os_value LIKE "Tizen%" OR mapped_os_value LIKE "%Mobile%" OR mapped_os_value LIKE "Android%") THEN "mobile" ELSE "other" END AS device_type
                                 FROM os_external_id_appxfactor')

device_type_appxfactor <- arrange(summarize(groupBy(mobile_desktop_appxfactor,"device_type"), sum = sum(mobile_desktop_appxfactor$count)), "sum", decreasing=T) # conto tipo device totale appxfactor

# riconduco i vari tipi di sistema operativo a linux, windows, apple, other

createOrReplaceTempView(os_external_id_appxfactor,'os_external_id_appxfactor')

os_appxfactor <- sql('SELECT *,
                     CASE WHEN (mapped_os_value LIKE "Linux%") THEN "linux"
                     WHEN (mapped_os_value LIKE "Window%") THEN "windows"
                     WHEN (mapped_os_value LIKE "OS%" OR mapped_os_value LIKE "Macintosh" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Mobile iOS%") THEN "apple"
                     WHEN (mapped_os_value LIKE "Android%") THEN "android" ELSE "other" END AS os
                     FROM os_external_id_appxfactor')

os_type_appxfactor <- arrange(summarize(groupBy(os_appxfactor, "os"),sum=sum(os_appxfactor$count)), "sum", decreasing=T)

last_touch_appxfactor <- arrange(summarize(groupBy(appxfactor, 'last_touch_label'), count=countDistinct(appxfactor$external_id_post_evar)), "count", decreasing=T) # calcolo lista last touch point per identificare chi proviene da social network







