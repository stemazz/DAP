
## 03_kpi ##

source("connection_R.R")
options(scipen = 10000)



df_variabile_7 <- read.parquet(path_df_variabile_7) # ultimo df da script 2

createOrReplaceTempView(df_variabile_7, "df_variabile_7")

##### CREAZIONE VARIABILI 38-44: media visite reportsuite per external id ####

df_variabile_8 <- withColumn(df_variabile_7, "media_visite_corporate", round(df_variabile_7$visite_totali_corporate / df_variabile_7$numero_mesi_corporate))

df_variabile_8 <- withColumn(df_variabile_8, "media_visite_tg24", round(df_variabile_8$visite_totali_tg24 / df_variabile_8$numero_mesi_tg24))

df_variabile_8 <- withColumn(df_variabile_8, "media_visite_sport", round(df_variabile_8$visite_totali_sport / df_variabile_8$numero_mesi_sport))

df_variabile_8 <- withColumn(df_variabile_8, "media_visite_appwsc", round(df_variabile_8$visite_totali_appwsc / df_variabile_8$numero_mesi_appwsc))

df_variabile_8 <- withColumn(df_variabile_8, "media_visite_appgtv", round(df_variabile_8$visite_totali_appgtv / df_variabile_8$numero_mesi_appgtv))

df_variabile_8 <- withColumn(df_variabile_8, "media_visite_appsport", round(df_variabile_8$visite_totali_appsport / df_variabile_8$numero_mesi_appsport))

df_variabile_8 <- withColumn(df_variabile_8, "media_visite_appxfactor", round(df_variabile_8$visite_totali_appxfactor / df_variabile_8$numero_mesi_appxfactor))

write.parquet(df_variabile_8, path_df_variabile_8)

df_variabile_8 <- read.parquet(path_df_variabile_8)

createOrReplaceTempView(df_variabile_8, "df_variabile_8")

##### CREAZIONE VARIABILI 45-47: 'percentuale_visite_mobile', 'percentuale_visite_desktop', 'percentuale_visite_other' #### 

totale_visite <- sql('select DISTINCT *, (count_visits_mobile + count_visits_desktop + count_visits_other) as totale_visite 
                     from df_variabile_8')

createOrReplaceTempView(totale_visite,'totale_visite')

df_variabile_9 <- sql('SELECT *,
                      round(count_visits_mobile/totale_visite,2) AS percentuale_mobile,
                      round(count_visits_desktop/totale_visite,2) AS percentuale_desktop,
                      round(count_visits_other/totale_visite,2) AS percentuale_other
                      FROM totale_visite')

df_variabile_9$percentuale_mobile <- ifelse((isNull(df_variabile_9$percentuale_mobile) & isNull(df_variabile_9$percentuale_desktop) & isNull(df_variabile_9$percentuale_other)), 
                                            round(1,1), df_variabile_9$percentuale_mobile)

df_variabile_9$percentuale_desktop <- ifelse(isNull(df_variabile_9$percentuale_desktop), round(0,1), df_variabile_9$percentuale_desktop)

df_variabile_9$percentuale_other <- ifelse(isNull(df_variabile_9$percentuale_other), round(0,1), df_variabile_9$percentuale_other)

write.parquet(df_variabile_9, path_df_variabile_9)

df_variabile_9 <- read.parquet(path_df_variabile_9)

##### CREAZIONE VARIABILI 47-54: durata media visite su reportsuite per external id #### 

df_variabile_10 <- withColumn(df_variabile_9, "durata_media_visite_corporate", round(df_variabile_9$secondi_totali_corporate / df_variabile_9$visite_totali_corporate))

df_variabile_10 <- withColumn(df_variabile_10, "durata_media_visite_tg24", round(df_variabile_10$secondi_totali_tg24 / df_variabile_10$visite_totali_tg24))

df_variabile_10 <- withColumn(df_variabile_10, "durata_media_visite_sport", round(df_variabile_10$secondi_totali_sport / df_variabile_10$visite_totali_sport))

df_variabile_10 <- withColumn(df_variabile_10, "durata_media_visite_appwsc", round(df_variabile_10$secondi_totali_appwsc / df_variabile_10$visite_totali_appwsc))

df_variabile_10 <- withColumn(df_variabile_10, "durata_media_visite_appgtv", round(df_variabile_10$secondi_totali_appgtv / df_variabile_10$visite_totali_appgtv))

df_variabile_10 <- withColumn(df_variabile_10, "durata_media_visite_appsport", round(df_variabile_10$secondi_totali_appsport / df_variabile_10$visite_totali_appsport))

df_variabile_10 <- withColumn(df_variabile_10, "durata_media_visite_appxfactor", round(df_variabile_10$secondi_totali_appxfactor / df_variabile_10$visite_totali_appxfactor))

write.parquet(df_variabile_10, path_df_variabile_10)

df_variabile_10 <- read.parquet(path_df_variabile_10)

createOrReplaceTempView(df_variabile_10, "df_variabile_10")

##### Preparazione variabili 55-61: ricorrenza su varie reportsuite ####

# N.B: 'corporate_2' e molti altri df sono stati creati nello script 2_digitalizzazione_variabili_1_37_adobe. Quindi ?? necessario riprenderli da l??

corporate_ric <- corporate_2

#Indice: 0 ='non trovato', 1 ='trovato poco', 2 ='trovato tanto'.
corporate_ric$ricorrenza <- ifelse((corporate_2$count > 2), 2, 
                                   ifelse((corporate_2$count>=1) & (corporate_2$count<=2), 1, 0)) 

# df con external id e somma ricorrenza [valore massimo: 12]
corporate_ric <- summarize(groupBy(corporate_ric, corporate_ric$external_id_post_evar), 
                           sommaRicorrenza = sum(corporate_ric$ricorrenza)) 

#Indice: sommaRicorrenza = 12 allora e' entrato almeno 2 volte per 6 mesi --> ricorrente.
corporate_ric$ricorrenza_corporate <- ifelse(corporate_ric$sommaRicorrenza == 12, 1, 0) 


tg24_ric <- tg24_2

#Indice: 0 ='non trovato', 1 ='trovato poco', 2 ='trovato tanto'.
tg24_ric$ricorrenza <- ifelse((tg24_2$count > 2), 2, ifelse((tg24_2$count>=1) & (tg24_2$count<=2), 1, 0)) 

# df con external id e somma ricorrenza [valore massimo: 12]
tg24_ric <- summarize(groupBy(tg24_ric, tg24_ric$external_id_post_evar), 
                      sommaRicorrenza = sum(tg24_ric$ricorrenza)) 

#Indice: sommaRicorrenza = 12 allora e' entrato almeno 2 volte per 6 mesi --> ricorrente.
tg24_ric$ricorrenza_tg24 <- ifelse(tg24_ric$sommaRicorrenza == 12, 1, 0) 


sport_ric <- sport_2

#Indice: 0 ='non trovato', 1 ='trovato poco', 2 ='trovato tanto'.
sport_ric$ricorrenza <- ifelse((sport_2$count > 2), 2, ifelse((sport_2$count>=1) & (sport_2$count<=2), 1, 0)) 

# df con external id e somma ricorrenza [valore massimo: 12]
sport_ric <- summarize(groupBy(sport_ric, sport_ric$external_id_post_evar), 
                       sommaRicorrenza = sum(sport_ric$ricorrenza)) 

#Indice: sommaRicorrenza = 12 allora e' entrato almeno 2 volte per 6 mesi --> ricorrente.
sport_ric$ricorrenza_sport <- ifelse(sport_ric$sommaRicorrenza == 12, 1, 0) 


appwsc_ric <- appwsc_2

#Indice: 0 ='non trovato', 1 ='trovato poco', 2 ='trovato tanto'.
appwsc_ric$ricorrenza <- ifelse((appwsc_2$count > 2), 2, ifelse((appwsc_2$count>=1) & (appwsc_2$count<=2), 1, 0)) 

# df con external id e somma ricorrenza [valore massimo: 12]
appwsc_ric <- summarize(groupBy(appwsc_ric, appwsc_ric$external_id_post_evar), 
                                sommaRicorrenza = sum(appwsc_ric$ricorrenza)) 

#Indice: sommaRicorrenza = 12 allora e' entrato almeno 2 volte per 6 mesi --> ricorrente.
appwsc_ric$ricorrenza_appwsc <- ifelse(appwsc_ric$sommaRicorrenza == 12, 1, 0) 


appgtv_ric <- appgtv_2

#Indice: 0 ='non trovato', 1 ='trovato poco', 2 ='trovato tanto'.
appgtv_ric$ricorrenza <- ifelse((appgtv_2$count > 2), 2, ifelse((appgtv_2$count>=1) & (appgtv_2$count<=2), 1, 0)) 

# df con external id e somma ricorrenza [valore massimo: 12]
appgtv_ric <- summarize(groupBy(appgtv_ric, appgtv_ric$external_id_post_evar), 
                                sommaRicorrenza = sum(appgtv_ric$ricorrenza)) 

#Indice: sommaRicorrenza = 12 allora e' entrato almeno 2 volte per 6 mesi --> ricorrente.
appgtv_ric$ricorrenza_appgtv <- ifelse(appgtv_ric$sommaRicorrenza == 12, 1, 0) 


appsport_ric <- appsport_2

#Indice: 0 ='non trovato', 1 ='trovato poco', 2 ='trovato tanto'.
appsport_ric$ricorrenza <- ifelse((appsport_2$count > 2), 2, ifelse((appsport_2$count>=1) & (appsport_2$count<=2), 1, 0)) 

# df con external id e somma ricorrenza [valore massimo: 12]
appsport_ric <- summarize(groupBy(appsport_ric, appsport_ric$external_id_post_evar), 
                                  sommaRicorrenza = sum(appsport_ric$ricorrenza)) 

#Indice: sommaRicorrenza = 12 allora e' entrato almeno 2 volte per 6 mesi --> ricorrente.
appsport_ric$ricorrenza_appsport <- ifelse(appsport_ric$sommaRicorrenza == 12, 1, 0) 


appxfactor_ric <- appxfactor_2

#Indice: 0 ='non trovato', 1 ='trovato poco', 2 ='trovato tanto'.
appxfactor_ric$ricorrenza <- ifelse((appxfactor_2$count > 2), 2, ifelse((appxfactor_2$count>=1) & (appxfactor_2$count<=2), 1, 0)) 

# df con external id e somma ricorrenza [valore massimo: 12]
appxfactor_ric <- summarize(groupBy(appxfactor_ric, appxfactor_ric$external_id_post_evar), 
                                    sommaRicorrenza = sum(appxfactor_ric$ricorrenza)) 

#Indice: sommaRicorrenza = 6 allora e' entrato almeno 2 volte per 3 mesi --> ricorrente.
appxfactor_ric$ricorrenza_appxfactor <- ifelse(appxfactor_ric$sommaRicorrenza == 6, 1, 0) 


createOrReplaceTempView(corporate_ric,'corporate_ric')

createOrReplaceTempView(tg24_ric,'tg24_ric')

createOrReplaceTempView(sport_ric,'sport_ric')

createOrReplaceTempView(appwsc_ric,'appwsc_ric')

createOrReplaceTempView(appgtv_ric,'appgtv_ric')

createOrReplaceTempView(appsport_ric,'appsport_ric')

createOrReplaceTempView(appxfactor_ric,'appxfactor_ric')

##### CREAZIONE VARIABILE 55: 'ricorrenza_corporate' ####

join1_ric <- sql('SELECT DISTINCT DF.*, C.ricorrenza_corporate
                 FROM df_variabile_10 DF
                 LEFT JOIN corporate_ric C
                 ON DF.ext_id= C.external_id_post_evar')

createOrReplaceTempView(join1_ric,'join1_ric')

##### CREAZIONE VARIABILE 56: 'ricorrenza_tg24' ####

join2_ric <- sql('SELECT DISTINCT J.*, T.ricorrenza_tg24
                 FROM join1_ric J
                 LEFT JOIN tg24_ric T
                 ON J.ext_id= T.external_id_post_evar')

createOrReplaceTempView(join2_ric, 'join2_ric')

##### CREAZIONE VARIABILE 57: 'ricorrenza_sport' ####

join3_ric <- sql('SELECT DISTINCT J2.*, S.ricorrenza_sport
                 FROM join2_ric J2
                 LEFT JOIN sport_ric S
                 ON J2.ext_id= S.external_id_post_evar')

createOrReplaceTempView(join3_ric, 'join3_ric')

##### CREAZIONE VARIABILE 58: 'ricorrenza_appwsc' ####

join4_ric <- sql('SELECT DISTINCT J3.*, W.ricorrenza_appwsc
                 FROM join3_ric J3
                 LEFT JOIN appwsc_ric W
                 ON J3.ext_id = W.external_id_post_evar')

createOrReplaceTempView(join4_ric, 'join4_ric')

##### CREAZIONE VARIABILE 59: 'ricorrenza_appgtv' ####

join5_ric <- sql('SELECT DISTINCT J4.*, G.ricorrenza_appgtv
                 FROM join4_ric J4
                 LEFT JOIN appgtv_ric G
                 ON J4.ext_id = G.external_id_post_evar')

createOrReplaceTempView(join5_ric, 'join5_ric')

##### CREAZIONE VARIABILE 60: 'ricorrenza_appsport' ####

join6_ric <- sql('SELECT DISTINCT J5.*, ASP.ricorrenza_appsport
                 FROM join5_ric J5
                 LEFT JOIN appsport_ric ASP
                 ON J5.ext_id = ASP.external_id_post_evar')

createOrReplaceTempView(join6_ric, 'join6_ric')

##### CREAZIONE VARIABILE 61: 'ricorrenza_appxfactor' ####


join7_ric <- sql('SELECT DISTINCT J6.*, XF.ricorrenza_appxfactor
                 FROM join6_ric J6
                 LEFT JOIN appxfactor_ric XF
                 ON J6.ext_id = XF.external_id_post_evar')

createOrReplaceTempView(join7_ric, 'join7_ric')

##### VARIABILE 62: 'ricorrenza_totale' ####

#faccio la somma delle colonne ricorrenza: se la somma e' >=1:

join7_ric <- fillna(join7_ric, 0, cols = "ricorrenza_corporate")
join7_ric <- fillna(join7_ric, 0, cols = "ricorrenza_tg24")
join7_ric <- fillna(join7_ric, 0, cols = "ricorrenza_sport")
join7_ric <- fillna(join7_ric, 0, cols = "ricorrenza_appwsc")
join7_ric <- fillna(join7_ric, 0, cols = "ricorrenza_appgtv")
join7_ric <- fillna(join7_ric, 0, cols = "ricorrenza_appsport")
join7_ric <- fillna(join7_ric, 0, cols = "ricorrenza_appxfactor")

createOrReplaceTempView(join7_ric, 'join7_ric')

# sommo tutte le ricorrenze
df_variabile_11 <- sql('SELECT DISTINCT *, (ricorrenza_corporate + ricorrenza_tg24 + ricorrenza_sport + ricorrenza_appgtv + 
                                  ricorrenza_appxfactor + ricorrenza_appsport + ricorrenza_appwsc) AS ricorrenza_totale 
                       FROM join7_ric') 

# ricorrenza totale su tutte le report suite
df_variabile_11$ricorrenza_totale <- ifelse(df_variabile_11$ricorrenza_totale > 0, 'ricorrente', 'non ricorrente') 

write.parquet(df_variabile_11, path_df_variabile_11)

df_variabile_11 <- read.parquet(path_df_variabile_11)

createOrReplaceTempView(df_variabile_11, "df_variabile_11")

##### PREPARAZIONE variabili 63-66: visite totali aggregate per corporate, wsc, guidatv; editoriale (tg24, sport, appsport, appxfactor); app (le 4 app); web (le 3 reportsuite di skyitdev) ####

# creo un subset per lavorare su calcoli pi?? leggeri
df_visite_aggregate <- select(df_variabile_11,
                              "ext_id",
                              "visite_totali_corporate",
                              "visite_totali_tg24",
                              "visite_totali_sport",
                              "visite_totali_appwsc",
                              "visite_totali_appgtv",
                              "visite_totali_appsport",
                              "visite_totali_appxfactor") 

df_visite_aggregate <- fillna(df_visite_aggregate, 0) # rimpiazzo gli NA con 0

createOrReplaceTempView(df_visite_aggregate, "df_visite_aggregate")

##### CREAZIONE VARIABILE 63: 'visite_corporate_appwsc_appgtv' #####

visite_corporate_appwsc_appgtv <- sql("select ext_id, sum (visite_totali_corporate + visite_totali_appwsc + visite_totali_appgtv) as visite_corporate_appwsc_appgtv
                                      from df_visite_aggregate
                                      group by ext_id") # somma visite totali su corporate_wsc_gtv per external id

createOrReplaceTempView(visite_corporate_appwsc_appgtv, "visite_corporate_appwsc_appgtv")

join1_aggregate <- sql("select DISTINCT DF.*, CAA.visite_corporate_appwsc_appgtv
                       from df_variabile_11 DF
                       left join visite_corporate_appwsc_appgtv CAA
                       on DF.ext_id = CAA.ext_id") # join con df 11 che contiene variabili 1-61

createOrReplaceTempView(join1_aggregate, "join1_aggregate")

##### CREAZIONE VARIABILE 64: 'visite_editoriale' #####

visite_editoriale <- sql("select ext_id, sum (visite_totali_tg24 + visite_totali_sport + visite_totali_appsport + visite_totali_appxfactor) as visite_editoriale
                         from df_visite_aggregate
                         group by ext_id") # somma visite totali su editoriale per external id

createOrReplaceTempView(visite_editoriale, "visite_editoriale")

join2_aggregate <- sql("select DISTINCT J.*, ED.visite_editoriale
                       from join1_aggregate J
                       left join visite_editoriale ED
                       on J.ext_id = ED.ext_id")

createOrReplaceTempView(join2_aggregate, "join2_aggregate")

##### CREAZIONE VARIABILE 65: 'visite_web' #####

visite_web <- sql("select ext_id, sum (visite_totali_corporate + visite_totali_tg24 + visite_totali_sport) as visite_web
                  from df_visite_aggregate
                  group by ext_id") # somma visite totali su web per external id

createOrReplaceTempView(visite_web, "visite_web")

join3_aggregate <- sql("select DISTINCT J2.*, WEB.visite_web
                       from join2_aggregate J2
                       left join visite_web WEB
                       on J2.ext_id = WEB.ext_id")

createOrReplaceTempView(join3_aggregate, "join3_aggregate")

##### CREAZIONE VARIABILE 66: 'visite_app' #####

visite_app <- sql("select ext_id, sum (visite_totali_appwsc + visite_totali_appgtv + visite_totali_appsport + visite_totali_appxfactor) as visite_app
                  from df_visite_aggregate
                  group by ext_id") # somma visite totali su app per external id

createOrReplaceTempView(visite_app, "visite_app")

join4_aggregate <- sql("select DISTINCT J3.*, APP.visite_app
                       from join3_aggregate J3
                       left join visite_app APP
                       on J3.ext_id = APP.ext_id")

write.parquet(join4_aggregate, path_df_variabile_12)

df_variabile_12 <- read.parquet(path_df_variabile_12)

createOrReplaceTempView(df_variabile_12, "df_variabile_12")

##### Preparazione variabili 67-70: numero massimo mesi in cui un external id ?? stato visto sui 4 aggregati ####

# creo un subset per lavorare su calcoli pi?? leggeri
df_mesi <-  select(df_variabile_12,
                   "ext_id",
                   "numero_mesi_corporate",
                   "numero_mesi_sport",
                   "numero_mesi_tg24",
                   "numero_mesi_appwsc",
                   "numero_mesi_appsport",
                   "numero_mesi_appgtv",
                   "numero_mesi_appxfactor") 

createOrReplaceTempView(df_mesi, "df_mesi")

# con la funzione greatest seleziono il numero pi?? alto tra i mesi in cui un ext id ?? stato visto. 
# Tutto questo mi serve per calcolare l'avg. visite mensili. Per non saper ne' leggere ne' scrivere lo tengo anche tra le KPI

max_mesi_corporate_appwsc_appgtv <- sql("select ext_id, greatest(numero_mesi_corporate, numero_mesi_appwsc, numero_mesi_appgtv) as max_mesi_corporate_appwsc_appgtv
                                        from df_mesi")

max_mesi_editoriale <- sql("select ext_id, greatest(numero_mesi_sport, numero_mesi_tg24, numero_mesi_appsport, numero_mesi_appxfactor) as max_mesi_editoriale
                           from df_mesi")

max_mesi_web <- sql("select ext_id, greatest(numero_mesi_corporate, numero_mesi_sport, numero_mesi_tg24) as max_mesi_web
                    from df_mesi")

max_mesi_app <- sql("select ext_id, greatest( numero_mesi_appwsc,  numero_mesi_appgtv, numero_mesi_appsport, numero_mesi_appxfactor) as max_mesi_app
                    from df_mesi")

createOrReplaceTempView(max_mesi_corporate_appwsc_appgtv, "max_mesi_corporate_appwsc_appgtv")
createOrReplaceTempView(max_mesi_editoriale, "max_mesi_editoriale")
createOrReplaceTempView(max_mesi_web, "max_mesi_web")
createOrReplaceTempView(max_mesi_app, "max_mesi_app")

##### CREAZIONE VARIABILE 67: 'max_mesi_corporate_appwsc_appgtv' ####

join1_mesi <- sql("select DISTINCT DF.*, MM.max_mesi_corporate_appwsc_appgtv
                  from df_variabile_12 DF
                  left join max_mesi_corporate_appwsc_appgtv MM
                  on DF.ext_id = MM.ext_id")

createOrReplaceTempView(join1_mesi, "join1_mesi")

##### CREAZIONE VARIABILE 68: 'max_mesi_editoriale' ####

join2_mesi <- sql("select DISTINCT J.*, MME.max_mesi_editoriale
                  from join1_mesi J
                  left join max_mesi_editoriale MME
                  on J.ext_id = MME.ext_id")

createOrReplaceTempView(join2_mesi, "join2_mesi")

##### CREAZIONE VARIABILE 69: 'max_mesi_web' ####

join3_mesi <- sql("select DISTINCT J2.*, MMW.max_mesi_web
                  from join2_mesi J2
                  left join max_mesi_web MMW
                  on J2.ext_id = MMW.ext_id")

createOrReplaceTempView(join3_mesi, "join3_mesi")

##### CREAZIONE VARIABILE 70: 'max_mesi_app' ####

join4_mesi <- sql("select DISTINCT J3.*, MMA.max_mesi_app
                  from join3_mesi J3
                  left join max_mesi_app MMA
                  on J3.ext_id = MMA.ext_id")


##### CREAZIONE VARIABILI 71-74: avg_visite_mese_corporate_appwsc_appgtv, avg_visite_mese_editoriale, avg_visite_mese_web, avg_visite_mese_app ####

# semplici medie delle visite mensili

df_variabile_13 <- withColumn(join4_mesi, "avg_visite_mese_corporate_appwsc_appgtv", 
                              join4_mesi$visite_corporate_appwsc_appgtv/join4_mesi$max_mesi_corporate_appwsc_appgtv)

df_variabile_13 <- withColumn(df_variabile_13, "avg_visite_mese_editoriale", 
                              df_variabile_13$visite_editoriale/df_variabile_13$max_mesi_editoriale)

df_variabile_13 <- withColumn(df_variabile_13, "avg_visite_mese_web", 
                              df_variabile_13$visite_web/df_variabile_13$max_mesi_web)

df_variabile_13 <- withColumn(df_variabile_13, "avg_visite_mese_app", 
                              df_variabile_13$visite_app/df_variabile_13$max_mesi_app)

write.parquet(df_variabile_13, path_df_variabile_13, mode = "overwrite")

df_variabile_13 <- read.parquet(path_df_variabile_13)

createOrReplaceTempView(df_variabile_13, "df_variabile_13")

##### Preparazione Variabili 75-78 ####

df_secondi <- select(df_variabile_13,
                     "ext_id",
                     "secondi_totali_corporate",
                     "secondi_totali_tg24",
                     "secondi_totali_sport",
                     "secondi_totali_appwsc",
                     "secondi_totali_appgtv",
                     "secondi_totali_appsport",
                     "secondi_totali_appxfactor",
                     "visite_corporate_appwsc_appgtv",
                     "visite_editoriale",
                     "visite_web",
                     "visite_app"
)

createOrReplaceTempView(df_secondi, "df_secondi")

df_secondi1 <- fillna(df_secondi, 0)

createOrReplaceTempView(df_secondi1, "df_secondi1")


##### CREAZIONE VARIABILE 75: 'secondi_corporate_appwsc_appgtv' ####

# somme dei secondi passati sui 4 diversi aggregati

secondi_corporate_appwsc_appgtv <- sql("select ext_id, 
                                              sum (secondi_totali_corporate + secondi_totali_appwsc + secondi_totali_appgtv) as secondi_corporate_appwsc_appgtv
                                       from df_secondi1
                                       group by ext_id")

createOrReplaceTempView(secondi_corporate_appwsc_appgtv , "secondi_corporate_appwsc_appgtv")

join1_secondi <- sql("select DISTINCT DF.*, SCAA.secondi_corporate_appwsc_appgtv
                     from df_variabile_13 DF
                     left join secondi_corporate_appwsc_appgtv SCAA 
                     on DF.ext_id = SCAA.ext_id")

createOrReplaceTempView(join1_secondi , "join1_secondi")

##### CREAZIONE VARIABILE 76: 'secondi_editoriale' ####

secondi_editoriale <- sql("select ext_id, sum (secondi_totali_tg24 + secondi_totali_sport + secondi_totali_appsport + secondi_totali_appxfactor) as secondi_editoriale
                          from df_secondi1
                          group by ext_id")

createOrReplaceTempView(secondi_editoriale , "secondi_editoriale")

join2_secondi <- sql("select DISTINCT J.*, SED.secondi_editoriale
                     from join1_secondi J
                     left join secondi_editoriale SED 
                     on J.ext_id = SED.ext_id")

createOrReplaceTempView(join2_secondi , "join2_secondi")

##### CREAZIONE VARIABILE 77: 'secondi_web' ####

secondi_web <- sql("select ext_id, sum (secondi_totali_corporate + secondi_totali_tg24 + secondi_totali_sport) as secondi_web
                   from df_secondi1
                   group by ext_id")

createOrReplaceTempView(secondi_web , "secondi_web")

join3_secondi <- sql("select DISTINCT J2.*, SW.secondi_web
                     from join2_secondi J2
                     left join secondi_web SW 
                     on J2.ext_id = SW.ext_id")

createOrReplaceTempView(join3_secondi , "join3_secondi")

##### CREAZIONE VARIABILE 78: 'secondi_app' ####

secondi_app <- sql("select ext_id, sum (secondi_totali_appwsc + secondi_totali_appgtv + secondi_totali_appsport + secondi_totali_appxfactor) as secondi_app
                   from df_secondi1
                   group by ext_id")

createOrReplaceTempView(secondi_app , "secondi_app")

join4_secondi <- sql("select DISTINCT J3.*, SA.secondi_app
                     from join3_secondi J3
                     left join secondi_app SA 
                     on J3.ext_id = SA.ext_id")


##### CREAZIONE VARIABILI 79-82: avg_sec_corporate_appwsc_appgtv, avg_sec_editoriale, avg_sec_web, avg_sec_app ####

# Semplice media dei secondi spesi su ciascun aggregato

df_variabile_14 <- withColumn(join4_secondi, "avg_sec_corporate_appwsc_appgtv", 
                              join4_secondi$secondi_corporate_appwsc_appgtv/join4_secondi$visite_corporate_appwsc_appgtv)

df_variabile_14 <- withColumn(df_variabile_14, "avg_sec_editoriale", 
                              df_variabile_14$secondi_editoriale/df_variabile_14$visite_editoriale)

df_variabile_14 <- withColumn(df_variabile_14, "avg_sec_web", 
                              df_variabile_14$secondi_web/df_variabile_14$visite_web)

df_variabile_14 <- withColumn(df_variabile_14, "avg_sec_app", 
                              df_variabile_14$secondi_app/df_variabile_14$visite_app)

write.parquet(df_variabile_14, path_df_variabile_14, mode = "overwrite")

df_variabile_14 <- read.parquet(path_df_variabile_14)

createOrReplaceTempView(df_variabile_14, "df_variabile_14")

##### CREAZIONE VARIABILE 83: 'score_utilizzo' ####

df_utilizzatori <- fillna(df_variabile_14, 0)

createOrReplaceTempView(df_utilizzatori, "df_utilizzatori")

# calcolo lo score utilizzo sommando le visite totali delle 7 reportsuite
score_utilizzo <- sql("select df_utilizzatori.ext_id, sum(visite_totali_corporate + visite_totali_tg24 + visite_totali_sport + 
                                          visite_totali_appwsc + visite_totali_appsport + visite_totali_appgtv + visite_totali_appxfactor) as score_utilizzo
                      from df_utilizzatori
                      group by ext_id") 

createOrReplaceTempView(score_utilizzo, "score_utilizzo_t")

df_variabile_15 <- sql("select DISTINCT DF.*, SU.score_utilizzo
                       from df_variabile_14 DF
                       left join score_utilizzo_t SU 
                       on DF.ext_id = SU.ext_id")

createOrReplaceTempView(df_variabile_15, "df_variabile_15")

##### CREAZIONE VARIABILE 84: 'percentile_score_utilizzo' ####

percentili_score_utilizzo <- sql("select percentile_approx(score_utilizzo, 0.10) as d1,
                                 percentile_approx(score_utilizzo, 0.20) as d2,
                                 percentile_approx(score_utilizzo, 0.30) as d3,
                                 percentile_approx(score_utilizzo, 0.40) as d4, 
                                 percentile_approx(score_utilizzo, 0.50) as mediana, -- q2 = mediana,
                                 percentile_approx(score_utilizzo, 0.60) as d6,    
                                 percentile_approx(score_utilizzo, 0.70) as d7,
                                 percentile_approx(score_utilizzo, 0.80) as d8,
                                 percentile_approx(score_utilizzo, 0.90) as d9, -- 90esimo percentile
                                 percentile_approx(score_utilizzo, 1) as max
                                 from score_utilizzo_t
                                 where score_utilizzo is NOT NULL") # calcolo i decili dello score utilizzo 

# View(head(percentili_score_utilizzo, 20)) 

percentili_score_utilizzo_df <- as.data.frame(percentili_score_utilizzo)
d1 <- percentili_score_utilizzo_df[1,1]
d2 <- percentili_score_utilizzo_df[1,2]
d3 <- percentili_score_utilizzo_df[1,3]
d4 <- percentili_score_utilizzo_df[1,4]
d5 <- percentili_score_utilizzo_df[1,5]
d6 <- percentili_score_utilizzo_df[1,6]
d7 <- percentili_score_utilizzo_df[1,7]
d8 <- percentili_score_utilizzo_df[1,8]
d9 <- percentili_score_utilizzo_df[1,9]
d10 <- percentili_score_utilizzo_df[1,10]

# sostituire i valori
df_variabile_15 <- withColumn(df_variabile_15, "percentile_score_utilizzo", ifelse(df_variabile_15$score_utilizzo > d9, 10, 
                                                                                   ifelse(df_variabile_15$score_utilizzo > d8 & df_variabile_15$score_utilizzo <= d9, 9, 
                                                                                          ifelse(df_variabile_15$score_utilizzo > d7 & df_variabile_15$score_utilizzo <= d8, 8, 
                                                                                                 ifelse(df_variabile_15$score_utilizzo > d6 & df_variabile_15$score_utilizzo <= d7, 7, 
                                                                                                        ifelse(df_variabile_15$score_utilizzo > d5 & df_variabile_15$score_utilizzo <= d6 , 6, 
                                                                                                               ifelse(df_variabile_15$score_utilizzo > d4 & df_variabile_15$score_utilizzo <= d5, 5, 
                                                                                                                      ifelse(df_variabile_15$score_utilizzo > d3 & df_variabile_15$score_utilizzo <= d4, 4, 
                                                                                                                             ifelse(df_variabile_15$score_utilizzo > d2 & df_variabile_15$score_utilizzo <= d3, 3, 
                                                                                                                                    ifelse(df_variabile_15$score_utilizzo > d1 & df_variabile_15$score_utilizzo <= d2, 2, 
                                                                                                                                           ifelse(df_variabile_15$score_utilizzo <= d1 , 1, 0)))))))))))

##### CREAZIONE VARIABILI 85-88: ricorrenza sui diversi aggregati #####

df_variabile_15 <- withColumn(df_variabile_15, "ricorrenza_corporate_appwsc_appgtv", 
                              ifelse(df_variabile_15$ricorrenza_corporate == 1 | df_variabile_15$ricorrenza_appgtv == 1 | df_variabile_15$ricorrenza_appwsc == 1, 
                                     1, 0))

df_variabile_15 <- withColumn(df_variabile_15, "ricorrenza_editoriale", 
                              ifelse(df_variabile_15$ricorrenza_tg24 == 1 | df_variabile_15$ricorrenza_sport == 1 | df_variabile_15$ricorrenza_appxfactor == 1 | 
                                       df_variabile_15$ricorrenza_appsport == 1, 1, 0))

df_variabile_15 <- withColumn(df_variabile_15, "ricorrenza_web", 
                              ifelse(df_variabile_15$ricorrenza_tg24 == 1 | df_variabile_15$ricorrenza_sport == 1 | df_variabile_15$ricorrenza_corporate == 1, 
                                     1, 0))

df_variabile_15 <- withColumn(df_variabile_15, "ricorrenza_app", 
                              ifelse(df_variabile_15$ricorrenza_appxfactor == 1 | df_variabile_15$ricorrenza_appsport == 1 | df_variabile_15$ricorrenza_appgtv == 1 | 
                                       df_variabile_15$ricorrenza_appwsc == 1 , 1, 0))

write.parquet(df_variabile_15, path_df_variabile_15)

df_variabile_15 <- read.parquet(path_df_variabile_15)

createOrReplaceTempView(df_variabile_15, "df_variabile_15")

##### CREAZIONE VARIABILE 89, 90: visite_settimana_corporate, visite_weekend_corporate ####

# aggiungo la giornata a corporate_new, che si trova nello script numero 2!!!
corporate_giorno <- withColumn(corporate_new, 'giorno', date_format(corporate_new$date_time_ts, 'EEEE')) 

# ifelse per distinguere settimana da weekend
corporate_giorno$week_divided <- ifelse((corporate_giorno$giorno == 'Saturday' | corporate_giorno$giorno == 'Sunday'), 'weekend' , 'week' ) 

corporate_giorno$visita_unica <- concat(corporate_giorno$post_visid_concatenated, corporate_giorno$visit_num) # aggiunta visita unica

corporate_giorno2 <- select(corporate_giorno,
                            "external_id_post_evar",
                            "post_visid_concatenated",
                            "visit_num",
                            "visita_unica",
                            "week_divided") # SUBSET

createOrReplaceTempView(corporate_giorno2, 'corporate_giorno2')

corporate_giorno3 <- sql('SELECT *,
                         CASE WHEN (week_divided=="week") then 1 else Null end as flag_settimana,
                         CASE WHEN (week_divided=="weekend") THEN 1 ELSE Null END AS flag_weekend
                         FROM corporate_giorno2') # flag visita settimanale o weekend

createOrReplaceTempView(corporate_giorno3, 'corporate_giorno3')

corporate_giorno4 <- sql('SELECT external_id_post_evar, visita_unica,
                         COUNT(DISTINCT(flag_settimana)) AS count_settimana,
                         COUNT(DISTINCT(flag_weekend)) AS count_weekend
                         FROM corporate_giorno3
                         GROUP BY external_id_post_evar, visita_unica') # count visite uniche settimanali/weekend per ext id

createOrReplaceTempView(corporate_giorno4, 'corporate_giorno4')

corporate_giorno5 <- sql('SELECT external_id_post_evar,
                         SUM(count_settimana) AS visite_settimana_corporate,
                         SUM(count_weekend) AS visite_weekend_corporate
                         FROM corporate_giorno4
                         GROUP BY external_id_post_evar') # totale visite settimaneli/weekend per ext id

createOrReplaceTempView(corporate_giorno5, "corporate_giorno5")

##### CREAZIONE VARIABILE 91, 92: visite_settimana_tg24, visite_weekend_tg24 ####

tg24_giorno <- withColumn(tg24_new, 'giorno', date_format(tg24_new$date_time_ts, 'EEEE')) # aggiungo la giornata a tg24_new, che si trova nello script numero 2!!!

tg24_giorno$week_divided <- ifelse((tg24_giorno$giorno == 'Saturday' | tg24_giorno$giorno == 'Sunday'), 'weekend' , 'week' ) # ifelse per distinguere settimana da weekend

tg24_giorno$visita_unica <- concat(tg24_giorno$post_visid_concatenated, tg24_giorno$visit_num) # aggiunta visita unica

tg24_giorno2 <- select(tg24_giorno,
                       "external_id_post_evar",
                       "post_visid_concatenated",
                       "visit_num",
                       "visita_unica",
                       "week_divided") # SUBSET

createOrReplaceTempView(tg24_giorno2, 'tg24_giorno2')

tg24_giorno3 <- sql('SELECT *,
                    CASE WHEN (week_divided=="week") then 1 else Null end as flag_settimana,
                    CASE WHEN (week_divided=="weekend") THEN 1 ELSE Null END AS flag_weekend
                    FROM tg24_giorno2') # flag visita settimanale o weekend

createOrReplaceTempView(tg24_giorno3, 'tg24_giorno3')

tg24_giorno4 <- sql('SELECT external_id_post_evar, visita_unica,
                    COUNT(DISTINCT(flag_settimana)) AS count_settimana,
                    COUNT(DISTINCT(flag_weekend)) AS count_weekend
                    FROM tg24_giorno3
                    GROUP BY external_id_post_evar, visita_unica') # count visite uniche settimanali/weekend per ext id

createOrReplaceTempView(tg24_giorno4, 'tg24_giorno4')

tg24_giorno5 <- sql('SELECT external_id_post_evar,
                    SUM(count_settimana) AS visite_settimana_tg24,
                    SUM(count_weekend) AS visite_weekend_tg24
                    FROM tg24_giorno4
                    GROUP BY external_id_post_evar') # totale visite settimaneli/weekend per ext id

createOrReplaceTempView(tg24_giorno5, "tg24_giorno5")

##### CREAZIONE VARIABILE 93, 94: visite_settimana_sport, visite_weekend_sport ####

sport_giorno <- withColumn(sport_new, 'giorno', date_format(sport_new$date_time_ts, 'EEEE')) # aggiungo la giornata a sport_new, che si trova nello script numero 2!!!

sport_giorno$week_divided <- ifelse((sport_giorno$giorno == 'Saturday' | sport_giorno$giorno == 'Sunday'), 'weekend' , 'week' ) # ifelse per distinguere settimana da weekend

sport_giorno$visita_unica <- concat(sport_giorno$post_visid_concatenated, sport_giorno$visit_num) # aggiunta visita unica

sport_giorno2 <- select(sport_giorno,
                        "external_id_post_evar",
                        "post_visid_concatenated",
                        "visit_num",
                        "visita_unica",
                        "week_divided") # SUBSET

createOrReplaceTempView(sport_giorno2, 'sport_giorno2')

sport_giorno3 <- sql('SELECT *,
                     CASE WHEN (week_divided=="week") then 1 else Null end as flag_settimana,
                     CASE WHEN (week_divided=="weekend") THEN 1 ELSE Null END AS flag_weekend
                     FROM sport_giorno2') # flag visita settimanale o weekend

createOrReplaceTempView(sport_giorno3, 'sport_giorno3')

sport_giorno4 <- sql('SELECT external_id_post_evar, visita_unica,
                     COUNT(DISTINCT(flag_settimana)) AS count_settimana,
                     COUNT(DISTINCT(flag_weekend)) AS count_weekend
                     FROM sport_giorno3
                     GROUP BY external_id_post_evar, visita_unica') # count visite uniche settimanali/weekend per ext id

createOrReplaceTempView(sport_giorno4, 'sport_giorno4')

sport_giorno5 <- sql('SELECT external_id_post_evar,
                     SUM(count_settimana) AS visite_settimana_sport,
                     SUM(count_weekend) AS visite_weekend_sport
                     FROM sport_giorno4
                     GROUP BY external_id_post_evar') # totale visite settimaneli/weekend per ext id

createOrReplaceTempView(sport_giorno5, "sport_giorno5")

##### CREAZIONE VARIABILE 95, 96: visite_settimana_appwsc, visite_weekend_appwsc ####

appwsc_giorno <- withColumn(appwsc_new, 'giorno', date_format(appwsc_new$date_time_ts, 'EEEE')) # aggiungo la giornata a appwsc_new, che si trova nello script numero 2!!!

appwsc_giorno$week_divided <- ifelse((appwsc_giorno$giorno == 'Saturday' | appwsc_giorno$giorno == 'Sunday'), 'weekend' , 'week' ) # ifelse per distinguere settimana da weekend

appwsc_giorno$visita_unica <- concat(appwsc_giorno$post_visid_concatenated, appwsc_giorno$visit_num) # aggiunta visita unica

appwsc_giorno2 <- select(appwsc_giorno,
                         "external_id_post_evar",
                         "post_visid_concatenated",
                         "visit_num",
                         "visita_unica",
                         "week_divided") # SUBSET

createOrReplaceTempView(appwsc_giorno2, 'appwsc_giorno2')

appwsc_giorno3 <- sql('SELECT *,
                      CASE WHEN (week_divided=="week") then 1 else Null end as flag_settimana,
                      CASE WHEN (week_divided=="weekend") THEN 1 ELSE Null END AS flag_weekend
                      FROM appwsc_giorno2') # flag visita settimanale o weekend

createOrReplaceTempView(appwsc_giorno3, 'appwsc_giorno3')

appwsc_giorno4 <- sql('SELECT external_id_post_evar, visita_unica,
                      COUNT(DISTINCT(flag_settimana)) AS count_settimana,
                      COUNT(DISTINCT(flag_weekend)) AS count_weekend
                      FROM appwsc_giorno3
                      GROUP BY external_id_post_evar, visita_unica') # count visite uniche settimanali/weekend per ext id

createOrReplaceTempView(appwsc_giorno4, 'appwsc_giorno4')

appwsc_giorno5 <- sql('SELECT external_id_post_evar,
                      SUM(count_settimana) AS visite_settimana_appwsc,
                      SUM(count_weekend) AS visite_weekend_appwsc
                      FROM appwsc_giorno4
                      GROUP BY external_id_post_evar') # totale visite settimaneli/weekend per ext id

createOrReplaceTempView(appwsc_giorno5, "appwsc_giorno5")

##### CREAZIONE VARIABILE 97, 98: visite_settimana_appgtv, visite_weekend_appgtv ####

appgtv_giorno <- withColumn(appgtv_new, 'giorno', date_format(appgtv_new$date_time_ts, 'EEEE')) # aggiungo la giornata a appgtv_new, che si trova nello script numero 2!!!

appgtv_giorno$week_divided <- ifelse((appgtv_giorno$giorno == 'Saturday' | appgtv_giorno$giorno == 'Sunday'), 'weekend' , 'week' ) # ifelse per distinguere settimana da weekend

appgtv_giorno$visita_unica <- concat(appgtv_giorno$post_visid_concatenated, appgtv_giorno$visit_num) # aggiunta visita unica

appgtv_giorno2 <- select(appgtv_giorno,
                         "external_id_post_evar",
                         "post_visid_concatenated",
                         "visit_num",
                         "visita_unica",
                         "week_divided") # SUBSET

createOrReplaceTempView(appgtv_giorno2, 'appgtv_giorno2')

appgtv_giorno3 <- sql('SELECT *,
                      CASE WHEN (week_divided=="week") then 1 else Null end as flag_settimana,
                      CASE WHEN (week_divided=="weekend") THEN 1 ELSE Null END AS flag_weekend
                      FROM appgtv_giorno2') # flag visita settimanale o weekend

createOrReplaceTempView(appgtv_giorno3, 'appgtv_giorno3')

appgtv_giorno4 <- sql('SELECT external_id_post_evar, visita_unica,
                      COUNT(DISTINCT(flag_settimana)) AS count_settimana,
                      COUNT(DISTINCT(flag_weekend)) AS count_weekend
                      FROM appgtv_giorno3
                      GROUP BY external_id_post_evar, visita_unica') # count visite uniche settimanali/weekend per ext id

createOrReplaceTempView(appgtv_giorno4, 'appgtv_giorno4')

appgtv_giorno5 <- sql('SELECT external_id_post_evar,
                      SUM(count_settimana) AS visite_settimana_appgtv,
                      SUM(count_weekend) AS visite_weekend_appgtv
                      FROM appgtv_giorno4
                      GROUP BY external_id_post_evar') # totale visite settimaneli/weekend per ext id

createOrReplaceTempView(appgtv_giorno5, "appgtv_giorno5")

##### CREAZIONE VARIABILE 99, 100: visite_settimana_appsport, visite_weekend_appsport ####

appsport_giorno <- withColumn(appsport_new, 'giorno', date_format(appsport_new$date_time_ts, 'EEEE')) # aggiungo la giornata a appsport_new, che si trova nello script numero 2!!!

appsport_giorno$week_divided <- ifelse((appsport_giorno$giorno == 'Saturday' | appsport_giorno$giorno == 'Sunday'), 'weekend' , 'week' ) # ifelse per distinguere settimana da weekend

appsport_giorno$visita_unica <- concat(appsport_giorno$post_visid_concatenated, appsport_giorno$visit_num) # aggiunta visita unica

appsport_giorno2 <- select(appsport_giorno,
                           "external_id_post_evar",
                           "post_visid_concatenated",
                           "visit_num",
                           "visita_unica",
                           "week_divided") # SUBSET

createOrReplaceTempView(appsport_giorno2, 'appsport_giorno2')

appsport_giorno3 <- sql('SELECT *,
                        CASE WHEN (week_divided=="week") then 1 else Null end as flag_settimana,
                        CASE WHEN (week_divided=="weekend") THEN 1 ELSE Null END AS flag_weekend
                        FROM appsport_giorno2') # flag visita settimanale o weekend

createOrReplaceTempView(appsport_giorno3, 'appsport_giorno3')

appsport_giorno4 <- sql('SELECT external_id_post_evar, visita_unica,
                        COUNT(DISTINCT(flag_settimana)) AS count_settimana,
                        COUNT(DISTINCT(flag_weekend)) AS count_weekend
                        FROM appsport_giorno3
                        GROUP BY external_id_post_evar, visita_unica') # count visite uniche settimanali/weekend per ext id

createOrReplaceTempView(appsport_giorno4, 'appsport_giorno4')

appsport_giorno5 <- sql('SELECT external_id_post_evar,
                        SUM(count_settimana) AS visite_settimana_appsport,
                        SUM(count_weekend) AS visite_weekend_appsport
                        FROM appsport_giorno4
                        GROUP BY external_id_post_evar') # totale visite settimaneli/weekend per ext id

createOrReplaceTempView(appsport_giorno5, "appsport_giorno5")

##### CREAZIONE VARIABILE 101, 102: visite_settimana_appxfactor, visite_weekend_appxfactor ####

appxfactor_giorno <- withColumn(appxfactor_new, 'giorno', date_format(appxfactor_new$date_time_ts, 'EEEE')) # aggiungo la giornata a appxfactor_new, che si trova nello script numero 2!!!

appxfactor_giorno$week_divided <- ifelse((appxfactor_giorno$giorno == 'Saturday' | appxfactor_giorno$giorno == 'Sunday'), 'weekend' , 'week' ) # ifelse per distinguere settimana da weekend

appxfactor_giorno$visita_unica <- concat(appxfactor_giorno$post_visid_concatenated, appxfactor_giorno$visit_num) # aggiunta visita unica

appxfactor_giorno2 <- select(appxfactor_giorno,
                             "external_id_post_evar",
                             "post_visid_concatenated",
                             "visit_num",
                             "visita_unica",
                             "week_divided") # SUBSET

createOrReplaceTempView(appxfactor_giorno2, 'appxfactor_giorno2')

appxfactor_giorno3 <- sql('SELECT *,
                          CASE WHEN (week_divided=="week") then 1 else Null end as flag_settimana,
                          CASE WHEN (week_divided=="weekend") THEN 1 ELSE Null END AS flag_weekend
                          FROM appxfactor_giorno2') # flag visita settimanale o weekend

createOrReplaceTempView(appxfactor_giorno3, 'appxfactor_giorno3')

appxfactor_giorno4 <- sql('SELECT external_id_post_evar, visita_unica,
                          COUNT(DISTINCT(flag_settimana)) AS count_settimana,
                          COUNT(DISTINCT(flag_weekend)) AS count_weekend
                          FROM appxfactor_giorno3
                          GROUP BY external_id_post_evar, visita_unica') # count visite uniche settimanali/weekend per ext id

createOrReplaceTempView(appxfactor_giorno4, 'appxfactor_giorno4')

appxfactor_giorno5 <- sql('SELECT external_id_post_evar,
                          SUM(count_settimana) AS visite_settimana_appxfactor,
                          SUM(count_weekend) AS visite_weekend_appxfactor
                          FROM appxfactor_giorno4
                          GROUP BY external_id_post_evar') # totale visite settimaneli/weekend per ext id

createOrReplaceTempView(appxfactor_giorno5, "appxfactor_giorno5")

##### JOIN DELLE VARIABILI 89-102 ####

join1_settimana <- sql("select DISTINCT DF.*, C.visite_settimana_corporate, C.visite_weekend_corporate
                       from df_variabile_15 DF
                       left join corporate_giorno5 C
                       on DF.ext_id = C.external_id_post_evar")

createOrReplaceTempView(join1_settimana, "join1_settimana")

join2_settimana <- sql("select DISTINCT J.*, T.visite_settimana_tg24, T.visite_weekend_tg24
                       from join1_settimana J
                       left join tg24_giorno5 T
                       on J.ext_id = T.external_id_post_evar")

createOrReplaceTempView(join2_settimana, "join2_settimana")

join3_settimana <- sql("select DISTINCT J2.*, S.visite_settimana_sport, S.visite_weekend_sport
                       from join2_settimana J2
                       left join sport_giorno5 S
                       on J2.ext_id = S.external_id_post_evar")

createOrReplaceTempView(join3_settimana, "join3_settimana")

join4_settimana <- sql("select DISTINCT J3.*, W.visite_settimana_appwsc, W.visite_weekend_appwsc
                       from join3_settimana J3
                       left join appwsc_giorno5 W
                       on J3.ext_id = W.external_id_post_evar")

createOrReplaceTempView(join4_settimana, "join4_settimana")

join5_settimana <- sql("select DISTINCT J4.*, G.visite_settimana_appgtv, G.visite_weekend_appgtv
                       from join4_settimana J4
                       left join appgtv_giorno5 G
                       on J4.ext_id = G.external_id_post_evar")

createOrReplaceTempView(join5_settimana, "join5_settimana")

join6_settimana <- sql("select DISTINCT J5.*, ASP.visite_settimana_appsport, ASP.visite_weekend_appsport
                       from join5_settimana J5
                       left join appsport_giorno5 ASP
                       on J5.ext_id = ASP.external_id_post_evar")

createOrReplaceTempView(join6_settimana, "join6_settimana")

join7_settimana <- sql("select DISTINCT J6.*, XF.visite_settimana_appxfactor, XF.visite_weekend_appxfactor
                       from join6_settimana J6
                       left join appxfactor_giorno5 XF
                       on J6.ext_id = XF.external_id_post_evar")

createOrReplaceTempView(join7_settimana, "join7_settimana")

write.parquet(join7_settimana, path_df_variabile_16)

df_variabile_16 <- read.parquet(path_df_variabile_16)

createOrReplaceTempView(df_variabile_16, 'df_variabile_16')

##### CREAZIONE variabili 103-106: visite nelle fasce orarie corporate ####

corporate_orari <- select(corporate_new, "external_id_post_evar",
                          "visit_num",
                          "post_visid_concatenated",
                          "date_time_ts")

createOrReplaceTempView(corporate_orari, "corporate_orari")

corporate_orari_2 <- withColumn(corporate_orari, "visit_hour", hour(corporate_orari$date_time_ts))

createOrReplaceTempView(corporate_orari_2, "corporate_orari_2")

corporate_orari_3 <- sql("select distinct corporate_orari_2.external_id_post_evar, corporate_orari_2.post_visid_concatenated, corporate_orari_2.visit_num, visit_hour 
                         from corporate_orari_2
                         where external_id_post_evar is not null")

corporate_orari_4 <- withColumn(corporate_orari_3, "time_slot", 
                                ifelse(corporate_orari_3$visit_hour>=0 & corporate_orari_3$visit_hour<6, "notte", 
                                       ifelse(corporate_orari_3$visit_hour>=6 & corporate_orari_3$visit_hour<12, "mattina", 
                                              ifelse(corporate_orari_3$visit_hour>=12 & corporate_orari_3$visit_hour<18, "pomeriggio", 
                                                     ifelse(corporate_orari_3$visit_hour>=18 & corporate_orari_3$visit_hour<=23, "sera", NA)))))

createOrReplaceTempView(corporate_orari_4, "corporate_orari_4")

corporate_orari_5 <- sql('SELECT *,
                         CASE WHEN (time_slot=="notte") then 1 else Null end as flag_corporate_notte,
                         CASE WHEN (time_slot=="mattina") THEN 1 ELSE Null END AS flag_corporate_mattina,
                         CASE WHEN (time_slot=="pomeriggio") then 1 else Null end as flag_corporate_pomeriggio,
                         CASE WHEN (time_slot=="sera") THEN 1 ELSE Null END AS flag_corporate_sera
                         FROM corporate_orari_4')

createOrReplaceTempView(corporate_orari_5, 'corporate_orari_5')

corporate_orari_6 <- sql('SELECT external_id_post_evar, post_visid_concatenated, visit_num,
                         COUNT(DISTINCT(flag_corporate_notte)) AS count_corporate_notte,
                         COUNT(DISTINCT(flag_corporate_mattina)) AS count_corporate_mattina,
                         COUNT(DISTINCT(flag_corporate_pomeriggio)) AS count_corporate_pomeriggio,
                         COUNT(DISTINCT(flag_corporate_sera)) AS count_corporate_sera
                         FROM corporate_orari_5
                         GROUP BY external_id_post_evar, post_visid_concatenated, visit_num 
                         ')

createOrReplaceTempView(corporate_orari_6, 'corporate_orari_6')

corporate_orari_7 <- sql('SELECT external_id_post_evar,
                         SUM(count_corporate_notte) AS visite_corporate_notte,
                         SUM(count_corporate_mattina) AS visite_corporate_mattina,
                         SUM(count_corporate_pomeriggio) AS visite_corporate_pomeriggio,
                         SUM(count_corporate_sera) AS visite_corporate_sera
                         FROM corporate_orari_6
                         GROUP BY external_id_post_evar
                         ')

createOrReplaceTempView(corporate_orari_7, "corporate_orari_7")

join1_orari <- sql("select DISTINCT DF.*, C.visite_corporate_notte, C.visite_corporate_mattina, C.visite_corporate_pomeriggio, C.visite_corporate_sera
                   from df_variabile_16 DF
                   left join corporate_orari_7 C
                   on DF.ext_id = C.external_id_post_evar")

write.parquet(join1_orari, path_df_variabile_17, mode = "overwrite")

df_variabile_17 <- read.parquet(path_df_variabile_17)

createOrReplaceTempView(df_variabile_17, 'df_variabile_17')

# Nei prossimi step creo il conto delle visite per fascia oraria per external id

##### CREAZIONE variabili 107-110: visite nelle fasce orarie tg24 ####

tg24_orari <- select(tg24_new, "external_id_post_evar",
                     "visit_num",
                     "post_visid_concatenated",
                     "date_time_ts")

createOrReplaceTempView(tg24_orari, "tg24_orari")

tg24_orari_2 <- withColumn(tg24_orari, "visit_hour", hour(tg24_orari$date_time_ts)) 

createOrReplaceTempView(tg24_orari_2, "tg24_orari_2")

tg24_orari_3 <- sql("select distinct tg24_orari_2.external_id_post_evar, tg24_orari_2.post_visid_concatenated, tg24_orari_2.visit_num, visit_hour 
                    from tg24_orari_2
                    where external_id_post_evar is not null")

tg24_orari_4 <- withColumn(tg24_orari_3, "time_slot", ifelse(tg24_orari_3$visit_hour>=0 & tg24_orari_3$visit_hour<6, "notte", 
                                                             ifelse(tg24_orari_3$visit_hour>=6 & tg24_orari_3$visit_hour<12, "mattina", 
                                                                    ifelse(tg24_orari_3$visit_hour>=12 & tg24_orari_3$visit_hour<18, "pomeriggio", 
                                                                           ifelse(tg24_orari_3$visit_hour>=18 & tg24_orari_3$visit_hour<=23, "sera", NA)))))

createOrReplaceTempView(tg24_orari_4, "tg24_orari_4")

tg24_orari_5 <- sql('SELECT *,
                    CASE WHEN (time_slot=="notte") then 1 else Null end as flag_tg24_notte,
                    CASE WHEN (time_slot=="mattina") THEN 1 ELSE Null END AS flag_tg24_mattina,
                    CASE WHEN (time_slot=="pomeriggio") then 1 else Null end as flag_tg24_pomeriggio,
                    CASE WHEN (time_slot=="sera") THEN 1 ELSE Null END AS flag_tg24_sera
                    FROM tg24_orari_4')

createOrReplaceTempView(tg24_orari_5, 'tg24_orari_5')

tg24_orari_6 <- sql('SELECT external_id_post_evar, post_visid_concatenated, visit_num,
                    COUNT(DISTINCT(flag_tg24_notte)) AS count_tg24_notte,
                    COUNT(DISTINCT(flag_tg24_mattina)) AS count_tg24_mattina,
                    COUNT(DISTINCT(flag_tg24_pomeriggio)) AS count_tg24_pomeriggio,
                    COUNT(DISTINCT(flag_tg24_sera)) AS count_tg24_sera
                    FROM tg24_orari_5
                    GROUP BY external_id_post_evar, post_visid_concatenated, visit_num 
                    ')

createOrReplaceTempView(tg24_orari_6, 'tg24_orari_6')

tg24_orari_7 <- sql('SELECT external_id_post_evar,
                    SUM(count_tg24_notte) AS visite_tg24_notte,
                    SUM(count_tg24_mattina) AS visite_tg24_mattina,
                    SUM(count_tg24_pomeriggio) AS visite_tg24_pomeriggio,
                    SUM(count_tg24_sera) AS visite_tg24_sera
                    FROM tg24_orari_6
                    GROUP BY external_id_post_evar
                    ')

createOrReplaceTempView(tg24_orari_7, "tg24_orari_7")

join2_orari <- sql("select DISTINCT DF.*, T.visite_tg24_notte, T.visite_tg24_mattina, T.visite_tg24_pomeriggio, T.visite_tg24_sera
                   from df_variabile_17 DF
                   left join tg24_orari_7 T
                   on DF.ext_id = T.external_id_post_evar")

write.parquet(join2_orari, path_df_variabile_18, mode = "overwrite")

df_variabile_18 <- read.parquet(path_df_variabile_18)

createOrReplaceTempView(df_variabile_18, 'df_variabile_18')

##### CREAZIONE variabili 111-114: visite nelle fasce orarie sport ####

sport_orari <- select(sport_new, "external_id_post_evar",
                      "visit_num",
                      "post_visid_concatenated",
                      "date_time_ts")

createOrReplaceTempView(sport_orari, "sport_orari")

sport_orari_2 <- withColumn(sport_orari, "visit_hour", hour(sport_orari$date_time_ts))

createOrReplaceTempView(sport_orari_2, "sport_orari_2")

sport_orari_3 <- sql("select distinct sport_orari_2.external_id_post_evar, sport_orari_2.post_visid_concatenated, sport_orari_2.visit_num, visit_hour 
                     from sport_orari_2
                     where external_id_post_evar is not null")

sport_orari_4 <- withColumn(sport_orari_3, "time_slot", ifelse(sport_orari_3$visit_hour>=0 & sport_orari_3$visit_hour<6, "notte", 
                                                               ifelse(sport_orari_3$visit_hour>=6 & sport_orari_3$visit_hour<12, "mattina", 
                                                                      ifelse(sport_orari_3$visit_hour>=12 & sport_orari_3$visit_hour<18, "pomeriggio", 
                                                                             ifelse(sport_orari_3$visit_hour>=18 & sport_orari_3$visit_hour<=23, "sera", NA)))))

createOrReplaceTempView(sport_orari_4, "sport_orari_4")

sport_orari_5 <- sql('SELECT *,
                     CASE WHEN (time_slot=="notte") then 1 else Null end as flag_sport_notte,
                     CASE WHEN (time_slot=="mattina") THEN 1 ELSE Null END AS flag_sport_mattina,
                     CASE WHEN (time_slot=="pomeriggio") then 1 else Null end as flag_sport_pomeriggio,
                     CASE WHEN (time_slot=="sera") THEN 1 ELSE Null END AS flag_sport_sera
                     FROM sport_orari_4')

createOrReplaceTempView(sport_orari_5, 'sport_orari_5')

sport_orari_6 <- sql('SELECT external_id_post_evar, post_visid_concatenated, visit_num,
                     COUNT(DISTINCT(flag_sport_notte)) AS count_sport_notte,
                     COUNT(DISTINCT(flag_sport_mattina)) AS count_sport_mattina,
                     COUNT(DISTINCT(flag_sport_pomeriggio)) AS count_sport_pomeriggio,
                     COUNT(DISTINCT(flag_sport_sera)) AS count_sport_sera
                     FROM sport_orari_5
                     GROUP BY external_id_post_evar, post_visid_concatenated, visit_num 
                     ')

createOrReplaceTempView(sport_orari_6, 'sport_orari_6')

sport_orari_7 <- sql('SELECT external_id_post_evar,
                     SUM(count_sport_notte) AS visite_sport_notte,
                     SUM(count_sport_mattina) AS visite_sport_mattina,
                     SUM(count_sport_pomeriggio) AS visite_sport_pomeriggio,
                     SUM(count_sport_sera) AS visite_sport_sera
                     FROM sport_orari_6
                     GROUP BY external_id_post_evar
                     ')

createOrReplaceTempView(sport_orari_7, "sport_orari_7")

join3_orari <- sql("select DISTINCT DF.*, S.visite_sport_notte, S.visite_sport_mattina, S.visite_sport_pomeriggio, S.visite_sport_sera
                   from df_variabile_18 DF
                   left join sport_orari_7 S
                   on DF.ext_id = S.external_id_post_evar")

write.parquet(join3_orari, path_df_variabile_19, mode = "overwrite")

df_variabile_19 <- read.parquet(path_df_variabile_19)

createOrReplaceTempView(df_variabile_19, 'df_variabile_19')

##### CREAZIONE variabili 115-118: visite nelle fasce orarie appwsc ####

appwsc_orari <- select(appwsc_new, "external_id_post_evar",
                       "visit_num",
                       "post_visid_concatenated",
                       "date_time_ts")

createOrReplaceTempView(appwsc_orari, "appwsc_orari")

appwsc_orari_2 <- withColumn(appwsc_orari, "visit_hour", hour(appwsc_orari$date_time_ts))

createOrReplaceTempView(appwsc_orari_2, "appwsc_orari_2")

appwsc_orari_3 <- sql("select distinct appwsc_orari_2.external_id_post_evar, appwsc_orari_2.post_visid_concatenated, appwsc_orari_2.visit_num, visit_hour 
                      from appwsc_orari_2
                      where external_id_post_evar is not null")

appwsc_orari_4 <- withColumn(appwsc_orari_3, "time_slot", ifelse(appwsc_orari_3$visit_hour>=0 & appwsc_orari_3$visit_hour<6, "notte", 
                                                                 ifelse(appwsc_orari_3$visit_hour>=6 & appwsc_orari_3$visit_hour<12, "mattina", 
                                                                        ifelse(appwsc_orari_3$visit_hour>=12 & appwsc_orari_3$visit_hour<18, "pomeriggio", 
                                                                               ifelse(appwsc_orari_3$visit_hour>=18 & appwsc_orari_3$visit_hour<=23, "sera", NA)))))

createOrReplaceTempView(appwsc_orari_4, "appwsc_orari_4")

appwsc_orari_5 <- sql('SELECT *,
                      CASE WHEN (time_slot=="notte") then 1 else Null end as flag_appwsc_notte,
                      CASE WHEN (time_slot=="mattina") THEN 1 ELSE Null END AS flag_appwsc_mattina,
                      CASE WHEN (time_slot=="pomeriggio") then 1 else Null end as flag_appwsc_pomeriggio,
                      CASE WHEN (time_slot=="sera") THEN 1 ELSE Null END AS flag_appwsc_sera
                      FROM appwsc_orari_4')

createOrReplaceTempView(appwsc_orari_5, 'appwsc_orari_5')

appwsc_orari_6 <- sql('SELECT external_id_post_evar, post_visid_concatenated, visit_num,
                      COUNT(DISTINCT(flag_appwsc_notte)) AS count_appwsc_notte,
                      COUNT(DISTINCT(flag_appwsc_mattina)) AS count_appwsc_mattina,
                      COUNT(DISTINCT(flag_appwsc_pomeriggio)) AS count_appwsc_pomeriggio,
                      COUNT(DISTINCT(flag_appwsc_sera)) AS count_appwsc_sera
                      FROM appwsc_orari_5
                      GROUP BY external_id_post_evar, post_visid_concatenated, visit_num 
                      ')

createOrReplaceTempView(appwsc_orari_6, 'appwsc_orari_6')

appwsc_orari_7 <- sql('SELECT external_id_post_evar,
                      SUM(count_appwsc_notte) AS visite_appwsc_notte,
                      SUM(count_appwsc_mattina) AS visite_appwsc_mattina,
                      SUM(count_appwsc_pomeriggio) AS visite_appwsc_pomeriggio,
                      SUM(count_appwsc_sera) AS visite_appwsc_sera
                      FROM appwsc_orari_6
                      GROUP BY external_id_post_evar
                      ')

createOrReplaceTempView(appwsc_orari_7, "appwsc_orari_7")

join4_orari <- sql("select DISTINCT DF.*, W.visite_appwsc_notte, W.visite_appwsc_mattina, W.visite_appwsc_pomeriggio, W.visite_appwsc_sera
                   from df_variabile_19 DF
                   left join appwsc_orari_7 W
                   on DF.ext_id = W.external_id_post_evar")

write.parquet(join4_orari, path_df_variabile_20, mode = "overwrite")

df_variabile_20 <- read.parquet(path_df_variabile_20)

createOrReplaceTempView(df_variabile_20, 'df_variabile_20')

##### CREAZIONE variabili 119-122: visite nelle fasce orarie appgtv ####

appgtv_orari <- select(appgtv_new, "external_id_post_evar",
                       "visit_num",
                       "post_visid_concatenated",
                       "date_time_ts")

createOrReplaceTempView(appgtv_orari, "appgtv_orari")

appgtv_orari_2 <- withColumn(appgtv_orari, "visit_hour", hour(appgtv_orari$date_time_ts))

createOrReplaceTempView(appgtv_orari_2, "appgtv_orari_2")

appgtv_orari_3 <- sql("select distinct appgtv_orari_2.external_id_post_evar, appgtv_orari_2.post_visid_concatenated, appgtv_orari_2.visit_num, visit_hour 
                      from appgtv_orari_2
                      where external_id_post_evar is not null")

appgtv_orari_4 <- withColumn(appgtv_orari_3, "time_slot", ifelse(appgtv_orari_3$visit_hour>=0 & appgtv_orari_3$visit_hour<6, "notte", 
                                                                 ifelse(appgtv_orari_3$visit_hour>=6 & appgtv_orari_3$visit_hour<12, "mattina", 
                                                                        ifelse(appgtv_orari_3$visit_hour>=12 & appgtv_orari_3$visit_hour<18, "pomeriggio", 
                                                                               ifelse(appgtv_orari_3$visit_hour>=18 & appgtv_orari_3$visit_hour<=23, "sera", NA)))))

createOrReplaceTempView(appgtv_orari_4, "appgtv_orari_4")

appgtv_orari_5 <- sql('SELECT *,
                      CASE WHEN (time_slot=="notte") then 1 else Null end as flag_appgtv_notte,
                      CASE WHEN (time_slot=="mattina") THEN 1 ELSE Null END AS flag_appgtv_mattina,
                      CASE WHEN (time_slot=="pomeriggio") then 1 else Null end as flag_appgtv_pomeriggio,
                      CASE WHEN (time_slot=="sera") THEN 1 ELSE Null END AS flag_appgtv_sera
                      FROM appgtv_orari_4')

createOrReplaceTempView(appgtv_orari_5, 'appgtv_orari_5')

appgtv_orari_6 <- sql('SELECT external_id_post_evar, post_visid_concatenated, visit_num,
                      COUNT(DISTINCT(flag_appgtv_notte)) AS count_appgtv_notte,
                      COUNT(DISTINCT(flag_appgtv_mattina)) AS count_appgtv_mattina,
                      COUNT(DISTINCT(flag_appgtv_pomeriggio)) AS count_appgtv_pomeriggio,
                      COUNT(DISTINCT(flag_appgtv_sera)) AS count_appgtv_sera
                      FROM appgtv_orari_5
                      GROUP BY external_id_post_evar, post_visid_concatenated, visit_num 
                      ')

createOrReplaceTempView(appgtv_orari_6, 'appgtv_orari_6')

appgtv_orari_7 <- sql('SELECT external_id_post_evar,
                      SUM(count_appgtv_notte) AS visite_appgtv_notte,
                      SUM(count_appgtv_mattina) AS visite_appgtv_mattina,
                      SUM(count_appgtv_pomeriggio) AS visite_appgtv_pomeriggio,
                      SUM(count_appgtv_sera) AS visite_appgtv_sera
                      FROM appgtv_orari_6
                      GROUP BY external_id_post_evar
                      ')

createOrReplaceTempView(appgtv_orari_7, "appgtv_orari_7")

join5_orari <- sql("select DISTINCT DF.*, G.visite_appgtv_notte, G.visite_appgtv_mattina, G.visite_appgtv_pomeriggio, G.visite_appgtv_sera
                   from df_variabile_20 DF
                   left join appgtv_orari_7 G
                   on DF.ext_id = G.external_id_post_evar")

write.parquet(join5_orari, path_df_variabile_21, mode = "overwrite")

df_variabile_21 <- read.parquet(path_df_variabile_21)

createOrReplaceTempView(df_variabile_21, 'df_variabile_21')

##### CREAZIONE variabili 123-126: visite nelle fasce orarie appsport ####

appsport_orari <- select(appsport_new, "external_id_post_evar",
                         "visit_num",
                         "post_visid_concatenated",
                         "date_time_ts")

createOrReplaceTempView(appsport_orari, "appsport_orari")

appsport_orari_2 <- withColumn(appsport_orari, "visit_hour", hour(appsport_orari$date_time_ts))

createOrReplaceTempView(appsport_orari_2, "appsport_orari_2")

appsport_orari_3 <- sql("select distinct appsport_orari_2.external_id_post_evar, appsport_orari_2.post_visid_concatenated, appsport_orari_2.visit_num, visit_hour 
                        from appsport_orari_2
                        where external_id_post_evar is not null")

appsport_orari_4 <- withColumn(appsport_orari_3, "time_slot", ifelse(appsport_orari_3$visit_hour>=0 & appsport_orari_3$visit_hour<6, "notte", 
                                                                     ifelse(appsport_orari_3$visit_hour>=6 & appsport_orari_3$visit_hour<12, "mattina", 
                                                                            ifelse(appsport_orari_3$visit_hour>=12 & appsport_orari_3$visit_hour<18, "pomeriggio", 
                                                                                   ifelse(appsport_orari_3$visit_hour>=18 & appsport_orari_3$visit_hour<=23, "sera", NA)))))

createOrReplaceTempView(appsport_orari_4, "appsport_orari_4")

appsport_orari_5 <- sql('SELECT *,
                        CASE WHEN (time_slot=="notte") then 1 else Null end as flag_appsport_notte,
                        CASE WHEN (time_slot=="mattina") THEN 1 ELSE Null END AS flag_appsport_mattina,
                        CASE WHEN (time_slot=="pomeriggio") then 1 else Null end as flag_appsport_pomeriggio,
                        CASE WHEN (time_slot=="sera") THEN 1 ELSE Null END AS flag_appsport_sera
                        FROM appsport_orari_4')

createOrReplaceTempView(appsport_orari_5, 'appsport_orari_5')

appsport_orari_6 <- sql('SELECT external_id_post_evar, post_visid_concatenated, visit_num,
                        COUNT(DISTINCT(flag_appsport_notte)) AS count_appsport_notte,
                        COUNT(DISTINCT(flag_appsport_mattina)) AS count_appsport_mattina,
                        COUNT(DISTINCT(flag_appsport_pomeriggio)) AS count_appsport_pomeriggio,
                        COUNT(DISTINCT(flag_appsport_sera)) AS count_appsport_sera
                        FROM appsport_orari_5
                        GROUP BY external_id_post_evar, post_visid_concatenated, visit_num 
                        ')

createOrReplaceTempView(appsport_orari_6, 'appsport_orari_6')

appsport_orari_7 <- sql('SELECT external_id_post_evar,
                        SUM(count_appsport_notte) AS visite_appsport_notte,
                        SUM(count_appsport_mattina) AS visite_appsport_mattina,
                        SUM(count_appsport_pomeriggio) AS visite_appsport_pomeriggio,
                        SUM(count_appsport_sera) AS visite_appsport_sera
                        FROM appsport_orari_6
                        GROUP BY external_id_post_evar
                        ')

createOrReplaceTempView(appsport_orari_7, "appsport_orari_7")

join6_orari <- sql("select DISTINCT DF.*, ASP.visite_appsport_notte, ASP.visite_appsport_mattina, ASP.visite_appsport_pomeriggio, ASP.visite_appsport_sera
                   from df_variabile_21 DF
                   left join appsport_orari_7 ASP
                   on DF.ext_id = ASP.external_id_post_evar")

write.parquet(join6_orari, path_df_variabile_22, mode = "overwrite")

df_variabile_22 <- read.parquet(path_df_variabile_22)

createOrReplaceTempView(df_variabile_22, 'df_variabile_22')

##### CREAZIONE variabili 127-130: visite nelle fasce orarie appxfactor ####

appxfactor_orari <- select(appxfactor_new, "external_id_post_evar",
                           "visit_num",
                           "post_visid_concatenated",
                           "date_time_ts")

createOrReplaceTempView(appxfactor_orari, "appxfactor_orari")

appxfactor_orari_2 <- withColumn(appxfactor_orari, "visit_hour", hour(appxfactor_orari$date_time_ts))

createOrReplaceTempView(appxfactor_orari_2, "appxfactor_orari_2")

appxfactor_orari_3 <- sql("select distinct appxfactor_orari_2.external_id_post_evar, appxfactor_orari_2.post_visid_concatenated, appxfactor_orari_2.visit_num, visit_hour 
                          from appxfactor_orari_2
                          where external_id_post_evar is not null")

appxfactor_orari_4 <- withColumn(appxfactor_orari_3, "time_slot", ifelse(appxfactor_orari_3$visit_hour>=0 & appxfactor_orari_3$visit_hour<6, "notte", 
                                                                         ifelse(appxfactor_orari_3$visit_hour>=6 & appxfactor_orari_3$visit_hour<12, "mattina", 
                                                                                ifelse(appxfactor_orari_3$visit_hour>=12 & appxfactor_orari_3$visit_hour<18, "pomeriggio", 
                                                                                       ifelse(appxfactor_orari_3$visit_hour>=18 & appxfactor_orari_3$visit_hour<=23, "sera", NA)))))

createOrReplaceTempView(appxfactor_orari_4, "appxfactor_orari_4")

appxfactor_orari_5 <- sql('SELECT *,
                          CASE WHEN (time_slot=="notte") then 1 else Null end as flag_appxfactor_notte,
                          CASE WHEN (time_slot=="mattina") THEN 1 ELSE Null END AS flag_appxfactor_mattina,
                          CASE WHEN (time_slot=="pomeriggio") then 1 else Null end as flag_appxfactor_pomeriggio,
                          CASE WHEN (time_slot=="sera") THEN 1 ELSE Null END AS flag_appxfactor_sera
                          FROM appxfactor_orari_4')

createOrReplaceTempView(appxfactor_orari_5, 'appxfactor_orari_5')

appxfactor_orari_6 <- sql('SELECT external_id_post_evar, post_visid_concatenated, visit_num,
                          COUNT(DISTINCT(flag_appxfactor_notte)) AS count_appxfactor_notte,
                          COUNT(DISTINCT(flag_appxfactor_mattina)) AS count_appxfactor_mattina,
                          COUNT(DISTINCT(flag_appxfactor_pomeriggio)) AS count_appxfactor_pomeriggio,
                          COUNT(DISTINCT(flag_appxfactor_sera)) AS count_appxfactor_sera
                          FROM appxfactor_orari_5
                          GROUP BY external_id_post_evar, post_visid_concatenated, visit_num 
                          ')

createOrReplaceTempView(appxfactor_orari_6, 'appxfactor_orari_6')

appxfactor_orari_7 <- sql('SELECT external_id_post_evar,
                          SUM(count_appxfactor_notte) AS visite_appxfactor_notte,
                          SUM(count_appxfactor_mattina) AS visite_appxfactor_mattina,
                          SUM(count_appxfactor_pomeriggio) AS visite_appxfactor_pomeriggio,
                          SUM(count_appxfactor_sera) AS visite_appxfactor_sera
                          FROM appxfactor_orari_6
                          GROUP BY external_id_post_evar
                          ')

createOrReplaceTempView(appxfactor_orari_7, "appxfactor_orari_7")

join7_orari <- sql("select DISTINCT DF.*, XF.visite_appxfactor_notte, XF.visite_appxfactor_mattina, XF.visite_appxfactor_pomeriggio, XF.visite_appxfactor_sera
                       from df_variabile_22 DF
                       left join appxfactor_orari_7 XF
                       on DF.ext_id = XF.external_id_post_evar")

# df_variabile_23 <- filter(join7_orari, join7_orari$numero_reportsuite != 0) ############## SPERIAMO CHE NON FILTRI NESSUNA RIGA!!!!

write.parquet(df_variabile_23, path_df_variabile_23, mode = "overwrite")

# df_variabile_23 <- read.parquet(path_df_variabile_23)



