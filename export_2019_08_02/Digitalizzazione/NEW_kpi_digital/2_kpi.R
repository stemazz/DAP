
# Digitalizzazione script 2, creazione variabili

source("connection_R.R")
options(scipen = 10000)


# external id corporate

scarico <- read.parquet(path_scarico_skyitdev) # leggo lo scarico skyitdev (corporate, tg24, sport)

scarico_new <- filter(scarico, (scarico$post_channel=='corporate'| scarico$post_channel== 'Guidatv' | scarico$post_channel=='tg24' | scarico$post_channel=='sport')) 

external_id_scarico <- distinct(select(scarico_new, scarico_new$external_id_post_evar)) # seleziono dallo scarico gli ext id distinti

# external id App WSC

appwsc <- read.parquet(path_scarico_appwsc) # leggo scarico app wsc

appwsc_new <- filter(appwsc, appwsc$date_time_dt <= data_fine) # filtro data appwsc

external_id_appwsc <- distinct(select(appwsc_new, appwsc_new$external_id_post_evar)) # seleziono dallo scarico gli ext id distinti

# external id App Guida Tv

appgtv <- read.parquet(path_scarico_appgtv)

appgtv_new <- filter(appgtv, appgtv$date_time_dt <= data_fine)

external_id_appguidatv <- distinct(select(appgtv_new, appgtv_new$external_id_post_evar))

# external id App Sport

appsport <- read.parquet(path_scarico_appsport)

appsport_new <- filter(appsport, appsport$date_time_dt <= data_fine)

external_id_appsport <- distinct(select(appsport_new, appsport_new$external_id_post_evar))

# external id App Xfactor

appxfactor <- read.parquet(path_scarico_appxf)

appxfactor_new <- filter(appxfactor, appxfactor$date_time_dt <= data_fine)

external_id_appxfactor <- distinct(select(appxfactor_new, appxfactor_new$external_id_post_evar))



# join numero 1: scarico skyitdev + app wsc

createOrReplaceTempView(external_id_scarico,'external_id_scarico')

createOrReplaceTempView(external_id_appwsc,'external_id_appwsc')

join1 <- sql('SELECT DISTINCT S.external_id_post_evar AS ext_id_corporate, W.external_id_post_evar AS ext_id_wsc
             FROM external_id_scarico S
             FULL JOIN external_id_appwsc W
             ON S.external_id_post_evar = W.external_id_post_evar')

join1$ext_id_join1 <- ifelse(isNull(join1$ext_id_corporate), join1$ext_id_wsc, join1$ext_id_corporate)



# join numero 2: join 1 + app guida tv

createOrReplaceTempView(join1,'join1')

createOrReplaceTempView(external_id_appguidatv,'external_id_appguidatv')

join2 <- sql('SELECT DISTINCT J.ext_id_join1, G.external_id_post_evar AS ext_id_gtv
             FROM join1 J
             FULL JOIN external_id_appguidatv G
             ON J.ext_id_join1 = G.external_id_post_evar')

join2$ext_id_join2 <- ifelse(isNull(join2$ext_id_join1), join2$ext_id_gtv, join2$ext_id_join1)



#join numero 2: join2 + app sport

createOrReplaceTempView(join2,'join2')

createOrReplaceTempView(external_id_appsport,'external_id_appsport')

join3 <- sql('SELECT DISTINCT J2.ext_id_join2, ASP.external_id_post_evar AS ext_id_appsport
             FROM join2 J2
             FULL JOIN external_id_appsport ASP
             ON J2.ext_id_join2 = ASP.external_id_post_evar')

join3$ext_id_join3 <- ifelse(isNull(join3$ext_id_join2), join3$ext_id_appsport, join3$ext_id_join2)



#join numero 3: join 2 + xfactor:

createOrReplaceTempView(join3,'join3')

createOrReplaceTempView(external_id_appxfactor,'external_id_appxfactor')

join4 <- sql('SELECT DISTINCT J3.ext_id_join3, XF.external_id_post_evar AS ext_id_xfactor
             FROM join3 J3
             FULL JOIN external_id_appxfactor XF
             ON J3.ext_id_join3 = XF.external_id_post_evar')

join4$ext_id_join4 <- ifelse(isNull(join4$ext_id_join3), join4$ext_id_xfactor, join4$ext_id_join3)



# Seleziono solo gli external id del join finale numero 4, dove tutti gli external id sono stati raggrupati con le join

ext_id_distinti <- distinct(select(join4, join4$ext_id_join4))
# pulizia sporcature
ext_id_senza_na <- filter(ext_id_distinti, ext_id_distinti$ext_id_join4 != 'null' | ext_id_distinti$ext_id_join4 !='n-a' | isNotNull(ext_id_distinti$ext_id_join4)) 

write.parquet(ext_id_distinti, path_ext_id_distinti_senza_NA)


###### Preparazione VARIABILI 2-8  ######

# corporate

corporate <- read.parquet(path_scarico_corporate) # leggo scarico corporate

corporate_new <- filter(corporate, corporate$date_time_dt <= data_fine)

corporate_1 <- withColumn(corporate_new, "month", month(corporate_new$date_time_dt)) # creo la colonna month

# creo una tabella con il conto delle visite uniche per mese per external id
corporate_2 <- summarize(groupBy(corporate_1, corporate_1$external_id_post_evar, corporate_1$month), 
                                 count = countDistinct(concat(corporate_1$post_visid_concatenated, corporate_1$visit_num)))

corporate_2$mesi_trovato <- ifelse((corporate_2$count > 0), 1, 0) # Indice: 0 = non trovato, 1 = trovato.

# sommando i mesi in cui ?? stato trovato ho il totale di mesi diversi in cui un ext id ?? andato su corporate
corporate_3 <- summarize(groupBy(corporate_2, corporate_2$external_id_post_evar), numero_mesi_corporate = sum(corporate_2$mesi_trovato)) 


# tg24:

tg24 <- read.parquet(path_scarico_tg24) # leggo scarico tg24

tg24_new <- filter(tg24, tg24$date_time_dt <= data_fine) #filtro temporale

tg24_1 <- withColumn(tg24_new, "month", month(tg24_new$date_time_dt)) # aggiungo mese

# creo una tabella con il conto delle visite uniche per mese per external id 
tg24_2 <- summarize(groupBy(tg24_1, tg24_1$external_id_post_evar,tg24_1$month),count = countDistinct(concat(tg24_1$post_visid_concatenated, tg24_1$visit_num)))

tg24_2$mesi_trovato <- ifelse((tg24_2$count > 0), 1, 0) # Indice: 0 = non trovato, 1 = trovato.

# sommando i mesi in cui ?? stato trovato ho il totale di mesi diversi in cui un ext id ?? andato su tg24
tg24_3 <- summarize(groupBy(tg24_2, tg24_2$external_id_post_evar), numero_mesi_tg24 = sum(tg24_2$mesi_trovato)) 


# sport:

sport <- read.parquet(path_scarico_sport)

sport_new <- filter(sport, sport$date_time_dt<=data_fine)

sport_1 <- withColumn(sport_new, "month", month(sport_new$date_time_dt)) # aggiungo mese

# creo una tabella con il conto delle visite uniche per mese per external id 
sport_2 <- summarize(groupBy(sport_1, sport_1$external_id_post_evar, sport_1$month), 
                     count = countDistinct(concat(sport_1$post_visid_concatenated, sport_1$visit_num))) 

sport_2$mesi_trovato <- ifelse((sport_2$count > 0), 1, 0) # Indice: 0 = non trovato, 1 = trovato.

# sommando i mesi in cui ?? stato trovato ho il totale di mesi diversi in cui un ext id ?? andato su sport
sport_3 <- summarize(groupBy(sport_2, sport_2$external_id_post_evar), numero_mesi_sport = sum(sport_2$mesi_trovato)) 


# app wsc:

appwsc <- read.parquet(path_scarico_appwsc) 

appwsc_1 <- withColumn(appwsc_new, "month", month(appwsc_new$date_time_dt))

appwsc_2 <- summarize(groupBy(appwsc_1, appwsc_1$external_id_post_evar, appwsc_1$month), 
                      count = countDistinct(concat(appwsc_1$post_visid_concatenated, appwsc_1$visit_num)))

appwsc_2$mesi_trovato <- ifelse((appwsc_2$count > 0), 1,0)

appwsc_3 <- summarize(groupBy(appwsc_2, appwsc_2$external_id_post_evar), numero_mesi_appwsc = sum(appwsc_2$mesi_trovato))


# app gtv:

appgtv <- read.parquet(path_scarico_appgtv) 

appgtv_1 <- withColumn(appgtv_new, "month", month(appgtv_new$date_time_dt))

appgtv_2 <- summarize(groupBy(appgtv_1, appgtv_1$external_id_post_evar,appgtv_1$month), 
                      count = countDistinct(concat(appgtv_1$post_visid_concatenated, appgtv_1$visit_num)))

appgtv_2$mesi_trovato <- ifelse((appgtv_2$count > 0), 1,0)

appgtv_3 <- summarize(groupBy(appgtv_2, appgtv_2$external_id_post_evar), numero_mesi_appgtv = sum(appgtv_2$mesi_trovato))


# app sport:

appsport <- read.parquet(path_scarico_appsport) 

appsport_1 <- withColumn(appsport_new, "month", month(appsport_new$date_time_dt))

appsport_2 <- summarize(groupBy(appsport_1, appsport_1$external_id_post_evar,appsport_1$month), 
                        count = countDistinct(concat(appsport_1$post_visid_concatenated, appsport_1$visit_num)))

appsport_2$mesi_trovato <- ifelse((appsport_2$count > 0), 1,0)

appsport_3 <- summarize(groupBy(appsport_2, appsport_2$external_id_post_evar), numero_mesi_appsport = sum(appsport_2$mesi_trovato))


# app xfactor:

appxfactor_1 <- withColumn(appxfactor_new, "month", month(appxfactor_new$date_time_dt))

appxfactor_2 <- summarize(groupBy(appxfactor_1, appxfactor_1$external_id_post_evar,appxfactor_1$month), 
                          count = countDistinct(concat(appxfactor_1$post_visid_concatenated, appxfactor_1$visit_num)))

appxfactor_2$mesi_trovato <- ifelse((appxfactor_2$count > 0), 1,0)

appxfactor_3 <- summarize(groupBy(appxfactor_2, appxfactor_2$external_id_post_evar), numero_mesi_appxfactor = sum(appxfactor_2$mesi_trovato))



ext_id_senza_na <- read.parquet(path_ext_id_distinti_senza_NA) # rileggo il parquet con gli external id uniti da tutte le reportsuite

createOrReplaceTempView(ext_id_senza_na, 'ext_id_senza_na')

##### CREAZIONE VARIABILE 2: 'numero_mesi_corporate' ####

createOrReplaceTempView(corporate_3, 'corporate_3')

join1 <- sql('SELECT DISTINCT E.ext_id_join4 as ext_id, C.numero_mesi_corporate
             FROM ext_id_senza_na E
             LEFT JOIN corporate_3 C
             ON E.ext_id_join4 = C.external_id_post_evar') 

createOrReplaceTempView(join1, 'join1')

##### CREAZIONE VARIABILE 3: 'numero_mesi_tg24' ####

createOrReplaceTempView(tg24_3, 'tg24_3')

join2 <- sql('SELECT DISTINCT J.*, TG.numero_mesi_tg24
             FROM join1 J
             LEFT JOIN tg24_3 TG
             ON J.ext_id = TG.external_id_post_evar')

createOrReplaceTempView(join2, 'join2')

##### CREAZIONE VARIABILE 4: 'numero_mesi_sport' ####

createOrReplaceTempView(sport_3, 'sport_3')

join3 <- sql('SELECT DISTINCT J2.*, SP.numero_mesi_sport
             FROM join2 J2
             LEFT JOIN sport_3 SP
             ON J2.ext_id = SP.external_id_post_evar')

createOrReplaceTempView(join3,'join3')

##### CREAZIONE VARIABILE 5: 'numero_mesi_appwsc' ####

createOrReplaceTempView(appwsc_3, 'appwsc_3')

join4 <- sql('SELECT DISTINCT J3.*, W.numero_mesi_appwsc
             FROM join3 J3
             LEFT JOIN appwsc_3 W
             ON J3.ext_id = W.external_id_post_evar')

createOrReplaceTempView(join4, 'join4')

##### CREAZIONE VARIABILE 6: 'numero_mesi_appgtv' ####

createOrReplaceTempView(appgtv_3, 'appgtv_3')

join5 <- sql('SELECT DISTINCT J4.*, G.numero_mesi_appgtv
             FROM join4 J4
             LEFT JOIN appgtv_3 G
             ON J4.ext_id = G.external_id_post_evar')

createOrReplaceTempView(join5, 'join5')

##### CREAZIONE VARIABILE 7: 'numero_mesi_appsport' ####

createOrReplaceTempView(appsport_3, 'appsport_3')

join6 <- sql('SELECT DISTINCT J5.*, ASP.numero_mesi_appsport
             FROM join5 J5
             LEFT JOIN appsport_3 ASP
             ON J5.ext_id = ASP.external_id_post_evar')

createOrReplaceTempView(join6,'join6')

##### CREAZIONE VARIABILE 8: 'numero_mesi_appxfactor' ####

createOrReplaceTempView(appxfactor_3,'appxfactor_3')

join7 <- sql('SELECT DISTINCT J6.*, XF.numero_mesi_appxfactor
             FROM join6 J6
             LEFT JOIN appxfactor_3 XF
             ON J6.ext_id = XF.external_id_post_evar')

# write file con tutte le prime 8 variabili

write.parquet(join7, path_df_variabile_1) #, mode = "overwrite"

df_variabile_1 <- read.parquet(path_df_variabile_1)


##### Preparazione VARIABILI 9-15 ####

# corporate

visite_uniche_corporate <- select(corporate_new,
                                  "external_id_post_evar",
                                  "post_visid_concatenated",
                                  "visit_num") # seleziono solo external id, cookie, visit num

# concatenazione visit num e cookie per ottenere numero visite e aggregazione per external_id
visite_uniche_corporate_conc <- summarize(groupBy(visite_uniche_corporate, "external_id_post_evar"), 
                                                  count = countDistinct(concat(visite_uniche_corporate$post_visid_concatenated, visite_uniche_corporate$visit_num))) 

# tg24

visite_uniche_tg24 <- select(tg24_new,
                             "external_id_post_evar",
                             "post_visid_concatenated",
                             "visit_num")

# concatenazione visit num e cookie per ottenere numero visite e aggregazione per external_id
visite_uniche_tg24_conc <- summarize(groupBy(visite_uniche_tg24, "external_id_post_evar"), 
                                     count = countDistinct(concat(visite_uniche_tg24$post_visid_concatenated, visite_uniche_tg24$visit_num)))

# sport

visite_uniche_sport <- select(sport_new,
                              "external_id_post_evar",
                              "post_visid_concatenated",
                              "visit_num")

# concatenazione visit num e cookie per ottenere numero visite e aggregazione per external_id
visite_uniche_sport_conc <- summarize(groupBy(visite_uniche_sport, "external_id_post_evar"), 
                                      count = countDistinct(concat(visite_uniche_sport$post_visid_concatenated, visite_uniche_sport$visit_num))) 

# appwsc

visite_uniche_appwsc <- select(appwsc_new,
                               "external_id_post_evar",
                               "post_visid_concatenated",
                               "visit_num")

visite_uniche_appwsc_conc <- summarize(groupBy(visite_uniche_appwsc, "external_id_post_evar"), 
                                       count = countDistinct(concat(visite_uniche_appwsc$post_visid_concatenated, visite_uniche_appwsc$visit_num)))

# appgtv

visite_uniche_appgtv <- select(appgtv_new,
                               "external_id_post_evar",
                               "post_visid_concatenated",
                               "visit_num")

visite_uniche_appgtv_conc <- summarize(groupBy(visite_uniche_appgtv, "external_id_post_evar"), 
                                       count = countDistinct(concat(visite_uniche_appgtv$post_visid_concatenated, visite_uniche_appgtv$visit_num)))

# appsport

visite_uniche_appsport <- select(appsport_new,
                                 "external_id_post_evar",
                                 "post_visid_concatenated",
                                 "visit_num")

visite_uniche_appsport_conc <- summarize(groupBy(visite_uniche_appsport, "external_id_post_evar"), 
                                         count = countDistinct(concat(visite_uniche_appsport$post_visid_concatenated, visite_uniche_appsport$visit_num)))

# appxfactor

visite_uniche_appxfactor <- select(appxfactor_new,
                                   "external_id_post_evar",
                                   "post_visid_concatenated",
                                   "visit_num")

visite_uniche_appxfactor_conc <- summarize(groupBy(visite_uniche_appxfactor, "external_id_post_evar"), 
                                           count = countDistinct(concat(visite_uniche_appxfactor$post_visid_concatenated, visite_uniche_appxfactor$visit_num)))

createOrReplaceTempView(visite_uniche_corporate_conc, "visite_uniche_corporate_conc")
createOrReplaceTempView(visite_uniche_tg24_conc, "visite_uniche_tg24_conc")
createOrReplaceTempView(visite_uniche_sport_conc, "visite_uniche_sport_conc")
createOrReplaceTempView(visite_uniche_appwsc_conc, "visite_uniche_appwsc_conc")
createOrReplaceTempView(visite_uniche_appgtv_conc, "visite_uniche_appgtv_conc")
createOrReplaceTempView(visite_uniche_appsport_conc, "visite_uniche_appsport_conc")
createOrReplaceTempView(visite_uniche_appxfactor_conc, "visite_uniche_appxfactor_conc")

createOrReplaceTempView(df_variabile_1, "df_variabile_1")

# Left join con external_id e tutte le altre tabelle per comporre la tabella degli utilizzatori

##### CREAZIONE VARIABILE 9: 'visite_totali_corporate' ####

utilizzatori1 <- sql("select DISTINCT DF.*, C.count as visite_totali_corporate
                     from df_variabile_1 DF
                     left join visite_uniche_corporate_conc C
                     on DF.ext_id = C.external_id_post_evar")

createOrReplaceTempView(utilizzatori1, "utilizzatori1")

##### CREAZIONE VARIABILE 10: 'visite_totali_tg24' ####

utilizzatori2 <- sql("select DISTINCT UT.*, T.count as visite_totali_tg24
                     from utilizzatori1 UT
                     left join visite_uniche_tg24_conc T
                     on UT.ext_id = T.external_id_post_evar")

createOrReplaceTempView(utilizzatori2, "utilizzatori2")

##### CREAZIONE VARIABILE 11: 'visite_totali_sport' ####

utilizzatori3 <- sql("select DISTINCT UT.*, S.count as visite_totali_sport
                     from utilizzatori2 UT
                     left join visite_uniche_sport_conc S
                     on UT.ext_id = S.external_id_post_evar")

createOrReplaceTempView(utilizzatori3, "utilizzatori3")

##### CREAZIONE VARIABILE 12: 'visite_totali_appwsc' ####

utilizzatori4 <- sql("select DISTINCT UT.*, W.count as visite_totali_appwsc
                     from utilizzatori3 UT
                     left join visite_uniche_appwsc_conc W
                     on UT.ext_id = W.external_id_post_evar")

createOrReplaceTempView(utilizzatori4, "utilizzatori4")

##### CREAZIONE VARIABILE 13: 'visite_totali_appgtv' ####

utilizzatori5 <- sql("select DISTINCT UT.*, G.count as visite_totali_appgtv
                     from utilizzatori4 UT
                     left join visite_uniche_appgtv_conc G
                     on UT.ext_id = G.external_id_post_evar")

createOrReplaceTempView(utilizzatori5, "utilizzatori5")

##### CREAZIONE VARIABILE 14: 'visite_totali_appsport' ####

utilizzatori6 <- sql("select DISTINCT UT.*, ASP.count as visite_totali_appsport
                     from utilizzatori5 UT
                     left join visite_uniche_appsport_conc ASP
                     on UT.ext_id = ASP.external_id_post_evar")

createOrReplaceTempView(utilizzatori6, "utilizzatori6")

##### CREAZIONE VARIABILE 15: 'visite_totali_appxfactor' ####

utilizzatori7 <- sql("select DISTINCT UT.*, XF.count as visite_totali_appxfactor
                     from utilizzatori6 UT
                     left join visite_uniche_appxfactor_conc XF
                     on UT.ext_id = XF.external_id_post_evar") 

# write utilizzatori 7 che contiene le variabili da 1 a 15

write.parquet(utilizzatori7, path_df_variabile_2, mode = "overwrite")

df_variabile_2 <- read.parquet(path_df_variabile_2)

##### CREAZIONE VARIABILE 16: 'numero_reportsuite' ####

df_na <- fillna(df_variabile_1, 0) # sostituzione na con 0 per il df_variabile_1 ( che contiene le variabili da 1-8)

# nrow(df_na) # ext.id
# creazione colonne temporanee: 1 se l'ext. id ha utilizzato quella reportsuite almeno una volta, altrimenti 0. 
# La somma mi dar?? il numero di rs dove ?? stato visto l'ext.id negli ultimi 6 mesi

df_na$corporate <- ifelse(df_na$numero_mesi_corporate > 0, 1, 0) 

df_na$tg24 <- ifelse(df_na$numero_mesi_tg24 > 0, 1, 0)

df_na$sport <- ifelse(df_na$numero_mesi_sport > 0, 1, 0)

df_na$appwsc <- ifelse(df_na$numero_mesi_appwsc > 0, 1, 0)

df_na$appsport <- ifelse(df_na$numero_mesi_appsport > 0, 1, 0)

df_na$appgtv <- ifelse(df_na$numero_mesi_appgtv > 0, 1, 0)

df_na$appxfactor <- ifelse(df_na$numero_mesi_appxfactor > 0, 1, 0)

createOrReplaceTempView(df_na, "df_na")

somma_df <- sql("select D.ext_id, sum(corporate + sport + tg24 + appwsc + appsport + appgtv + appxfactor) as numero_reportsuite
                from df_na D
                group by ext_id")


createOrReplaceTempView(df_variabile_2, "df_variabile_2")

createOrReplaceTempView(somma_df, "somma_df")

df_variabile_3 <- sql("select DISTINCT DF.*, S.numero_reportsuite
                      from df_variabile_2 DF
                      left join somma_df S
                      on DF.ext_id = S.ext_id")

write.parquet(df_variabile_3, path_df_variabile_3)

df_variabile_3 <- read.parquet(path_df_variabile_3)

##### Preparazione VARIABILI 17-24: cookie unici per external id ####

cookie_corporate <- summarize(groupBy(corporate_new, "external_id_post_evar"), 
                                      cookie_corporate = countDistinct(corporate_new$post_visid_concatenated))

cookie_tg24<- summarize(groupBy(tg24_new, "external_id_post_evar"), 
                        cookie_tg24 = countDistinct(tg24_new$post_visid_concatenated))

cookie_sport<- summarize(groupBy(sport_new, "external_id_post_evar"), 
                         cookie_sport = countDistinct(sport_new$post_visid_concatenated))

cookie_skyitdev<- summarize(groupBy(scarico_new, "external_id_post_evar"), 
                            cookie_skyitdev = countDistinct(scarico_new$post_visid_concatenated))

cookie_appwsc<- summarize(groupBy(appwsc_new, "external_id_post_evar"), 
                          cookie_appwsc = countDistinct(appwsc_new$post_visid_concatenated))

cookie_appgtv<- summarize(groupBy(appgtv_new, "external_id_post_evar"), 
                          cookie_appgtv = countDistinct(appgtv_new$post_visid_concatenated))

cookie_appsport<- summarize(groupBy(appsport_new, "external_id_post_evar"), 
                            cookie_appsport = countDistinct(appsport_new$post_visid_concatenated))

cookie_appxfactor<- summarize(groupBy(appxfactor_new, "external_id_post_evar"), 
                              cookie_appxfactor = countDistinct(appxfactor_new$post_visid_concatenated))

createOrReplaceTempView(cookie_corporate, "cookie_corporate")
createOrReplaceTempView(cookie_tg24, "cookie_tg24")
createOrReplaceTempView(cookie_sport, "cookie_sport")
createOrReplaceTempView(cookie_skyitdev, "cookie_skyitdev")
createOrReplaceTempView(cookie_appgtv, "cookie_appgtv")
createOrReplaceTempView(cookie_appxfactor,"cookie_appxfactor")
createOrReplaceTempView(cookie_appsport, "cookie_appsport")
createOrReplaceTempView(cookie_appwsc, "cookie_appwsc")

# variabili che contano i cookie unici per external id

##### CREAZIONE VARIABILE 17: 'cookie_corporate' ####

createOrReplaceTempView(df_variabile_3, "df_variabile_3")

cookie1 <- sql("select DISTINCT DF.*, C.cookie_corporate
               from df_variabile_3 DF
               left join cookie_corporate C
               on DF.ext_id = C.external_id_post_evar")

##### CREAZIONE VARIABILE 18: 'cookie_tg24' ####

createOrReplaceTempView(cookie1, "cookie1")

cookie2 <- sql("select DISTINCT cookie1.*, TG.cookie_tg24
               from cookie1
               left join cookie_tg24 TG
               on cookie1.ext_id = TG.external_id_post_evar")

##### CREAZIONE VARIABILE 19: 'cookie_sport' ####

createOrReplaceTempView(cookie2, "cookie2")

cookie3 <- sql("select DISTINCT cookie2.*, SP.cookie_sport
               from cookie2
               left join cookie_sport SP
               on cookie2.ext_id = SP.external_id_post_evar")

##### CREAZIONE VARIABILE 20: 'cookie_skyitdev' ####

createOrReplaceTempView(cookie3, "cookie3")

cookie4 <- sql("select DISTINCT cookie3.*, S.cookie_skyitdev
               from cookie3
               left join cookie_skyitdev S
               on cookie3.ext_id = S.external_id_post_evar")

##### CREAZIONE VARIABILE 21: 'cookie_appwsc' ####

createOrReplaceTempView(cookie4, "cookie4")

cookie5 <- sql("select DISTINCT cookie4.*, W.cookie_appwsc
               from cookie4
               left join cookie_appwsc W
               on cookie4.ext_id = W.external_id_post_evar")

##### CREAZIONE VARIABILE 22: 'cookie_appgtv' ####

createOrReplaceTempView(cookie5, "cookie5")

cookie6 <- sql("select DISTINCT cookie5.*, G.cookie_appgtv
               from cookie5
               left join cookie_appgtv G
               on cookie5.ext_id = G.external_id_post_evar")

##### CREAZIONE VARIABILE 23: 'cookie_appsport' ####

createOrReplaceTempView(cookie6, "cookie6")

cookie7 <- sql("select DISTINCT cookie6.*, ASP.cookie_appsport
               from cookie6
               left join cookie_appsport ASP
               on cookie6.ext_id = ASP.external_id_post_evar")

##### CREAZIONE VARIABILE 24: 'cookie_appxfactor' ####

createOrReplaceTempView(cookie7, "cookie7")

cookie8 <- sql("select DISTINCT cookie7.*, XF.cookie_appxfactor
               from cookie7
               left join cookie_appxfactor XF
               on cookie7.ext_id = XF.external_id_post_evar")

write.parquet(cookie8, path_df_variabile_4)

df_variabile_4 <- read.parquet(path_df_variabile_4)

##### CREAZIONE VARIABILE 25-26-27: 'count_visits_mobile', 'count_visits_desktop', 'count_visits_other' ####

createOrReplaceTempView(scarico_new,'scarico_new')

mobile_desktop <- sql('SELECT *,
                      CASE WHEN (mapped_os_value LIKE "Linux%" OR mapped_os_value LIKE "Window%" OR mapped_os_value LIKE "OS%" OR 
                                  mapped_os_value LIKE "Macintosh") THEN "desktop"
                      WHEN (mapped_os_value LIKE "Firefox%" OR mapped_os_value LIKE "%iPhone%" OR mapped_os_value LIKE "Chrome%" OR 
                            mapped_os_value LIKE "Tizen%" OR mapped_os_value LIKE "%Mobile%" OR mapped_os_value LIKE "Android%") THEN "mobile" 
                      ELSE "other" END AS device_type
                      FROM scarico_new') # converto manualmente i nomi dei sistemi operativi in desktop/mobile/other

createOrReplaceTempView(mobile_desktop,'mobile_desktop')

mobile_desktop1 <- sql('SELECT *,
                       CASE WHEN (device_type=="mobile") THEN 1 ELSE Null END AS flag_mobile,
                       CASE WHEN (device_type=="desktop") THEN 1 ELSE Null END AS flag_desktop,
                       CASE WHEN (device_type=="other") THEN 1 ELSE Null END AS flag_other
                       FROM mobile_desktop') # flag mobile, desktop, other

#aggiungo le visite uniche
mobile_desktop2 <- withColumn(mobile_desktop1, "visita_unica", concat(mobile_desktop1$post_visid_concatenated, mobile_desktop1$visit_num)) 

createOrReplaceTempView(mobile_desktop2, 'mobile_desktop2')

# raggruppo per ext.id e visita unica contando le visite da mobile, desktop, other
mobile_desktop3 <- sql('SELECT external_id_post_evar, visita_unica,
                       COUNT(DISTINCT(flag_mobile)) AS count_mobile,
                       COUNT(DISTINCT(flag_desktop)) AS count_desktop,
                       COUNT(DISTINCT(flag_other)) AS count_other
                       FROM mobile_desktop2
                       GROUP BY external_id_post_evar, visita_unica') 

createOrReplaceTempView(mobile_desktop3, 'mobile_desktop3')

mobile_desktop4 <- sql('SELECT external_id_post_evar,
                       SUM(count_mobile) AS count_visits_mobile,
                       SUM(count_desktop) AS count_visits_desktop,
                       SUM(count_other) AS count_visits_other
                       FROM mobile_desktop3
                       GROUP BY external_id_post_evar') # somma visite da desktop, mobile, other per external id

##### CREAZIONE VARIABILE 28: 'tipo_device' ####

# 7 categorie che incrociano l'uso: mobile, desktop, other. IBRIDO = uno che li ha usati tutti

mobile_desktop4$tipo_device <- ifelse((mobile_desktop4$count_visits_mobile > 0 & mobile_desktop4$count_visits_desktop==0 & mobile_desktop4$count_visits_other==0),'mobile',
                                      ifelse((mobile_desktop4$count_visits_mobile == 0 & mobile_desktop4$count_visits_desktop>0 & mobile_desktop4$count_visits_other==0),'desktop',
                                             ifelse((mobile_desktop4$count_visits_mobile == 0 & mobile_desktop4$count_visits_desktop==0 & mobile_desktop4$count_visits_other>0),'other',
                                                    ifelse((mobile_desktop4$count_visits_mobile > 0 & mobile_desktop4$count_visits_desktop>0 & mobile_desktop4$count_visits_other==0),'mobile_desktop',
                                                           ifelse((mobile_desktop4$count_visits_mobile > 0 & mobile_desktop4$count_visits_desktop==0 & mobile_desktop4$count_visits_other>0),'mobile_other',
                                                                  ifelse((mobile_desktop4$count_visits_mobile == 0 & mobile_desktop4$count_visits_desktop>0 & mobile_desktop4$count_visits_other>0),'desktop_other','ibrido'))))))

write.parquet(mobile_desktop4, path_mobile)

df_device <- read.parquet(path_mobile) # rileggo quello che ho appena scritto con mobile, desktop, other

createOrReplaceTempView(df_variabile_4,'df_variabile_4') # df_variabile4 ?? l'ultimo join con variabili 1-24 a cui unir?? df_device appena creato

createOrReplaceTempView(df_device,'df_device')

# join dataset con variabili 25-27
df_variabile_5 <- sql('SELECT DF.*, DEV.count_visits_mobile, DEV.count_visits_desktop, DEV.count_visits_other, DEV.tipo_device
                      FROM df_variabile_4 DF
                      LEFT JOIN df_device DEV
                      ON DF.ext_id=DEV.external_id_post_evar') 

df_variabile_5$tipo_device <- ifelse(isNull(df_variabile_5$tipo_device), 'mobile', df_variabile_5$tipo_device) # aggiungo anche la variabile 28

write.parquet(df_variabile_5, path_df_variabile_5)

df_variabile_5 <- read.parquet(path_df_variabile_5)

##### CREAZIONE VARIABILE 29: 'count_visits_from_social' ####

scarico_new2 <- withColumn(scarico_new, "visita_unica", concat(scarico_new$post_visid_concatenated, scarico_new$visit_num)) # aggiungo la visita unica allo scarico skyitdev

createOrReplaceTempView(scarico_new2, 'scarico_new2') 

# last_touch_label indica il sito da cui proviene l'external id, seleziono solo social network e paid social. 1 se proviene da social, 0 altrimenti.
social1 <- sql('SELECT *,
               CASE WHEN (last_touch_label=="social_network" OR last_touch_label=="paid_social") THEN 1 ELSE Null END AS social
               FROM scarico_new2') 

createOrReplaceTempView(social1, 'social1')

social2 <- sql('SELECT external_id_post_evar, visita_unica,
               COUNT(DISTINCT(social)) AS count_social
               FROM social1
               GROUP BY external_id_post_evar, visita_unica') 

createOrReplaceTempView(social2, 'social2')

# conto visite da social per external id sommando le singole visite da social
social3 <- sql('SELECT external_id_post_evar,
               SUM(count_social) AS count_visits_from_social
               FROM social2
               GROUP BY external_id_post_evar') 

##### CREAZIONE VARIABILE 30: 'social_network ####

# flag per chi ?? arrivato da social o meno
social3$social_network <- ifelse(social3$count_visits_from_social > 0, "social", "no_social") 

write.parquet(social3, path_social_network)

df_social <- read.parquet(path_social_network) # rileggo file con variabili 29, 30

createOrReplaceTempView(df_social, 'df_social')

createOrReplaceTempView(df_variabile_5,'df_variabile_5') # df con variabili da 1 a 28

df_variabile_6 <- sql('SELECT DF.*, S.count_visits_from_social, S.social_network
                      FROM df_variabile_5 DF
                      LEFT JOIN df_social S
                      ON DF.ext_id = S.external_id_post_evar')

write.parquet(df_variabile_6, path_df_variabile_6, mode = 'overwrite')

df_variabile_6 <-  read.parquet(path_df_variabile_6) # df con variabili da 1 a 30

createOrReplaceTempView(df_variabile_6, "df_variabile_6")

##### Preparazione VARIABILI 31-37: secondi totali passati su ciascuna reportsuite per ext id ####

##### VARIABILE 31: 'secondi_totali_corporate' ####

secondi_corporate <- tempo_in_pagina(corporate_new) # applico funzione tempo in pagina per skyitdev

secondi_corporate_2 <- summarize(groupBy(secondi_corporate, secondi_corporate$external_id_post_evar), 
                                 secondi_totali_corporate = sum(secondi_corporate$sec_on_page)) # ottengo df con secondi_totali_corporate per ext id

write.parquet(secondi_corporate_2, path_secondi_corporate)

path_secondi_corporate_2 <- read.parquet(path_secondi_corporate)

createOrReplaceTempView(path_secondi_corporate_2, "path_secondi_corporate_2")

##### VARIABILE 32: 'secondi_totali_tg24' ####

secondi_tg24 <- tempo_in_pagina(tg24_new)

secondi_tg24_2 <- summarize(groupBy(secondi_tg24, secondi_tg24$external_id_post_evar), secondi_totali_tg24 = sum(secondi_tg24$sec_on_page))

write.parquet(secondi_tg24_2, path_secondi_tg24)

path_secondi_tg24_2 <- read.parquet(path_secondi_tg24)

createOrReplaceTempView(path_secondi_tg24_2, "path_secondi_tg24_2")

##### VARIABILE 33: 'secondi_totali_sport' ####

secondi_sport <- tempo_in_pagina(sport_new) # applico funzione tempo in pagina per skyitdev

secondi_sport_2 <- summarize(groupBy(secondi_sport, secondi_sport$external_id_post_evar), secondi_totali_sport = sum(secondi_sport$sec_on_page)) # ottengo df con secondi_totali_sport per ext id

write.parquet(secondi_sport_2, path_secondi_sport)

path_secondi_sport_2 <- read.parquet(path_secondi_sport)

createOrReplaceTempView(path_secondi_sport_2, "path_secondi_sport_2")

##### VARIABILE 34: 'secondi_totali_appwsc' ####

secondi_appwsc <- tempo_in_pagina(appwsc_new3) 

secondi_appwsc_2 <- summarize(groupBy(secondi_appwsc, secondi_appwsc$external_id_post_evar), secondi_totali_appwsc = sum(secondi_appwsc$sec_on_page))

write.parquet(secondi_appwsc_2, path_secondi_appwsc)

path_secondi_appwsc_2 <- read.parquet(path_secondi_appwsc)

createOrReplaceTempView(path_secondi_appwsc_2, "path_secondi_appwsc_2")

##### VARIABILE 35: 'secondi_totali_appgtv' ####

secondi_appgtv <- tempo_in_pagina(appgtv_new3) 

secondi_appgtv_2 <- summarize(groupBy(secondi_appgtv, secondi_appgtv$external_id_post_evar), secondi_totali_appgtv = sum(secondi_appgtv$sec_on_page))

write.parquet(secondi_appgtv_2, path_secondi_appgtv)

path_secondi_appgtv_2 <- read.parquet(path_secondi_appgtv)

createOrReplaceTempView(path_secondi_appgtv_2, "path_secondi_appgtv_2")

##### VARIABILE 36: 'secondi_totali_appsport' ####

secondi_appsport <- tempo_in_pagina(appsport_new3) 

secondi_appsport_2 <- summarize(groupBy(secondi_appsport, secondi_appsport$external_id_post_evar), secondi_totali_appsport = sum(secondi_appsport$sec_on_page))

write.parquet(secondi_appsport_2, path_secondi_appsport)

path_secondi_appsport_2 <- read.parquet(path_secondi_appsport)

createOrReplaceTempView(path_secondi_appsport_2, "path_secondi_appsport_2")

##### VARIABILE 37: 'secondi_totali_appxfactor' ####

secondi_appxfactor <- tempo_in_pagina(appxfactor_new3) 

secondi_appxfactor_2 <- summarize(groupBy(secondi_appxfactor, secondi_appxfactor$external_id_post_evar), secondi_totali_appxfactor = sum(secondi_appxfactor$sec_on_page))

write.parquet(secondi_appxfactor_2, path_secondi_appxfactor)

path_secondi_appxfactor_2 <- read.parquet(path_secondi_appxfactor)

createOrReplaceTempView(path_secondi_appxfactor_2, "path_secondi_appxfactor_2")

##### JOIN VARIABILI 31-37 AL RESTO DEL DF ####

join1_secondi <-sql('SELECT DF.*, C.secondi_totali_corporate
                    FROM df_variabile_6 DF
                    LEFT JOIN path_secondi_corporate_2 C
                    ON DF.ext_id = C.external_id_post_evar')

createOrReplaceTempView(join1_secondi, 'join1_secondi')

join2_secondi <-sql('SELECT J.*, TG.secondi_totali_tg24
                    FROM join1_secondi J
                    LEFT JOIN path_secondi_tg24_2 TG
                    ON J.ext_id = TG.external_id_post_evar')

createOrReplaceTempView(join2_secondi, 'join2_secondi')

join3_secondi <-sql('SELECT J2.*, S.secondi_totali_sport
                    FROM join2_secondi J2
                    LEFT JOIN path_secondi_sport_2 S
                    ON J2.ext_id = S.external_id_post_evar')

createOrReplaceTempView(join3_secondi, 'join3_secondi')

join4_secondi <-sql('SELECT J3.*, W.secondi_totali_appwsc
                    FROM join3_secondi J3
                    LEFT JOIN path_secondi_appwsc_2 W
                    ON J3.ext_id = W.external_id_post_evar')

createOrReplaceTempView(join4_secondi, 'join4_secondi')

join5_secondi <-sql('SELECT J4.*, G.secondi_totali_appgtv
                    FROM join4_secondi J4
                    LEFT JOIN path_secondi_appgtv_2 G
                    ON J4.ext_id = G.external_id_post_evar')

createOrReplaceTempView(join5_secondi, 'join5_secondi')

join6_secondi <-sql('SELECT J5.*, ASP.secondi_totali_appsport
                    FROM join5_secondi J5
                    LEFT JOIN path_secondi_appsport_2 ASP
                    ON J5.ext_id = ASP.external_id_post_evar')

createOrReplaceTempView(join6_secondi, 'join6_secondi')

join7_secondi <-sql('SELECT J6.*, XF.secondi_totali_appxfactor
                    FROM join6_secondi J6
                    LEFT JOIN path_secondi_appxfactor_2 XF
                    ON J6.ext_id = XF.external_id_post_evar')

write.parquet(join7_secondi, path_df_variabile_7)

##### FINE PARTE 1: variabili 1-37 ####
