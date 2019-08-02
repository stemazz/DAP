
#Script per digitalizzazione numero 4

source("connection_R.R")
options(scipen = 10000)



df_variabile_23 <- read.parquet(path_df_variabile_23)

df_variabile_24 <- df_variabile_23 # file temporaneo per non fare paciughi


################## VERIFICA PERCENTILI ################## 

createOrReplaceTempView(df_variabile_24, "df_variabile_24")

# LE seguenti funzioni non necessitano di pulizia per loro stessa natura:

# numero_mesi_corporate
# numero_mesi_tg24
# numero_mesi_sport
# numero_mesi_appwsc
# numero_mesi_appgtv
# numero_mesi_appsport
# numero_mesi_appxfactor
# count_visits_mobile
# count_visits_desktop
# count_visits_other
# tipo_device
# social_network
# percentuale_visite_mobile
# percentuale_visite_desktop
# percentuale_visite_other
# ricorrenza_corporate
# ricorrenza_tg24
# ricorrenza_sport
# ricorrenza_appwsc
# ricorrenza_appgtv
# ricorrenza_appsport
# ricorrenza_appxfactor
# max_mesi_corporate_appwsc_appgtv
# max_mesi_editoriale
# max_mesi_web
# max_mesi_app
# ricorrenza_corp_appwsc_appgtv
# ricorrenza_editoriale
# ricorrenza_web
# ricorrenza_app


##### 9) visite_totali_corporate: ####


percentili_visite_totali_corporate <- sql("select percentile_approx(visite_totali_corporate, 0.99) as p99 
                                          from df_variabile_24 
                                          where visite_totali_corporate is NOT NULL")

novantanove_visite_totali_corporate <- first(percentili_visite_totali_corporate) # seleziono il p99

nn_visite_totali_corporate <- as.integer(novantanove_visite_totali_corporate) # lo trasformo in integer

df_variabile_24$visite_totali_corporate <- ifelse(df_variabile_24$visite_totali_corporate > nn_visite_totali_corporate , nn_visite_totali_corporate, df_variabile_24$visite_totali_corporate) # sostituisco ci?? che e' oltre il p99 col p99


##### 10) visite_totali_tg24: ####


percentili_visite_totali_tg24 <- sql("select percentile_approx(visite_totali_tg24, 0.99) as p99 
                                     from df_variabile_24 
                                     where visite_totali_tg24 is NOT NULL")

novantanove_visite_totali_tg24 <- first(percentili_visite_totali_tg24) # seleziono il p99

nn_visite_totali_tg24 <- as.integer(novantanove_visite_totali_tg24) # lo trasformo in integer

df_variabile_24$visite_totali_tg24 <- ifelse(df_variabile_24$visite_totali_tg24 > nn_visite_totali_tg24 , nn_visite_totali_tg24, df_variabile_24$visite_totali_tg24) # sostituisco ci?? che e' oltre il p99 col p99


##### 11) visite_totali_sport: ####


percentili_visite_totali_sport <- sql("select percentile_approx(visite_totali_sport, 0.99) as p99 
                                      from df_variabile_24 
                                      where visite_totali_sport is NOT NULL")

novantanove_visite_totali_sport <- first(percentili_visite_totali_sport) # seleziono il p99

nn_visite_totali_sport <- as.integer(novantanove_visite_totali_sport) # lo trasformo in integer

df_variabile_24$visite_totali_sport <- ifelse(df_variabile_24$visite_totali_sport > nn_visite_totali_sport , nn_visite_totali_sport, df_variabile_24$visite_totali_sport) # sostituisco ci?? che e' oltre il p99 col p99


##### 12) visite_totali_appwsc: ####


percentili_visite_totali_appwsc <- sql("select percentile_approx(visite_totali_appwsc, 0.99) as p99 
                                       from df_variabile_24 
                                       where visite_totali_appwsc is NOT NULL")

novantanove_visite_totali_appwsc <- first(percentili_visite_totali_appwsc) # seleziono il p99

nn_visite_totali_appwsc <- as.integer(novantanove_visite_totali_appwsc) # lo trasformo in integer

df_variabile_24$visite_totali_appwsc <- ifelse(df_variabile_24$visite_totali_appwsc > nn_visite_totali_appwsc , nn_visite_totali_appwsc, df_variabile_24$visite_totali_appwsc) # sostituisco ci?? che e' oltre il p99 col p99


##### 13) visite_totali_appgtv: ####


percentili_visite_totali_appgtv <- sql("select percentile_approx(visite_totali_appgtv, 0.99) as p99 
                                       from df_variabile_24 
                                       where visite_totali_appgtv is NOT NULL")

novantanove_visite_totali_appgtv <- first(percentili_visite_totali_appgtv) # seleziono il p99

nn_visite_totali_appgtv <- as.integer(novantanove_visite_totali_appgtv) # lo trasformo in integer

df_variabile_24$visite_totali_appgtv <- ifelse(df_variabile_24$visite_totali_appgtv > nn_visite_totali_appgtv , nn_visite_totali_appgtv, df_variabile_24$visite_totali_appgtv) # sostituisco ci?? che e' oltre il p99 col p99


##### 14) visite_totali_appsport: ####


percentili_visite_totali_appsport <- sql("select percentile_approx(visite_totali_appsport, 0.99) as p99 
                                         from df_variabile_24 
                                         where visite_totali_appsport is NOT NULL")

novantanove_visite_totali_appsport <- first(percentili_visite_totali_appsport) # seleziono il p99

nn_visite_totali_appsport <- as.integer(novantanove_visite_totali_appsport) # lo trasformo in integer

df_variabile_24$visite_totali_appsport <- ifelse(df_variabile_24$visite_totali_appsport > nn_visite_totali_appsport , nn_visite_totali_appsport, df_variabile_24$visite_totali_appsport) # sostituisco ci?? che e' oltre il p99 col p99



##### 15) visite_totali_appxfactor: ####


percentili_visite_totali_appxfactor <- sql("select percentile_approx(visite_totali_appxfactor, 0.99) as p99 
                                           from df_variabile_24 
                                           where visite_totali_appxfactor is NOT NULL")

novantanove_visite_totali_appxfactor <- first(percentili_visite_totali_appxfactor) # seleziono il p99

nn_visite_totali_appxfactor <- as.integer(novantanove_visite_totali_appxfactor) # lo trasformo in integer

df_variabile_24$visite_totali_appxfactor <- ifelse(df_variabile_24$visite_totali_appxfactor > nn_visite_totali_appxfactor , nn_visite_totali_appxfactor, df_variabile_24$visite_totali_appxfactor) # sostituisco ci?? che e' oltre il p99 col p99


##### 17) cookie_corporate: ####

percentili_cookie_corporate <- sql("select percentile_approx(cookie_corporate, 0.99) as p99 
                                   from df_variabile_24 
                                   where cookie_corporate is NOT NULL")

novantanove_cookie_corporate <- first(percentili_cookie_corporate) # seleziono il p99

nn_cookie_corporate <- as.integer(novantanove_cookie_corporate) # lo trasformo in integer

df_variabile_24$cookie_corporate <- ifelse(df_variabile_24$cookie_corporate > nn_cookie_corporate , nn_cookie_corporate, df_variabile_24$cookie_corporate) # sostituisco ci?? che e' oltre il p99 col p99


##### 18) cookie_tg24: ####


percentili_cookie_tg24 <- sql("select percentile_approx(cookie_tg24, 0.99) as p99 
                              from df_variabile_24 
                              where cookie_tg24 is NOT NULL")

novantanove_cookie_tg24 <- first(percentili_cookie_tg24) # seleziono il p99

nn_cookie_tg24 <- as.integer(novantanove_cookie_tg24) # lo trasformo in integer

df_variabile_24$cookie_tg24 <- ifelse(df_variabile_24$cookie_tg24 > nn_cookie_tg24 , nn_cookie_tg24, df_variabile_24$cookie_tg24) # sostituisco ci?? che e' oltre il p99 col p99


##### 19) cookie_sport: ####


percentili_cookie_sport <- sql("select percentile_approx(cookie_sport, 0.99) as p99 
                               from df_variabile_24 
                               where cookie_sport is NOT NULL")

novantanove_cookie_sport <- first(percentili_cookie_sport) # seleziono il p99

nn_cookie_sport <- as.integer(novantanove_cookie_sport) # lo trasformo in integer

df_variabile_24$cookie_sport <- ifelse(df_variabile_24$cookie_sport > nn_cookie_sport , nn_cookie_sport, df_variabile_24$cookie_sport) # sostituisco ci?? che e' oltre il p99 col p99


##### 20) cookie_skyitdev: ####


percentili_cookie_skyitdev <- sql("select percentile_approx(cookie_skyitdev, 0.99) as p99 
                                  from df_variabile_24 
                                  where cookie_skyitdev is NOT NULL")

novantanove_cookie_skyitdev <- first(percentili_cookie_skyitdev) # seleziono il p99

nn_cookie_skyitdev <- as.integer(novantanove_cookie_skyitdev) # lo trasformo in integer

df_variabile_24$cookie_skyitdev <- ifelse(df_variabile_24$cookie_skyitdev > nn_cookie_skyitdev , nn_cookie_skyitdev, df_variabile_24$cookie_skyitdev) # sostituisco ci?? che e' oltre il p99 col p99


##### 21) cookie_appwsc: ####


percentili_cookie_appwsc <- sql("select percentile_approx(cookie_appwsc, 0.99) as p99 
                                from df_variabile_24 
                                where cookie_appwsc is NOT NULL")

novantanove_cookie_appwsc <- first(percentili_cookie_appwsc) # seleziono il p99

nn_cookie_appwsc <- as.integer(novantanove_cookie_appwsc) # lo trasformo in integer

df_variabile_24$cookie_appwsc <- ifelse(df_variabile_24$cookie_appwsc > nn_cookie_appwsc , nn_cookie_appwsc, df_variabile_24$cookie_appwsc) # sostituisco ci?? che e' oltre il p99 col p99


##### 22) cookie_appgtv: ####


percentili_cookie_appgtv <- sql("select percentile_approx(cookie_appgtv, 0.99) as p99 
                                from df_variabile_24 
                                where cookie_appgtv is NOT NULL")

novantanove_cookie_appgtv <- first(percentili_cookie_appgtv) # seleziono il p99

nn_cookie_appgtv <- as.integer(novantanove_cookie_appgtv) # lo trasformo in integer

df_variabile_24$cookie_appgtv <- ifelse(df_variabile_24$cookie_appgtv > nn_cookie_appgtv , nn_cookie_appgtv, df_variabile_24$cookie_appgtv) # sostituisco ci?? che e' oltre il p99 col p99



##### 23) cookie_appsport: ####


percentili_cookie_appsport <- sql("select percentile_approx(cookie_appsport, 0.99) as p99 
                                  from df_variabile_24 
                                  where cookie_appsport is NOT NULL")

novantanove_cookie_appsport <- first(percentili_cookie_appsport) # seleziono il p99

nn_cookie_appsport <- as.integer(novantanove_cookie_appsport) # lo trasformo in integer

df_variabile_24$cookie_appsport <- ifelse(df_variabile_24$cookie_appsport > nn_cookie_appsport , nn_cookie_appsport, df_variabile_24$cookie_appsport) # sostituisco ci?? che e' oltre il p99 col p99


##### 24) cookie_appxfactor: ####


percentili_cookie_appxfactor <- sql("select percentile_approx(cookie_appxfactor, 0.99) as p99 
                                    from df_variabile_24 
                                    where cookie_appxfactor is NOT NULL")

novantanove_cookie_appxfactor <- first(percentili_cookie_appxfactor) # seleziono il p99

nn_cookie_appxfactor <- as.integer(novantanove_cookie_appxfactor) # lo trasformo in integer

df_variabile_24$cookie_appxfactor <- ifelse(df_variabile_24$cookie_appxfactor > nn_cookie_appxfactor , nn_cookie_appxfactor, df_variabile_24$cookie_appxfactor) # sostituisco ci?? che e' oltre il p99 col p99


##### 29) count_visits_from_social: ####


percentili_count_visits_from_social <- sql("select percentile_approx(count_visits_from_social, 0.99) as p99 
                                           from df_variabile_24 
                                           where count_visits_from_social is NOT NULL")

novantanove_count_visits_from_social <- first(percentili_count_visits_from_social) # seleziono il p99

nn_count_visits_from_social <- as.integer(novantanove_count_visits_from_social) # lo trasformo in integer

df_variabile_24$count_visits_from_social <- ifelse(df_variabile_24$count_visits_from_social > nn_count_visits_from_social , nn_count_visits_from_social, df_variabile_24$count_visits_from_social) # sostituisco ci?? che e' oltre il p99 col p99



##### 31) secondi_totali_corporate: ####


percentili_secondi_totali_corporate <- sql("select percentile_approx(secondi_totali_corporate, 0.99) as p99 
                                           from df_variabile_24 
                                           where secondi_totali_corporate is NOT NULL")

novantanove_secondi_totali_corporate <- first(percentili_secondi_totali_corporate) # seleziono il p99

nn_secondi_totali_corporate <- as.integer(novantanove_secondi_totali_corporate) # lo trasformo in integer

df_variabile_24$secondi_totali_corporate <- ifelse(df_variabile_24$secondi_totali_corporate > nn_secondi_totali_corporate , nn_secondi_totali_corporate, df_variabile_24$secondi_totali_corporate) # sostituisco ci?? che e' oltre il p99 col p99


##### 32) secondi_totali_tg24: ####


percentili_secondi_totali_tg24 <- sql("select percentile_approx(secondi_totali_tg24, 0.99) as p99 
                                      from df_variabile_24 
                                      where secondi_totali_tg24 is NOT NULL")

novantanove_secondi_totali_tg24 <- first(percentili_secondi_totali_tg24) # seleziono il p99

nn_secondi_totali_tg24 <- as.integer(novantanove_secondi_totali_tg24) # lo trasformo in integer

df_variabile_24$secondi_totali_tg24 <- ifelse(df_variabile_24$secondi_totali_tg24 > nn_secondi_totali_tg24 , nn_secondi_totali_tg24, df_variabile_24$secondi_totali_tg24) # sostituisco ci?? che e' oltre il p99 col p99


##### 33) secondi_totali_sport: ####


percentili_secondi_totali_sport <- sql("select percentile_approx(secondi_totali_sport, 0.99) as p99 
                                       from df_variabile_24 
                                       where secondi_totali_sport is NOT NULL")

novantanove_secondi_totali_sport <- first(percentili_secondi_totali_sport) # seleziono il p99

nn_secondi_totali_sport <- as.integer(novantanove_secondi_totali_sport) # lo trasformo in integer

df_variabile_24$secondi_totali_sport <- ifelse(df_variabile_24$secondi_totali_sport > nn_secondi_totali_sport , nn_secondi_totali_sport, df_variabile_24$secondi_totali_sport) # sostituisco ci?? che e' oltre il p99 col p99


##### 34) secondi_totali_appwsc: ####


percentili_secondi_totali_appwsc <- sql("select percentile_approx(secondi_totali_appwsc, 0.99) as p99 
                                        from df_variabile_24 
                                        where secondi_totali_appwsc is NOT NULL")

novantanove_secondi_totali_appwsc <- first(percentili_secondi_totali_appwsc) # seleziono il p99

nn_secondi_totali_appwsc <- as.integer(novantanove_secondi_totali_appwsc) # lo trasformo in integer

df_variabile_24$secondi_totali_appwsc <- ifelse(df_variabile_24$secondi_totali_appwsc > nn_secondi_totali_appwsc , nn_secondi_totali_appwsc, df_variabile_24$secondi_totali_appwsc) # sostituisco ci?? che e' oltre il p99 col p99


##### 35) secondi_totali_appgtv: ####


percentili_secondi_totali_appgtv <- sql("select percentile_approx(secondi_totali_appgtv, 0.99) as p99 
                                        from df_variabile_24 
                                        where secondi_totali_appgtv is NOT NULL")

novantanove_secondi_totali_appgtv <- first(percentili_secondi_totali_appgtv) # seleziono il p99

nn_secondi_totali_appgtv <- as.integer(novantanove_secondi_totali_appgtv) # lo trasformo in integer

df_variabile_24$secondi_totali_appgtv <- ifelse(df_variabile_24$secondi_totali_appgtv > nn_secondi_totali_appgtv , nn_secondi_totali_appgtv, df_variabile_24$secondi_totali_appgtv) # sostituisco ci?? che e' oltre il p99 col p99


##### 36) secondi_totali_appsport: ####


percentili_secondi_totali_appsport <- sql("select percentile_approx(secondi_totali_appsport, 0.99) as p99 
                                          from df_variabile_24 
                                          where secondi_totali_appsport is NOT NULL")

novantanove_secondi_totali_appsport <- first(percentili_secondi_totali_appsport) # seleziono il p99

nn_secondi_totali_appsport <- as.integer(novantanove_secondi_totali_appsport) # lo trasformo in integer

df_variabile_24$secondi_totali_appsport <- ifelse(df_variabile_24$secondi_totali_appsport > nn_secondi_totali_appsport , nn_secondi_totali_appsport, df_variabile_24$secondi_totali_appsport) # sostituisco ci?? che e' oltre il p99 col p99


##### 37) secondi_totali_appxfactor: ####


percentili_secondi_totali_appxfactor <- sql("select percentile_approx(secondi_totali_appxfactor, 0.99) as p99 
                                            from df_variabile_24 
                                            where secondi_totali_appxfactor is NOT NULL")

novantanove_secondi_totali_appxfactor <- first(percentili_secondi_totali_appxfactor) # seleziono il p99

nn_secondi_totali_appxfactor <- as.integer(novantanove_secondi_totali_appxfactor) # lo trasformo in integer

df_variabile_24$secondi_totali_appxfactor <- ifelse(df_variabile_24$secondi_totali_appxfactor > nn_secondi_totali_appxfactor , nn_secondi_totali_appxfactor, df_variabile_24$secondi_totali_appxfactor) # sostituisco ci?? che e' oltre il p99 col p99


##### 38) media_visite_corporate: ####


percentili_media_visite_corporate <- sql("select percentile_approx(media_visite_corporate, 0.99) as p99 
                                         from df_variabile_24 
                                         where media_visite_corporate is NOT NULL")

novantanove_media_visite_corporate <- first(percentili_media_visite_corporate) # seleziono il p99

nn_media_visite_corporate <- as.integer(novantanove_media_visite_corporate) # lo trasformo in integer

df_variabile_24$media_visite_corporate <- ifelse(df_variabile_24$media_visite_corporate > nn_media_visite_corporate , nn_media_visite_corporate, df_variabile_24$media_visite_corporate) # sostituisco ci?? che e' oltre il p99 col p99


##### 39) media_visite_tg24: ####


percentili_media_visite_tg24 <- sql("select percentile_approx(media_visite_tg24, 0.99) as p99 
                                    from df_variabile_24
                                    where media_visite_tg24 is NOT NULL")

novantanove_media_visite_tg24 <- first(percentili_media_visite_tg24) # seleziono il p99

nn_media_visite_tg24 <- as.integer(novantanove_media_visite_tg24) # lo trasformo in integer

df_variabile_24$media_visite_tg24 <- ifelse(df_variabile_24$media_visite_tg24 > nn_media_visite_tg24 , nn_media_visite_tg24, df_variabile_24$media_visite_tg24) # sostituisco ci?? che e' oltre il p99 col p99


##### 40) media_visite_sport: ####


percentili_media_visite_sport <- sql("select percentile_approx(media_visite_sport, 0.99) as p99 
                                     from df_variabile_24 
                                     where media_visite_sport is NOT NULL")

novantanove_media_visite_sport <- first(percentili_media_visite_sport) # seleziono il p99

nn_media_visite_sport <- as.integer(novantanove_media_visite_sport) # lo trasformo in integer

df_variabile_24$media_visite_sport <- ifelse(df_variabile_24$media_visite_sport > nn_media_visite_sport , nn_media_visite_sport, df_variabile_24$media_visite_sport) # sostituisco ci?? che e' oltre il p99 col p99


##### 41) media_visite_appwsc: ####


percentili_media_visite_appwsc <- sql("select percentile_approx(media_visite_appwsc, 0.99) as p99 
                                      from df_variabile_24 
                                      where media_visite_appwsc is NOT NULL")

novantanove_media_visite_appwsc <- first(percentili_media_visite_appwsc) # seleziono il p99

nn_media_visite_appwsc <- as.integer(novantanove_media_visite_appwsc) # lo trasformo in integer

df_variabile_24$media_visite_appwsc <- ifelse(df_variabile_24$media_visite_appwsc > nn_media_visite_appwsc , nn_media_visite_appwsc, df_variabile_24$media_visite_appwsc) # sostituisco ci?? che e' oltre il p99 col p99


##### 42) media_visite_appgtv: ####


percentili_media_visite_appgtv <- sql("select percentile_approx(media_visite_appgtv, 0.99) as p99 
                                      from df_variabile_24 
                                      where media_visite_appgtv is NOT NULL")

novantanove_media_visite_appgtv <- first(percentili_media_visite_appgtv) # seleziono il p99

nn_media_visite_appgtv <- as.integer(novantanove_media_visite_appgtv) # lo trasformo in integer

df_variabile_24$media_visite_appgtv <- ifelse(df_variabile_24$media_visite_appgtv > nn_media_visite_appgtv , nn_media_visite_appgtv, df_variabile_24$media_visite_appgtv) # sostituisco ci?? che e' oltre il p99 col p99


##### 43) media_visite_appsport: ####


percentili_media_visite_appsport <- sql("select percentile_approx(media_visite_appsport, 0.99) as p99 
                                        from df_variabile_24 
                                        where media_visite_appsport is NOT NULL")

novantanove_media_visite_appsport <- first(percentili_media_visite_appsport) # seleziono il p99

nn_media_visite_appsport <- as.integer(novantanove_media_visite_appsport) # lo trasformo in integer

df_variabile_24$media_visite_appsport <- ifelse(df_variabile_24$media_visite_appsport > nn_media_visite_appsport , nn_media_visite_appsport, df_variabile_24$media_visite_appsport) # sostituisco ci?? che e' oltre il p99 col p99


##### 44) media_visite_appxfactor: ####


percentili_media_visite_appxfactor <- sql("select percentile_approx(media_visite_appxfactor, 0.99) as p99 
                                          from df_variabile_24 
                                          where media_visite_appxfactor is NOT NULL")

novantanove_media_visite_appxfactor <- first(percentili_media_visite_appxfactor) # seleziono il p99

nn_media_visite_appxfactor <- as.integer(novantanove_media_visite_appxfactor) # lo trasformo in integer

df_variabile_24$media_visite_appxfactor <- ifelse(df_variabile_24$media_visite_appxfactor > nn_media_visite_appxfactor , nn_media_visite_appsport, df_variabile_24$media_visite_appxfactor) # sostituisco ci?? che e' oltre il p99 col p99


##### 48) durata_media_visite_corporate: ####


percentili_durata_media_visite_corporate <- sql("select percentile_approx(durata_media_visite_corporate, 0.99) as p99 
                                                from df_variabile_24 
                                                where durata_media_visite_corporate is NOT NULL")

novantanove_durata_media_visite_corporate <- first(percentili_durata_media_visite_corporate) # seleziono il p99

nn_durata_media_visite_corporate <- as.integer(novantanove_durata_media_visite_corporate) # lo trasformo in integer

df_variabile_24$durata_media_visite_corporate <- ifelse(df_variabile_24$durata_media_visite_corporate > nn_durata_media_visite_corporate, 
                                                        nn_durata_media_visite_corporate, df_variabile_24$durata_media_visite_corporate) # sostituisco ci?? che e' oltre il p99 col p99


##### 49) durata_media_visite_tg24: ####


percentili_durata_media_visite_tg24 <- sql("select percentile_approx(durata_media_visite_tg24, 0.99) as p99 
                                           from df_variabile_24 
                                           where durata_media_visite_tg24 is NOT NULL")

novantanove_durata_media_visite_tg24 <- first(percentili_durata_media_visite_tg24) # seleziono il p99

nn_durata_media_visite_tg24 <- as.integer(novantanove_durata_media_visite_tg24) # lo trasformo in integer

df_variabile_24$durata_media_visite_tg24 <- ifelse(df_variabile_24$durata_media_visite_tg24 > nn_durata_media_visite_tg24 , nn_media_visite_appsport, df_variabile_24$durata_media_visite_tg24) # sostituisco ci?? che e' oltre il p99 col p99


##### 50) durata_media_visite_sport: ####


percentili_durata_media_visite_sport <- sql("select percentile_approx(durata_media_visite_sport, 0.99) as p99 
                                            from df_variabile_24 
                                            where durata_media_visite_sport is NOT NULL")

novantanove_durata_media_visite_sport <- first(percentili_durata_media_visite_sport) # seleziono il p99

nn_durata_media_visite_sport <- as.integer(novantanove_durata_media_visite_sport) # lo trasformo in integer

df_variabile_24$durata_media_visite_sport <- ifelse(df_variabile_24$durata_media_visite_sport > nn_durata_media_visite_sport , nn_media_visite_appsport, df_variabile_24$durata_media_visite_sport) # sostituisco ci?? che e' oltre il p99 col p99


##### 51) durata_media_visite_appwsc: ####


percentili_durata_media_visite_appwsc <- sql("select percentile_approx(durata_media_visite_appwsc, 0.99) as p99 
                                             from df_variabile_24 
                                             where durata_media_visite_appwsc is NOT NULL")

novantanove_durata_media_visite_appwsc <- first(percentili_durata_media_visite_appwsc) # seleziono il p99

nn_durata_media_visite_appwsc <- as.integer(novantanove_durata_media_visite_appwsc) # lo trasformo in integer

df_variabile_24$durata_media_visite_appwsc <- ifelse(df_variabile_24$durata_media_visite_appwsc > nn_durata_media_visite_appwsc , nn_media_visite_appsport, df_variabile_24$durata_media_visite_appwsc) # sostituisco ci?? che e' oltre il p99 col p99


##### 52) durata_media_visite_appgtv: ####


percentili_durata_media_visite_appgtv <- sql("select percentile_approx(durata_media_visite_appgtv, 0.99) as p99 
                                             from df_variabile_24 
                                             where durata_media_visite_appgtv is NOT NULL")

novantanove_durata_media_visite_appgtv <- first(percentili_durata_media_visite_appgtv) # seleziono il p99

nn_durata_media_visite_appgtv <- as.integer(novantanove_durata_media_visite_appgtv) # lo trasformo in integer

df_variabile_24$durata_media_visite_appgtv <- ifelse(df_variabile_24$durata_media_visite_appgtv > nn_durata_media_visite_appgtv , nn_media_visite_appsport, df_variabile_24$durata_media_visite_appgtv) # sostituisco ci?? che e' oltre il p99 col p99


##### 53) durata_media_visite_appsport: ####


percentili_durata_media_visite_appsport <- sql("select percentile_approx(durata_media_visite_appsport, 0.99) as p99 
                                               from df_variabile_24 
                                               where durata_media_visite_appsport is NOT NULL")

novantanove_durata_media_visite_appsport <- first(percentili_durata_media_visite_appsport) # seleziono il p99

nn_durata_media_visite_appsport <- as.integer(novantanove_durata_media_visite_appsport) # lo trasformo in integer

df_variabile_24$durata_media_visite_appsport <- ifelse(df_variabile_24$durata_media_visite_appsport > nn_durata_media_visite_appsport , nn_media_visite_appsport, df_variabile_24$durata_media_visite_appsport) # sostituisco ci?? che e' oltre il p99 col p99


##### 54) durata_media_visite_appxfactor: ####


percentili_durata_media_visite_appxfactor <- sql("select percentile_approx(durata_media_visite_appxfactor, 0.99) as p99 
                                                 from df_variabile_24 
                                                 where durata_media_visite_appxfactor is NOT NULL")

novantanove_durata_media_visite_appxfactor <- first(percentili_durata_media_visite_appxfactor) # seleziono il p99

nn_durata_media_visite_appxfactor <- as.integer(novantanove_durata_media_visite_appxfactor) # lo trasformo in integer

df_variabile_24$durata_media_visite_appxfactor <- ifelse(df_variabile_24$durata_media_visite_appxfactor > nn_durata_media_visite_appxfactor, 
                                                         nn_media_visite_appxfactor, df_variabile_24$durata_media_visite_appxfactor) # sostituisco ci?? che e' oltre il p99 col p99


##### 63) visite_corporate_appwsc_appgtv: ####


percentili_visite_corporate_appwsc_appgtv <- sql("select percentile_approx(visite_corporate_appwsc_appgtv, 0.99) as p99 
                                                 from df_variabile_24 
                                                 where visite_corporate_appwsc_appgtv is NOT NULL")

novantanove_visite_corporate_appwsc_appgtv <- first(percentili_visite_corporate_appwsc_appgtv) # seleziono il p99

nn_visite_corporate_appwsc_appgtv <- as.integer(novantanove_visite_corporate_appwsc_appgtv) # lo trasformo in integer

df_variabile_24$visite_corporate_appwsc_appgtv <- ifelse(df_variabile_24$visite_corporate_appwsc_appgtv > nn_visite_corporate_appwsc_appgtv , 
                                                         nn_visite_corporate_appwsc_appgtv, df_variabile_24$visite_corporate_appwsc_appgtv) # sostituisco ci?? che e' oltre il p99 col p99


##### 64) visite_editoriale: ####


percentili_visite_editoriale <- sql("select percentile_approx(visite_editoriale, 0.99) as p99 
                                    from df_variabile_24 
                                    where visite_editoriale is NOT NULL")

novantanove_visite_editoriale <- first(percentili_visite_editoriale) # seleziono il p99

nn_visite_editoriale <- as.integer(novantanove_visite_editoriale) # lo trasformo in integer

df_variabile_24$visite_editoriale <- ifelse(df_variabile_24$visite_editoriale > nn_visite_editoriale , nn_visite_editoriale, df_variabile_24$visite_editoriale) # sostituisco ci?? che e' oltre il p99 col p99


##### 65) visite_web: ####


percentili_visite_web <- sql("select percentile_approx(visite_web, 0.99) as p99 
                             from df_variabile_24 
                             where visite_web is NOT NULL")

novantanove_visite_web <- first(percentili_visite_web) # seleziono il p99

nn_visite_web <- as.integer(novantanove_visite_web) # lo trasformo in integer

df_variabile_24$visite_web <- ifelse(df_variabile_24$visite_web > nn_visite_web , nn_visite_web, df_variabile_24$visite_web) # sostituisco ci?? che e' oltre il p99 col p99


##### 66) visite_app: ####


percentili_visite_app <- sql("select percentile_approx(visite_app, 0.99) as p99 
                             from df_variabile_24 
                             where visite_app is NOT NULL")

novantanove_visite_app <- first(percentili_visite_app) # seleziono il p99

nn_visite_app <- as.integer(novantanove_visite_app) # lo trasformo in integer

df_variabile_24$visite_app <- ifelse(df_variabile_24$visite_app > nn_visite_app , nn_visite_app, df_variabile_24$visite_app) # sostituisco ci?? che e' oltre il p99 col p99


##### 71) avg_visite_mese_corporate_appwsc_appgtv: ####


percentili_avg_visite_mese_corporate_appwsc_appgtv <- sql("select percentile_approx(avg_visite_mese_corporate_appwsc_appgtv, 0.99) as p99 
                                                          from df_variabile_24 
                                                          where avg_visite_mese_corporate_appwsc_appgtv is NOT NULL")

novantanove_avg_visite_mese_corporate_appwsc_appgtv <- first(percentili_avg_visite_mese_corporate_appwsc_appgtv) # seleziono il p99

nn_avg_visite_mese_corporate_appwsc_appgtv <- as.integer(novantanove_avg_visite_mese_corporate_appwsc_appgtv) # lo trasformo in integer

df_variabile_24$avg_visite_mese_corporate_appwsc_appgtv <- ifelse(df_variabile_24$avg_visite_mese_corporate_appwsc_appgtv > nn_avg_visite_mese_corporate_appwsc_appgtv, 
                                                                  nn_avg_visite_mese_corporate_appwsc_appgtv, df_variabile_24$avg_visite_mese_corporate_appwsc_appgtv) # sostituisco ci?? che e' oltre il p99 col p99


##### 72) avg_visite_mese_editoriale: ####


percentili_avg_visite_mese_editoriale <- sql("select percentile_approx(avg_visite_mese_editoriale, 0.99) as p99 
                                             from df_variabile_24 
                                             where avg_visite_mese_editoriale is NOT NULL")

novantanove_avg_visite_mese_editoriale <- first(percentili_avg_visite_mese_editoriale) # seleziono il p99

nn_avg_visite_mese_editoriale <- as.integer(novantanove_avg_visite_mese_editoriale) # lo trasformo in integer

df_variabile_24$avg_visite_mese_editoriale <- ifelse(df_variabile_24$avg_visite_mese_editoriale > nn_avg_visite_mese_editoriale , nn_avg_visite_mese_editoriale, df_variabile_24$avg_visite_mese_editoriale) # sostituisco ci?? che e' oltre il p99 col p99


##### 73) avg_visite_mese_web: ####


percentili_avg_visite_mese_web <- sql("select percentile_approx(avg_visite_mese_web, 0.99) as p99 
                                      from df_variabile_24 
                                      where avg_visite_mese_web is NOT NULL")

novantanove_avg_visite_mese_web <- first(percentili_avg_visite_mese_web) # seleziono il p99

nn_avg_visite_mese_web <- as.integer(novantanove_avg_visite_mese_web) # lo trasformo in integer

df_variabile_24$avg_visite_mese_web <- ifelse(df_variabile_24$avg_visite_mese_web > nn_avg_visite_mese_web , nn_avg_visite_mese_web, df_variabile_24$avg_visite_mese_web) # sostituisco ci?? che e' oltre il p99 col p99


##### 74) avg_visite_mese_app: ####


percentili_avg_visite_mese_app <- sql("select percentile_approx(avg_visite_mese_app, 0.99) as p99 
                                      from df_variabile_24 
                                      where avg_visite_mese_app is NOT NULL")

novantanove_avg_visite_mese_app <- first(percentili_avg_visite_mese_app) # seleziono il p99

nn_avg_visite_mese_app <- as.integer(novantanove_avg_visite_mese_app) # lo trasformo in integer

df_variabile_24$avg_visite_mese_app <- ifelse(df_variabile_24$avg_visite_mese_app > nn_avg_visite_mese_app , nn_avg_visite_mese_app, df_variabile_24$avg_visite_mese_app) # sostituisco ci?? che e' oltre il p99 col p99


##### 75) secondi_corporate_appwsc_appgtv: ####


percentili_secondi_corporate_appwsc_appgtv <- sql("select percentile_approx(secondi_corporate_appwsc_appgtv, 0.99) as p99 
                                                  from df_variabile_24 
                                                  where secondi_corporate_appwsc_appgtv is NOT NULL")

novantanove_secondi_corporate_appwsc_appgtv <- first(percentili_secondi_corporate_appwsc_appgtv) # seleziono il p99

nn_secondi_corporate_appwsc_appgtv <- as.integer(novantanove_secondi_corporate_appwsc_appgtv) # lo trasformo in integer

df_variabile_24$secondi_corporate_appwsc_appgtv <- ifelse(df_variabile_24$secondi_corporate_appwsc_appgtv > nn_secondi_corporate_appwsc_appgtv , 
                                                          nn_secondi_corporate_appwsc_appgtv, df_variabile_24$secondi_corporate_appwsc_appgtv) # sostituisco ci?? che e' oltre il p99 col p99


##### 76) secondi_editoriale: ####


percentili_secondi_editoriale <- sql("select percentile_approx(secondi_editoriale, 0.99) as p99 
                                     from df_variabile_24 
                                     where secondi_editoriale is NOT NULL")

novantanove_secondi_editoriale <- first(percentili_secondi_editoriale) # seleziono il p99

nn_secondi_editoriale <- as.integer(novantanove_secondi_editoriale) # lo trasformo in integer

df_variabile_24$secondi_editoriale <- ifelse(df_variabile_24$secondi_editoriale > nn_secondi_editoriale , nn_secondi_editoriale, df_variabile_24$secondi_editoriale) # sostituisco ci?? che e' oltre il p99 col p99


##### 77) secondi_web: ####


percentili_secondi_web <- sql("select percentile_approx(secondi_web, 0.99) as p99 
                              from df_variabile_24 
                              where secondi_web is NOT NULL")

novantanove_secondi_web <- first(percentili_secondi_web) # seleziono il p99

nn_secondi_web <- as.integer(novantanove_secondi_web) # lo trasformo in integer

df_variabile_24$secondi_web <- ifelse(df_variabile_24$secondi_web > nn_secondi_web , nn_secondi_web, df_variabile_24$secondi_web) # sostituisco ci?? che e' oltre il p99 col p99


##### 78) secondi_app: ####


percentili_secondi_app <- sql("select percentile_approx(secondi_app, 0.99) as p99 
                              from df_variabile_24 
                              where secondi_app is NOT NULL")

novantanove_secondi_app <- first(percentili_secondi_app) # seleziono il p99

nn_secondi_app <- as.integer(novantanove_secondi_app) # lo trasformo in integer

df_variabile_24$secondi_app <- ifelse(df_variabile_24$secondi_app > nn_secondi_app , nn_secondi_app, df_variabile_24$secondi_app) # sostituisco ci?? che e' oltre il p99 col p99


##### 79) avg_sec_corporate_appwsc_appgtv: ####


percentili_avg_sec_corporate_appwsc_appgtv <- sql("select percentile_approx(avg_sec_corporate_appwsc_appgtv, 0.99) as p99 
                                                  from df_variabile_24 
                                                  where avg_sec_corporate_appwsc_appgtv is NOT NULL")

novantanove_avg_sec_corporate_appwsc_appgtv <- first(percentili_avg_sec_corporate_appwsc_appgtv) # seleziono il p99

nn_avg_sec_corporate_appwsc_appgtv <- as.integer(novantanove_avg_sec_corporate_appwsc_appgtv) # lo trasformo in integer

df_variabile_24$avg_sec_corporate_appwsc_appgtv <- ifelse(df_variabile_24$avg_sec_corporate_appwsc_appgtv > nn_avg_sec_corporate_appwsc_appgtv , 
                                                          nn_avg_sec_corporate_appwsc_appgtv, df_variabile_24$avg_sec_corporate_appwsc_appgtv) # sostituisco ci?? che e' oltre il p99 col p99


##### 80) avg_sec_editoriale: ####


percentili_avg_sec_editoriale <- sql("select percentile_approx(avg_sec_editoriale, 0.99) as p99 
                                     from df_variabile_24 
                                     where avg_sec_editoriale is NOT NULL")

novantanove_avg_sec_editoriale <- first(percentili_avg_sec_editoriale) # seleziono il p99

nn_avg_sec_editoriale <- as.integer(novantanove_avg_sec_editoriale) # lo trasformo in integer

df_variabile_24$avg_sec_editoriale <- ifelse(df_variabile_24$avg_sec_editoriale > nn_avg_sec_editoriale , nn_avg_sec_editoriale, df_variabile_24$avg_sec_editoriale) # sostituisco ci?? che e' oltre il p99 col p99


##### 81) avg_sec_web: ####


percentili_avg_sec_web <- sql("select percentile_approx(avg_sec_web, 0.99) as p99 
                              from df_variabile_24 
                              where avg_sec_web is NOT NULL")

novantanove_avg_sec_web <- first(percentili_avg_sec_web) # seleziono il p99

nn_avg_sec_web <- as.integer(novantanove_avg_sec_web) # lo trasformo in integer

df_variabile_24$avg_sec_web <- ifelse(df_variabile_24$avg_sec_web > nn_avg_sec_web , nn_avg_sec_web, df_variabile_24$avg_sec_web) # sostituisco ci?? che e' oltre il p99 col p99


##### 82) avg_sec_app: ####


percentili_avg_sec_app <- sql("select percentile_approx(avg_sec_app, 0.99) as p99 
                              from df_variabile_24 
                              where avg_sec_app is NOT NULL")

novantanove_avg_sec_app <- first(percentili_avg_sec_app) # seleziono il p99

nn_avg_sec_app <- as.integer(novantanove_avg_sec_app) # lo trasformo in integer

df_variabile_24$avg_sec_app <- ifelse(df_variabile_24$avg_sec_app > nn_avg_sec_app , nn_avg_sec_app, df_variabile_24$avg_sec_app) # sostituisco ci?? che e' oltre il p99 col p99


##### 83) score_utilizzo: ####


percentili_score_utilizzo <- sql("select percentile_approx(score_utilizzo, 0.99) as p99 
                                 from df_variabile_24 
                                 where score_utilizzo is NOT NULL")

novantanove_score_utilizzo <- first(percentili_score_utilizzo) # seleziono il p99

nn_score_utilizzo <- as.integer(novantanove_score_utilizzo) # lo trasformo in integer

df_variabile_24$score_utilizzo <- ifelse(df_variabile_24$score_utilizzo > nn_score_utilizzo , nn_score_utilizzo, df_variabile_24$score_utilizzo) # sostituisco ci?? che e' oltre il p99 col p99


##### 89) visite_settimana_corporate: ####


percentili_visite_settimana_corporate <- sql("select percentile_approx(visite_settimana_corporate, 0.99) as p99 
                                             from df_variabile_24 
                                             where visite_settimana_corporate is NOT NULL")

novantanove_visite_settimana_corporate <- first(percentili_visite_settimana_corporate) # seleziono il p99

nn_visite_settimana_corporate <- as.integer(novantanove_visite_settimana_corporate) # lo trasformo in integer

df_variabile_24$visite_settimana_corporate <- ifelse(df_variabile_24$visite_settimana_corporate > nn_visite_settimana_corporate , nn_visite_settimana_corporate, df_variabile_24$visite_settimana_corporate) # sostituisco ci?? che e' oltre il p99 col p99


##### 90) visite_weekend_corporate: ####


percentili_visite_weekend_corporate <- sql("select percentile_approx(visite_weekend_corporate, 0.99) as p99 
                                           from df_variabile_24 
                                           where visite_weekend_corporate is NOT NULL")

novantanove_visite_weekend_corporate <- first(percentili_visite_weekend_corporate) # seleziono il p99

nn_visite_weekend_corporate <- as.integer(novantanove_visite_weekend_corporate) # lo trasformo in integer

df_variabile_24$visite_weekend_corporate <- ifelse(df_variabile_24$visite_weekend_corporate > nn_visite_weekend_corporate , nn_visite_weekend_corporate, df_variabile_24$visite_weekend_corporate) # sostituisco ci?? che e' oltre il p99 col p99


##### 91) visite_settimana_tg24: ####


percentili_visite_settimana_tg24 <- sql("select percentile_approx(visite_settimana_tg24, 0.99) as p99 
                                        from df_variabile_24 
                                        where visite_settimana_tg24 is NOT NULL")

novantanove_visite_settimana_tg24 <- first(percentili_visite_settimana_tg24) # seleziono il p99

nn_visite_settimana_tg24 <- as.integer(novantanove_visite_settimana_tg24) # lo trasformo in integer

df_variabile_24$visite_settimana_tg24 <- ifelse(df_variabile_24$visite_settimana_tg24 > nn_visite_settimana_tg24 , nn_visite_settimana_tg24, df_variabile_24$visite_settimana_tg24) # sostituisco ci?? che e' oltre il p99 col p99


##### 92) visite_weekend_tg24: ####


percentili_visite_weekend_tg24 <- sql("select percentile_approx(visite_weekend_tg24, 0.99) as p99 
                                      from df_variabile_24 
                                      where visite_weekend_tg24 is NOT NULL")

novantanove_visite_weekend_tg24 <- first(percentili_visite_weekend_tg24) # seleziono il p99

nn_visite_weekend_tg24 <- as.integer(novantanove_visite_weekend_tg24) # lo trasformo in integer

df_variabile_24$visite_weekend_tg24 <- ifelse(df_variabile_24$visite_weekend_tg24 > nn_visite_weekend_tg24 , nn_visite_weekend_tg24, df_variabile_24$visite_weekend_tg24) # sostituisco ci?? che e' oltre il p99 col p99


##### 93) visite_settimana_sport: ####


percentili_visite_settimana_sport <- sql("select percentile_approx(visite_settimana_sport, 0.99) as p99 
                                         from df_variabile_24 
                                         where visite_settimana_sport is NOT NULL")

novantanove_visite_settimana_sport <- first(percentili_visite_settimana_sport) # seleziono il p99

nn_visite_settimana_sport <- as.integer(novantanove_visite_settimana_sport) # lo trasformo in integer

df_variabile_24$visite_settimana_sport <- ifelse(df_variabile_24$visite_settimana_sport > nn_visite_settimana_sport , nn_visite_settimana_sport, df_variabile_24$visite_settimana_sport) # sostituisco ci?? che e' oltre il p99 col p99


##### 94) visite_weekend_sport: ####


percentili_visite_weekend_sport <- sql("select percentile_approx(visite_weekend_sport, 0.99) as p99 
                                       from df_variabile_24 
                                       where visite_weekend_sport is NOT NULL")

novantanove_visite_weekend_sport <- first(percentili_visite_weekend_sport) # seleziono il p99

nn_visite_weekend_sport <- as.integer(novantanove_visite_weekend_sport) # lo trasformo in integer

df_variabile_24$visite_weekend_sport <- ifelse(df_variabile_24$visite_weekend_sport > nn_visite_weekend_sport , nn_visite_weekend_sport, df_variabile_24$visite_weekend_sport) # sostituisco ci?? che e' oltre il p99 col p99


##### 95) visite_settimana_appwsc: ####


percentili_visite_settimana_appwsc <- sql("select percentile_approx(visite_settimana_appwsc, 0.99) as p99 
                                          from df_variabile_24 
                                          where visite_settimana_appwsc is NOT NULL")

novantanove_visite_settimana_appwsc <- first(percentili_visite_settimana_appwsc) # seleziono il p99

nn_visite_settimana_appwsc <- as.integer(novantanove_visite_settimana_appwsc) # lo trasformo in integer

df_variabile_24$visite_settimana_appwsc <- ifelse(df_variabile_24$visite_settimana_appwsc > nn_visite_settimana_appwsc , nn_visite_settimana_appwsc, df_variabile_24$visite_settimana_appwsc) # sostituisco ci?? che e' oltre il p99 col p99


##### 96) visite_weekend_appwsc: ####


percentili_visite_weekend_appwsc <- sql("select percentile_approx(visite_weekend_appwsc, 0.99) as p99 
                                        from df_variabile_24 
                                        where visite_weekend_appwsc is NOT NULL")

novantanove_visite_weekend_appwsc <- first(percentili_visite_weekend_appwsc) # seleziono il p99

nn_visite_weekend_appwsc <- as.integer(novantanove_visite_weekend_appwsc) # lo trasformo in integer

df_variabile_24$visite_weekend_appwsc <- ifelse(df_variabile_24$visite_weekend_appwsc > nn_visite_weekend_appwsc , nn_visite_weekend_appwsc, df_variabile_24$visite_weekend_appwsc) # sostituisco ci?? che e' oltre il p99 col p99


##### 97) visite_settimana_appgtv: ####


percentili_visite_settimana_appgtv <- sql("select percentile_approx(visite_settimana_appgtv, 0.99) as p99 
                                          from df_variabile_24 
                                          where visite_settimana_appgtv is NOT NULL")

novantanove_visite_settimana_appgtv <- first(percentili_visite_settimana_appgtv) # seleziono il p99

nn_visite_settimana_appgtv <- as.integer(novantanove_visite_settimana_appgtv) # lo trasformo in integer

df_variabile_24$visite_settimana_appgtv <- ifelse(df_variabile_24$visite_settimana_appgtv > nn_visite_settimana_appgtv , nn_visite_settimana_appgtv, df_variabile_24$visite_settimana_appgtv) # sostituisco ci?? che e' oltre il p99 col p99


##### 98) visite_weekend_appgtv: ####


percentili_visite_weekend_appgtv <- sql("select percentile_approx(visite_weekend_appgtv, 0.99) as p99 
                                        from df_variabile_24 
                                        where visite_weekend_appgtv is NOT NULL")

novantanove_visite_weekend_appgtv <- first(percentili_visite_weekend_appgtv) # seleziono il p99

nn_visite_weekend_appgtv <- as.integer(novantanove_visite_weekend_appgtv) # lo trasformo in integer

df_variabile_24$visite_weekend_appgtv <- ifelse(df_variabile_24$visite_weekend_appgtv > nn_visite_weekend_appgtv , nn_visite_weekend_appgtv, df_variabile_24$visite_weekend_appgtv) # sostituisco ci?? che e' oltre il p99 col p99


##### 99) visite_settimana_appsport: ####


percentili_visite_settimana_appsport <- sql("select percentile_approx(visite_settimana_appsport, 0.99) as p99 
                                            from df_variabile_24 
                                            where visite_settimana_appsport is NOT NULL")

novantanove_visite_settimana_appsport <- first(percentili_visite_settimana_appsport) # seleziono il p99

nn_visite_settimana_appsport <- as.integer(novantanove_visite_settimana_appsport) # lo trasformo in integer

df_variabile_24$visite_settimana_appsport <- ifelse(df_variabile_24$visite_settimana_appsport > nn_visite_settimana_appsport , nn_visite_settimana_appsport, df_variabile_24$visite_settimana_appsport) # sostituisco ci?? che e' oltre il p99 col p99


##### 100) visite_weekend_appsport: ####


percentili_visite_weekend_appsport <- sql("select percentile_approx(visite_weekend_appsport, 0.99) as p99 
                                          from df_variabile_24 
                                          where visite_weekend_appsport is NOT NULL")

novantanove_visite_weekend_appsport <- first(percentili_visite_weekend_appsport) # seleziono il p99

nn_visite_weekend_appsport <- as.integer(novantanove_visite_weekend_appsport) # lo trasformo in integer

df_variabile_24$visite_weekend_appsport <- ifelse(df_variabile_24$visite_weekend_appsport > nn_visite_weekend_appsport , nn_visite_weekend_appsport, df_variabile_24$visite_weekend_appsport) # sostituisco ci?? che e' oltre il p99 col p99


##### 101) visite_settimana_appxfactor: ####


percentili_visite_settimana_appxfactor <- sql("select percentile_approx(visite_settimana_appxfactor, 0.99) as p99 
                                              from df_variabile_24 
                                              where visite_settimana_appxfactor is NOT NULL")

novantanove_visite_settimana_appxfactor <- first(percentili_visite_settimana_appxfactor) # seleziono il p99

nn_visite_settimana_appxfactor <- as.integer(novantanove_visite_settimana_appxfactor) # lo trasformo in integer

df_variabile_24$visite_settimana_appxfactor <- ifelse(df_variabile_24$visite_settimana_appxfactor > nn_visite_settimana_appxfactor , nn_visite_settimana_appxfactor, df_variabile_24$visite_settimana_appxfactor) # sostituisco ci?? che e' oltre il p99 col p99


##### 102) visite_weekend_appxfactor: ####


percentili_visite_weekend_appxfactor <- sql("select percentile_approx(visite_weekend_appxfactor, 0.99) as p99 
                                            from df_variabile_24 
                                            where visite_weekend_appxfactor is NOT NULL")

novantanove_visite_weekend_appxfactor <- first(percentili_visite_weekend_appxfactor) # seleziono il p99

nn_visite_weekend_appxfactor <- as.integer(novantanove_visite_weekend_appxfactor) # lo trasformo in integer

df_variabile_24$visite_weekend_appxfactor <- ifelse(df_variabile_24$visite_weekend_appxfactor > nn_visite_weekend_appxfactor , nn_visite_weekend_appxfactor, df_variabile_24$visite_weekend_appxfactor) # sostituisco ci?? che e' oltre il p99 col p99


##### 103) visite_corporate_notte: ####


percentili_visite_corporate_notte <- sql("select percentile_approx(visite_corporate_notte, 0.99) as p99 
                                         from df_variabile_24 
                                         where visite_corporate_notte is NOT NULL")

novantanove_visite_corporate_notte <- first(percentili_visite_corporate_notte) # seleziono il p99

nn_visite_corporate_notte <- as.integer(novantanove_visite_corporate_notte) # lo trasformo in integer

df_variabile_24$visite_corporate_notte <- ifelse(df_variabile_24$visite_corporate_notte > nn_visite_corporate_notte , nn_visite_corporate_notte, df_variabile_24$visite_corporate_notte) # sostituisco ci?? che e' oltre il p99 col p99


##### 104) visite_corporate_mattina: ####


percentili_visite_corporate_mattina <- sql("select percentile_approx(visite_corporate_mattina, 0.99) as p99 
                                           from df_variabile_24 
                                           where visite_corporate_mattina is NOT NULL")

novantanove_visite_corporate_mattina <- first(percentili_visite_corporate_mattina) # seleziono il p99

nn_visite_corporate_mattina <- as.integer(novantanove_visite_corporate_mattina) # lo trasformo in integer

df_variabile_24$visite_corporate_mattina <- ifelse(df_variabile_24$visite_corporate_mattina > nn_visite_corporate_mattina , nn_visite_corporate_mattina, df_variabile_24$visite_corporate_mattina) # sostituisco ci?? che e' oltre il p99 col p99


##### 105) visite_corporate_pomeriggio: ####


percentili_visite_corporate_pomeriggio <- sql("select percentile_approx(visite_corporate_pomeriggio, 0.99) as p99 
                                              from df_variabile_24 
                                              where visite_corporate_pomeriggio is NOT NULL")

novantanove_visite_corporate_pomeriggio <- first(percentili_visite_corporate_pomeriggio) # seleziono il p99

nn_visite_corporate_pomeriggio <- as.integer(novantanove_visite_corporate_pomeriggio) # lo trasformo in integer

df_variabile_24$visite_corporate_pomeriggio <- ifelse(df_variabile_24$visite_corporate_pomeriggio > nn_visite_corporate_pomeriggio , nn_visite_corporate_pomeriggio, df_variabile_24$visite_corporate_pomeriggio) # sostituisco ci?? che e' oltre il p99 col p99


##### 106) visite_corporate_sera: ####


percentili_visite_corporate_sera <- sql("select percentile_approx(visite_corporate_sera, 0.99) as p99 
                                        from df_variabile_24 
                                        where visite_corporate_sera is NOT NULL")

novantanove_visite_corporate_sera <- first(percentili_visite_corporate_sera) # seleziono il p99

nn_visite_corporate_sera <- as.integer(novantanove_visite_corporate_sera) # lo trasformo in integer

df_variabile_24$visite_corporate_sera <- ifelse(df_variabile_24$visite_corporate_sera > nn_visite_corporate_sera , nn_visite_corporate_sera, df_variabile_24$visite_corporate_sera) # sostituisco ci?? che e' oltre il p99 col p99



##### 107) visite_tg24_notte: ####


percentili_visite_tg24_notte <- sql("select percentile_approx(visite_tg24_notte, 0.99) as p99 
                                    from df_variabile_24 
                                    where visite_tg24_notte is NOT NULL")

novantanove_visite_tg24_notte <- first(percentili_visite_tg24_notte) # seleziono il p99

nn_visite_tg24_notte <- as.integer(novantanove_visite_tg24_notte) # lo trasformo in integer

df_variabile_24$visite_tg24_notte <- ifelse(df_variabile_24$visite_tg24_notte > nn_visite_tg24_notte , nn_visite_tg24_notte, df_variabile_24$visite_tg24_notte) # sostituisco ci?? che e' oltre il p99 col p99


##### 108) visite_tg24_mattina: ####


percentili_visite_tg24_mattina <- sql("select percentile_approx(visite_tg24_mattina, 0.99) as p99 
                                      from df_variabile_24 
                                      where visite_tg24_mattina is NOT NULL")

novantanove_visite_tg24_mattina <- first(percentili_visite_tg24_mattina) # seleziono il p99

nn_visite_tg24_mattina <- as.integer(novantanove_visite_tg24_mattina) # lo trasformo in integer

df_variabile_24$visite_tg24_mattina <- ifelse(df_variabile_24$visite_tg24_mattina > nn_visite_tg24_mattina , nn_visite_tg24_mattina, df_variabile_24$visite_tg24_mattina) # sostituisco ci?? che e' oltre il p99 col p99


##### 109) visite_tg24_pomeriggio: ####


percentili_visite_tg24_pomeriggio <- sql("select percentile_approx(visite_tg24_pomeriggio, 0.99) as p99 
                                         from df_variabile_24 
                                         where visite_tg24_pomeriggio is NOT NULL")

novantanove_visite_tg24_pomeriggio <- first(percentili_visite_tg24_pomeriggio) # seleziono il p99

nn_visite_tg24_pomeriggio <- as.integer(novantanove_visite_tg24_pomeriggio) # lo trasformo in integer

df_variabile_24$visite_tg24_pomeriggio <- ifelse(df_variabile_24$visite_tg24_pomeriggio > nn_visite_tg24_pomeriggio , nn_visite_tg24_pomeriggio, df_variabile_24$visite_tg24_pomeriggio) # sostituisco ci?? che e' oltre il p99 col p99


##### 110) visite_tg24_sera: ####


percentili_visite_tg24_sera <- sql("select percentile_approx(visite_tg24_sera, 0.99) as p99 
                                   from df_variabile_24 
                                   where visite_tg24_sera is NOT NULL")

novantanove_visite_tg24_sera <- first(percentili_visite_tg24_sera) # seleziono il p99

nn_visite_tg24_sera <- as.integer(novantanove_visite_tg24_sera) # lo trasformo in integer

df_variabile_24$visite_tg24_sera <- ifelse(df_variabile_24$visite_tg24_sera > nn_visite_tg24_sera , nn_visite_tg24_sera, df_variabile_24$visite_tg24_sera) # sostituisco ci?? che e' oltre il p99 col p99



##### 111) visite_sport_notte: ####


percentili_visite_sport_notte <- sql("select percentile_approx(visite_sport_notte, 0.99) as p99 
                                     from df_variabile_24 
                                     where visite_sport_notte is NOT NULL")

novantanove_visite_sport_notte <- first(percentili_visite_sport_notte) # seleziono il p99

nn_visite_sport_notte <- as.integer(novantanove_visite_sport_notte) # lo trasformo in integer

df_variabile_24$visite_sport_notte <- ifelse(df_variabile_24$visite_sport_notte > nn_visite_sport_notte , nn_visite_sport_notte, df_variabile_24$visite_sport_notte) # sostituisco ci?? che e' oltre il p99 col p99


##### 112) visite_sport_mattina: ####


percentili_visite_sport_mattina <- sql("select percentile_approx(visite_sport_mattina, 0.99) as p99 
                                       from df_variabile_24 
                                       where visite_sport_mattina is NOT NULL")

novantanove_visite_sport_mattina <- first(percentili_visite_sport_mattina) # seleziono il p99

nn_visite_sport_mattina <- as.integer(novantanove_visite_sport_mattina) # lo trasformo in integer

df_variabile_24$visite_sport_mattina <- ifelse(df_variabile_24$visite_sport_mattina > nn_visite_sport_mattina , nn_visite_sport_mattina, df_variabile_24$visite_sport_mattina) # sostituisco ci?? che e' oltre il p99 col p99


##### 113) visite_sport_pomeriggio: ####


percentili_visite_sport_pomeriggio <- sql("select percentile_approx(visite_sport_pomeriggio, 0.99) as p99 
                                          from df_variabile_24 
                                          where visite_sport_pomeriggio is NOT NULL")

novantanove_visite_sport_pomeriggio <- first(percentili_visite_sport_pomeriggio) # seleziono il p99

nn_visite_sport_pomeriggio <- as.integer(novantanove_visite_sport_pomeriggio) # lo trasformo in integer

df_variabile_24$visite_sport_pomeriggio <- ifelse(df_variabile_24$visite_sport_pomeriggio > nn_visite_sport_pomeriggio , nn_visite_sport_pomeriggio, df_variabile_24$visite_sport_pomeriggio) # sostituisco ci?? che e' oltre il p99 col p99


##### 114) visite_sport_sera: ####


percentili_visite_sport_sera <- sql("select percentile_approx(visite_sport_sera, 0.99) as p99 
                                    from df_variabile_24 
                                    where visite_sport_sera is NOT NULL")

novantanove_visite_sport_sera <- first(percentili_visite_sport_sera) # seleziono il p99

nn_visite_sport_sera <- as.integer(novantanove_visite_sport_sera) # lo trasformo in integer

df_variabile_24$visite_sport_sera <- ifelse(df_variabile_24$visite_sport_sera > nn_visite_sport_sera , nn_visite_sport_sera, df_variabile_24$visite_sport_sera) # sostituisco ci?? che e' oltre il p99 col p99


##### 115) visite_appwsc_notte: ####


percentili_visite_appwsc_notte <- sql("select percentile_approx(visite_appwsc_notte, 0.99) as p99 
                                      from df_variabile_24 
                                      where visite_appwsc_notte is NOT NULL")

novantanove_visite_appwsc_notte <- first(percentili_visite_appwsc_notte) # seleziono il p99

nn_visite_appwsc_notte <- as.integer(novantanove_visite_appwsc_notte) # lo trasformo in integer

df_variabile_24$visite_appwsc_notte <- ifelse(df_variabile_24$visite_appwsc_notte > nn_visite_appwsc_notte , nn_visite_appwsc_notte, df_variabile_24$visite_appwsc_notte) # sostituisco ci?? che e' oltre il p99 col p99


##### 116) visite_appwsc_mattina: ####


percentili_visite_appwsc_mattina <- sql("select percentile_approx(visite_appwsc_mattina, 0.99) as p99 
                                        from df_variabile_24 
                                        where visite_appwsc_mattina is NOT NULL")

novantanove_visite_appwsc_mattina <- first(percentili_visite_appwsc_mattina) # seleziono il p99

nn_visite_appwsc_mattina <- as.integer(novantanove_visite_appwsc_mattina) # lo trasformo in integer

df_variabile_24$visite_appwsc_mattina <- ifelse(df_variabile_24$visite_appwsc_mattina > nn_visite_appwsc_mattina , nn_visite_appwsc_mattina, df_variabile_24$visite_appwsc_mattina) # sostituisco ci?? che e' oltre il p99 col p99


##### 117) visite_appwsc_pomeriggio: ####


percentili_visite_appwsc_pomeriggio <- sql("select percentile_approx(visite_appwsc_pomeriggio, 0.99) as p99 
                                           from df_variabile_24 
                                           where visite_appwsc_pomeriggio is NOT NULL")

novantanove_visite_appwsc_pomeriggio <- first(percentili_visite_appwsc_pomeriggio) # seleziono il p99

nn_visite_appwsc_pomeriggio <- as.integer(novantanove_visite_appwsc_pomeriggio) # lo trasformo in integer

df_variabile_24$visite_appwsc_pomeriggio <- ifelse(df_variabile_24$visite_appwsc_pomeriggio > nn_visite_appwsc_pomeriggio , nn_visite_appwsc_pomeriggio, df_variabile_24$visite_appwsc_pomeriggio) # sostituisco ci?? che e' oltre il p99 col p99


##### 118) visite_appwsc_sera: ####


percentili_visite_appwsc_sera <- sql("select percentile_approx(visite_appwsc_sera, 0.99) as p99 
                                     from df_variabile_24 
                                     where visite_appwsc_sera is NOT NULL")

novantanove_visite_appwsc_sera <- first(percentili_visite_appwsc_sera) # seleziono il p99

nn_visite_appwsc_sera <- as.integer(novantanove_visite_appwsc_sera) # lo trasformo in integer

df_variabile_24$visite_appwsc_sera <- ifelse(df_variabile_24$visite_appwsc_sera > nn_visite_appwsc_sera , nn_visite_appwsc_sera, df_variabile_24$visite_appwsc_sera) # sostituisco ci?? che e' oltre il p99 col p99


##### 119) visite_appgtv_notte: ####


percentili_visite_appgtv_notte <- sql("select percentile_approx(visite_appgtv_notte, 0.99) as p99 
                                      from df_variabile_24 
                                      where visite_appgtv_notte is NOT NULL")

novantanove_visite_appgtv_notte <- first(percentili_visite_appgtv_notte) # seleziono il p99

nn_visite_appgtv_notte <- as.integer(novantanove_visite_appgtv_notte) # lo trasformo in integer

df_variabile_24$visite_appgtv_notte <- ifelse(df_variabile_24$visite_appgtv_notte > nn_visite_appgtv_notte , nn_visite_appgtv_notte, df_variabile_24$visite_appgtv_notte) # sostituisco ci?? che e' oltre il p99 col p99


##### 120) visite_appgtv_mattina: ####


percentili_visite_appgtv_mattina <- sql("select percentile_approx(visite_appgtv_mattina, 0.99) as p99 
                                        from df_variabile_24 
                                        where visite_appgtv_mattina is NOT NULL")

novantanove_visite_appgtv_mattina <- first(percentili_visite_appgtv_mattina) # seleziono il p99

nn_visite_appgtv_mattina <- as.integer(novantanove_visite_appgtv_mattina) # lo trasformo in integer

df_variabile_24$visite_appgtv_mattina <- ifelse(df_variabile_24$visite_appgtv_mattina > nn_visite_appgtv_mattina , nn_visite_appgtv_mattina, df_variabile_24$visite_appgtv_mattina) # sostituisco ci?? che e' oltre il p99 col p99


##### 121) visite_appgtv_pomeriggio: ####


percentili_visite_appgtv_pomeriggio <- sql("select percentile_approx(visite_appgtv_pomeriggio, 0.99) as p99 
                                           from df_variabile_24 
                                           where visite_appgtv_pomeriggio is NOT NULL")

novantanove_visite_appgtv_pomeriggio <- first(percentili_visite_appgtv_pomeriggio) # seleziono il p99

nn_visite_appgtv_pomeriggio <- as.integer(novantanove_visite_appgtv_pomeriggio) # lo trasformo in integer

df_variabile_24$visite_appgtv_pomeriggio <- ifelse(df_variabile_24$visite_appgtv_pomeriggio > nn_visite_appgtv_pomeriggio , nn_visite_appgtv_pomeriggio, df_variabile_24$visite_appgtv_pomeriggio) # sostituisco ci?? che e' oltre il p99 col p99


##### 122) visite_appgtv_sera: ####


percentili_visite_appgtv_sera <- sql("select percentile_approx(visite_appgtv_sera, 0.99) as p99 
                                     from df_variabile_24 
                                     where visite_appgtv_sera is NOT NULL")

novantanove_visite_appgtv_sera <- first(percentili_visite_appgtv_sera) # seleziono il p99

nn_visite_appgtv_sera <- as.integer(novantanove_visite_appgtv_sera) # lo trasformo in integer

df_variabile_24$visite_appgtv_sera <- ifelse(df_variabile_24$visite_appgtv_sera > nn_visite_appgtv_sera , nn_visite_appgtv_sera, df_variabile_24$visite_appgtv_sera) # sostituisco ci?? che e' oltre il p99 col p99


##### 123) visite_appsport_notte: ####


percentili_visite_appsport_notte <- sql("select percentile_approx(visite_appsport_notte, 0.99) as p99 
                                        from df_variabile_24 
                                        where visite_appsport_notte is NOT NULL")

novantanove_visite_appsport_notte <- first(percentili_visite_appsport_notte) # seleziono il p99

nn_visite_appsport_notte <- as.integer(novantanove_visite_appsport_notte) # lo trasformo in integer

df_variabile_24$visite_appsport_notte <- ifelse(df_variabile_24$visite_appsport_notte > nn_visite_appsport_notte , nn_visite_appsport_notte, df_variabile_24$visite_appsport_notte) # sostituisco ci?? che e' oltre il p99 col p99


##### 124) visite_appsport_mattina: ####


percentili_visite_appsport_mattina <- sql("select percentile_approx(visite_appsport_mattina, 0.99) as p99 
                                          from df_variabile_24 
                                          where visite_appsport_mattina is NOT NULL")

novantanove_visite_appsport_mattina <- first(percentili_visite_appsport_mattina) # seleziono il p99

nn_visite_appsport_mattina <- as.integer(novantanove_visite_appsport_mattina) # lo trasformo in integer

df_variabile_24$visite_appsport_mattina <- ifelse(df_variabile_24$visite_appsport_mattina > nn_visite_appsport_mattina , nn_visite_appsport_mattina, df_variabile_24$visite_appsport_mattina) # sostituisco ci?? che e' oltre il p99 col p99


##### 125) visite_appsport_pomeriggio: ####


percentili_visite_appsport_pomeriggio <- sql("select percentile_approx(visite_appsport_pomeriggio, 0.99) as p99 
                                             from df_variabile_24 
                                             where visite_appsport_pomeriggio is NOT NULL")

novantanove_visite_appsport_pomeriggio <- first(percentili_visite_appsport_pomeriggio) # seleziono il p99

nn_visite_appsport_pomeriggio <- as.integer(novantanove_visite_appsport_pomeriggio) # lo trasformo in integer

df_variabile_24$visite_appsport_pomeriggio <- ifelse(df_variabile_24$visite_appsport_pomeriggio > nn_visite_appsport_pomeriggio , nn_visite_appsport_pomeriggio, df_variabile_24$visite_appsport_pomeriggio) # sostituisco ci?? che e' oltre il p99 col p99


##### 126) visite_appsport_sera: ####


percentili_visite_appsport_sera <- sql("select percentile_approx(visite_appsport_sera, 0.99) as p99 
                                       from df_variabile_24 
                                       where visite_appsport_sera is NOT NULL")

novantanove_visite_appsport_sera <- first(percentili_visite_appsport_sera) # seleziono il p99

nn_visite_appsport_sera <- as.integer(novantanove_visite_appsport_sera) # lo trasformo in integer

df_variabile_24$visite_appsport_sera <- ifelse(df_variabile_24$visite_appsport_sera > nn_visite_appsport_sera , nn_visite_appsport_sera, df_variabile_24$visite_appsport_sera) # sostituisco ci?? che e' oltre il p99 col p99


##### 127) visite_appxfactor_notte: ####


percentili_visite_appxfactor_notte <- sql("select percentile_approx(visite_appxfactor_notte, 0.99) as p99 
                                          from df_variabile_24 
                                          where visite_appxfactor_notte is NOT NULL")

novantanove_visite_appxfactor_notte <- first(percentili_visite_appxfactor_notte) # seleziono il p99

nn_visite_appxfactor_notte <- as.integer(novantanove_visite_appxfactor_notte) # lo trasformo in integer

df_variabile_24$visite_appxfactor_notte <- ifelse(df_variabile_24$visite_appxfactor_notte > nn_visite_appxfactor_notte , nn_visite_appxfactor_notte, df_variabile_24$visite_appxfactor_notte) # sostituisco ci?? che e' oltre il p99 col p99


##### 128) visite_appxfactor_mattina: ####


percentili_visite_appxfactor_mattina <- sql("select percentile_approx(visite_appxfactor_mattina, 0.99) as p99 
                                            from df_variabile_24 
                                            where visite_appxfactor_mattina is NOT NULL")

novantanove_visite_appxfactor_mattina <- first(percentili_visite_appxfactor_mattina) # seleziono il p99

nn_visite_appxfactor_mattina <- as.integer(novantanove_visite_appxfactor_mattina) # lo trasformo in integer

df_variabile_24$visite_appxfactor_mattina <- ifelse(df_variabile_24$visite_appxfactor_mattina > nn_visite_appxfactor_mattina , nn_visite_appxfactor_mattina, df_variabile_24$visite_appxfactor_mattina) # sostituisco ci?? che e' oltre il p99 col p99


##### 129) visite_appxfactor_pomeriggio: ####


percentili_visite_appxfactor_pomeriggio <- sql("select percentile_approx(visite_appxfactor_pomeriggio, 0.99) as p99 
                                               from df_variabile_24 
                                               where visite_appxfactor_pomeriggio is NOT NULL")

novantanove_visite_appxfactor_pomeriggio <- first(percentili_visite_appxfactor_pomeriggio) # seleziono il p99

nn_visite_appxfactor_pomeriggio <- as.integer(novantanove_visite_appxfactor_pomeriggio) # lo trasformo in integer

df_variabile_24$visite_appxfactor_pomeriggio <- ifelse(df_variabile_24$visite_appxfactor_pomeriggio > nn_visite_appxfactor_pomeriggio, 
                                                       nn_visite_appxfactor_pomeriggio, df_variabile_24$visite_appxfactor_pomeriggio) # sostituisco ci?? che e' oltre il p99 col p99


##### 130) visite_appxfactor_sera: ####


percentili_visite_appxfactor_sera <- sql("select percentile_approx(visite_appxfactor_sera, 0.99) as p99 
                                         from df_variabile_24 
                                         where visite_appxfactor_sera is NOT NULL")

novantanove_visite_appxfactor_sera <- first(percentili_visite_appxfactor_sera) # seleziono il p99

nn_visite_appxfactor_sera <- as.integer(novantanove_visite_appxfactor_sera) # lo trasformo in integer

df_variabile_24$visite_appxfactor_sera <- ifelse(df_variabile_24$visite_appxfactor_sera > nn_visite_appxfactor_sera , nn_visite_appxfactor_sera, df_variabile_24$visite_appxfactor_sera) # sostituisco ci?? che e' oltre il p99 col p99




write.parquet(df_variabile_24, path_df_variabile_24)

df_variabile_24 <- read.parquet(path_df_variabile_24)

# # test a campione
# 
# createOrReplaceTempView(df_variabile_24, 'df_variabile_24')
# 
# percentili_avg_visite_mese_app <- sql("select percentile_approx(avg_visite_mese_app, 0.99)  as p99,
#                                       percentile_approx(avg_visite_mese_app, 1) as max from df_variabile_24 where avg_visite_mese_app is NOT NULL")
# 
# View(head(percentili_avg_visite_mese_app
# ))
