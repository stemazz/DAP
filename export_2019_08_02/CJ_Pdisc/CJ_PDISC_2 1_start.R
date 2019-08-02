
# CJ_PDISC_2



#apri sessione
source("connection_R.R")
options(scipen = 1000)


###################################################################################################################################################################################

export_from_cmdm <- read.df("/user/valentina/export_base_pdisc_contattailita.csv", source = "csv", header = TRUE, delimiter = ",")

View(head(export_from_cmdm, 1000))
nrow(export_from_cmdm)
# 37.976


###################################################################################################################################################################################
###################################################################################################################################################################################
## Import output model CJ_PDISC 
###################################################################################################################################################################################
###################################################################################################################################################################################


output_model <- read.df("/user/stefano.mazzucca/export_model_cj_pdisc.csv", source = "csv", header = "true", delimiter = ",")
View(head(output_model,100))
nrow(output_model)
# 10.963

output_model_1 <- arrange(output_model, desc(output_model$Prediction_1_class))
View(head(output_model_1,100))


output_model_2 <- withColumn(output_model_1, "homepage_sky_", cast(output_model_1$homepage_sky, "integer"))
output_model_2$homepage_sky <- NULL

output_model_2 <- withColumn(output_model_2, "faidate_home_", cast(output_model_1$faidate_home, "integer"))
output_model_2$faidate_home <- NULL

output_model_2 <- withColumn(output_model_2, "faidate_fatture_pagamenti_", cast(output_model_1$faidate_fatture_pagamenti, "integer"))
output_model_2$faidate_fatture_pagamenti <- NULL

output_model_2 <- withColumn(output_model_2, "assistenza_moduli_contrattuali_", cast(output_model_1$assistenza_moduli_contrattuali, "integer"))
output_model_2$assistenza_moduli_contrattuali <- NULL

output_model_2 <- withColumn(output_model_2, "assistenza_contatta_", cast(output_model_1$assistenza_contatta, "integer"))
output_model_2$assistenza_contatta <- NULL

output_model_2 <- withColumn(output_model_2, "app_assistenza_contatta_", cast(output_model_1$app_assistenza_contatta, "integer"))
output_model_2$app_assistenza_contatta <- NULL

output_model_2 <- withColumn(output_model_2, "app_i_miei_dati_", cast(output_model_1$app_i_miei_dati, "integer"))
output_model_2$app_i_miei_dati <- NULL

output_model_2 <- withColumn(output_model_2, "faidate_gestisci_dati_servizi_", cast(output_model_1$faidate_gestisci_dati_servizi, "integer"))
output_model_2$faidate_gestisci_dati_servizi <- NULL

output_model_2 <- withColumn(output_model_2, "app_assistenza_ricerca_", cast(output_model_1$app_assistenza_ricerca, "integer"))
output_model_2$app_assistenza_ricerca <- NULL

output_model_2 <- withColumn(output_model_2, "assistenza_ricerca_", cast(output_model_1$assistenza_ricerca, "integer"))
output_model_2$assistenza_ricerca <- NULL

output_model_2 <- withColumn(output_model_2, "assistenza_sky_expert_", cast(output_model_1$assistenza_sky_expert, "integer"))
output_model_2$assistenza_sky_expert <- NULL

output_model_2 <- withColumn(output_model_2, "assistenza_trova_sky_service_", cast(output_model_1$assistenza_trova_sky_service, "integer"))
output_model_2$assistenza_trova_sky_service <- NULL

output_model_2 <- withColumn(output_model_2, "assistenza_info_disdetta_", cast(output_model_1$assistenza_info_disdetta, "integer"))
output_model_2$assistenza_info_disdetta <- NULL


output_model_2 <- withColumn(output_model_2, "Prediction_1_class_", cast(output_model_1$Prediction_1_class, "float"))
output_model_2$Prediction_1_class <- NULL

output_model_2 <- withColumn(output_model_2, "flg_31gg_", cast(output_model_1$flg_31gg, "integer"))
output_model_2$flg_31gg <- NULL


View(head(output_model_2,1000))
nrow(output_model_2)
# 10.963


createOrReplaceTempView(output_model_2, "output_model_2")
output_model_3 <- sql("select COD_CLIENTE_CIFRATO,
                              flg_31gg_,
                              Prediction_1_class_,
                              `Last(data_ingr_pdisc_formdate` as data_ingr_pdisc,
                              `Last(one_month_before_pdisc` as one_month_before_pdisc,
                              homepage_sky_,
                              faidate_home_,
                              faidate_fatture_pagamenti_,
                              assistenza_moduli_contrattuali_,
                              assistenza_contatta_,
                              app_assistenza_contatta_,
                              app_i_miei_dati_,
                              faidate_gestisci_dati_servizi_,
                              app_assistenza_ricerca_,
                              assistenza_ricerca_,
                              assistenza_sky_expert_,
                              assistenza_trova_sky_service_,
                              assistenza_info_disdetta_
                      from output_model_2")
View(head(output_model_3,1000))
nrow(output_model_3)
# 10.963

#     (homepage_sky + faidate_home + faidate_fatture_pagamenti + assistenza_moduli_contrattuali + assistenza_home + 
#     app_assistenza_home + app_assistenza_contatta + assistenza_contatta + 
#     app_i_miei_dati + faidate_gestisci_dati_servizi + app_assistenza_ricerca + 
#     assistenza_sky_expert + assistenza_ricerca + assistenza_trova_sky_service + 
#     Home_Guidatv + app_assistenza + assistenza_Other + assistenza_clienti + assistenza_disdetta + assistenza_fattura_elettronica + 
#     assistenza_gsa + assistenza_info_disdetta_modulo_recesso_entro_14_giorni_timsky + 
#     assistenza_info_disdetta_modulo_recesso_no_scadenza_timsky + `assistenza_informazioni-energetiche-sky-q` + 
#     `assistenza_piu-giorni-sky` + `assistenza_piusky-success` + assistenza_print + 
#     assistenza_registration_modifica + assistenza_riepilogo_homepack + assistenza_schedeservice + 
#     assistenza_sky_id + assistenza_sky_q + assistenza_sms + assistenza_vo + assistenza_info_disdetta_modulo_scadenza_contratto_timsky + 
#     assistenza_info_disdetta_modulo_recesso_entro_14_giorni + `assistenza_richiedi-assistenza-ok` + assistenza_faq_homepack + 
#     `assistenza_trasparenza-tariffaria` + assistenza_skyready + assistenza_benvenuto + app_assistenza_gestisci + 
#     app_assistenza_conosci + assistenza_info_disdetta_modulo_scadenza_contratto + app_widget_contatta_sky + 
#     `pacchetti-offerte` + assistenza_trasloca_sky + app_assistenza_risolvi + app_assistenza_e_supporto + 
#     app_widget + app_impostazioni + app_widget_ultimafattura + app_widget_gestisci + tecnologia + 
#     app_il_mio_abbonamento + app_widget_dispositivi + assistenza_info_disdetta_modulo_recesso_no_scadenza + 
#     assistenza_assistenza_skyq + assistenza_conferma_richiesta_assistenza + faidate_webtracking + 
#     app_sky_extra + app_home + `assistenza_richiedi-assistenza-ok-appview` + assistenza_conosci + 
#     assistenza_gestisci + faidate_extra + extra + app_dati_e_fatture + app_comunicazioni + 
#     assistenza_risolvi + app_arricchisci_abbonamento + faidate_arricchisci_abbonamento + 
#     pagine_di_servizio + assistenza_info_disdetta) as count_tot_pag_viz,


output_model_4 <- withColumn(output_model_3, "data_ingr_pdisc_dt", cast(output_model_3$data_ingr_pdisc, "date"))
output_model_5 <- withColumn(output_model_4, "one_month_before_pdisc_dt", cast(output_model_4$one_month_before_pdisc, "date"))

View(head(output_model_4,1000))
nrow(output_model_4)
# 10.963

ver_extid <- distinct(select(output_model_4, "COD_CLIENTE_CIFRATO"))
nrow(ver_extid)
# 9.640 (vs 10.963 records -> significa che sono stati presi alcuni clienti sia per flg=1 sia per flg=0)



###################################################################################################################################################################################
## Filtro sui soli utenti che hanno nella navigazione un numero di visite pi?? alto nelle pagine TOP SCORE rispetto alla pagina INFO_DISDETTA: 
###################################################################################################################################################################################

filt_top_id <- filter(output_model_5, "homepage_sky_ >= assistenza_info_disdetta_ and 
                                        faidate_home_ >= assistenza_info_disdetta_ and 
                                        faidate_fatture_pagamenti_ >= assistenza_info_disdetta_ and 
                                        assistenza_moduli_contrattuali_ >= assistenza_info_disdetta_ and 
                                        assistenza_contatta_ >= assistenza_info_disdetta_ and 
                                        app_assistenza_contatta_ >= assistenza_info_disdetta_ and 
                                        app_i_miei_dati_ >= assistenza_info_disdetta_ and 
                                        faidate_gestisci_dati_servizi_ >= assistenza_info_disdetta_ and 
                                        app_assistenza_ricerca_ >= assistenza_info_disdetta_ and 
                                        assistenza_ricerca_ >= assistenza_info_disdetta_ and 
                                        assistenza_sky_expert_ >= assistenza_info_disdetta_ and 
                                        assistenza_trova_sky_service_ >= assistenza_info_disdetta_")
View(head(filt_top_id,1000))
nrow(filt_top_id)
# 7.433

filt_top_id_2 <- filter(output_model_5, "homepage_sky_ > assistenza_info_disdetta_ and 
                                        faidate_home_ > assistenza_info_disdetta_ and 
                                        faidate_fatture_pagamenti_ > assistenza_info_disdetta_ and 
                                        assistenza_moduli_contrattuali_ > assistenza_info_disdetta_ and 
                                        assistenza_contatta_ > assistenza_info_disdetta_ and 
                                        app_assistenza_contatta_ > assistenza_info_disdetta_ and 
                                        app_i_miei_dati_ > assistenza_info_disdetta_ and 
                                        faidate_gestisci_dati_servizi_ > assistenza_info_disdetta_ and 
                                        app_assistenza_ricerca_ > assistenza_info_disdetta_ and 
                                        assistenza_ricerca_ > assistenza_info_disdetta_ and 
                                        assistenza_sky_expert_ > assistenza_info_disdetta_ and 
                                        assistenza_trova_sky_service_ > assistenza_info_disdetta_")
View(head(filt_top_id_2,1000))
nrow(filt_top_id_2)
# 0

filt_top_id_3 <- filter(output_model_5, "(homepage_sky_ >= assistenza_info_disdetta_ or 
                                        faidate_home_ >= assistenza_info_disdetta_) and 
                                        (faidate_fatture_pagamenti_ >= assistenza_info_disdetta_ or 
                                        assistenza_moduli_contrattuali_ >= assistenza_info_disdetta_) and 
                                        (assistenza_contatta_ >= assistenza_info_disdetta_ or 
                                        app_assistenza_contatta_ >= assistenza_info_disdetta_) and 
                                        (app_i_miei_dati_ >= assistenza_info_disdetta_ or 
                                        faidate_gestisci_dati_servizi_ >= assistenza_info_disdetta_) and 
                                        (app_assistenza_ricerca_ >= assistenza_info_disdetta_ or 
                                        assistenza_ricerca_ >= assistenza_info_disdetta_) and 
                                        (assistenza_sky_expert_ >= assistenza_info_disdetta_ or 
                                        assistenza_trova_sky_service_ >= assistenza_info_disdetta_)")
View(head(filt_top_id_3,1000))
nrow(filt_top_id_3)
# 7.442

filt_top_id_4 <- filter(output_model_5, "(homepage_sky_ > assistenza_info_disdetta_ or 
                                        faidate_home_ > assistenza_info_disdetta_) and 
                                        (faidate_fatture_pagamenti_ > assistenza_info_disdetta_ or 
                                        assistenza_moduli_contrattuali_ > assistenza_info_disdetta_) and 
                                        (assistenza_contatta_ > assistenza_info_disdetta_ or 
                                        app_assistenza_contatta_ > assistenza_info_disdetta_) and 
                                        (app_i_miei_dati_ > assistenza_info_disdetta_ or 
                                        faidate_gestisci_dati_servizi_ > assistenza_info_disdetta_) and 
                                        (app_assistenza_ricerca_ > assistenza_info_disdetta_ or 
                                        assistenza_ricerca_ > assistenza_info_disdetta_) and 
                                        (assistenza_sky_expert_ > assistenza_info_disdetta_ or 
                                        assistenza_trova_sky_service_ > assistenza_info_disdetta_)")
View(head(filt_top_id_4,1000))
nrow(filt_top_id_4)
# 45


###################################################################################################################################################################################

output_model_6 <- filter(output_model_5, "assistenza_info_disdetta_ > 0")
nrow(output_model_6)
# 3.530

filt_2_top_id <- filter(output_model_6, "homepage_sky_ >= assistenza_info_disdetta_ and 
                                        faidate_home_ >= assistenza_info_disdetta_ and 
                                        faidate_fatture_pagamenti_ >= assistenza_info_disdetta_ and 
                                        assistenza_moduli_contrattuali_ >= assistenza_info_disdetta_ and 
                                        assistenza_contatta_ >= assistenza_info_disdetta_ and 
                                        app_assistenza_contatta_ >= assistenza_info_disdetta_ and 
                                        app_i_miei_dati_ >= assistenza_info_disdetta_ and 
                                        faidate_gestisci_dati_servizi_ >= assistenza_info_disdetta_ and 
                                        app_assistenza_ricerca_ >= assistenza_info_disdetta_ and 
                                        assistenza_ricerca_ >= assistenza_info_disdetta_ and 
                                        assistenza_sky_expert_ >= assistenza_info_disdetta_ and 
                                        assistenza_trova_sky_service_ >= assistenza_info_disdetta_")
View(head(filt_2_top_id,1000))
nrow(filt_2_top_id)
# 0

filt_2_top_id_3 <- filter(output_model_6, "(homepage_sky_ >= assistenza_info_disdetta_ or 
                                        faidate_home_ >= assistenza_info_disdetta_) and 
                                        (faidate_fatture_pagamenti_ >= assistenza_info_disdetta_ or 
                                        assistenza_moduli_contrattuali_ >= assistenza_info_disdetta_) and 
                                        (assistenza_contatta_ >= assistenza_info_disdetta_ or 
                                        app_assistenza_contatta_ >= assistenza_info_disdetta_) and 
                                        (app_i_miei_dati_ >= assistenza_info_disdetta_ or 
                                        faidate_gestisci_dati_servizi_ >= assistenza_info_disdetta_) and 
                                        (app_assistenza_ricerca_ >= assistenza_info_disdetta_ or 
                                        assistenza_ricerca_ >= assistenza_info_disdetta_) and 
                                        (assistenza_sky_expert_ >= assistenza_info_disdetta_ or 
                                        assistenza_trova_sky_service_ >= assistenza_info_disdetta_)")
View(head(filt_2_top_id_3,1000))
nrow(filt_2_top_id_3)
# 9

filt_2_top_id_4 <- filter(output_model_6, "(homepage_sky_ > assistenza_info_disdetta_ or 
                                        faidate_home_ > assistenza_info_disdetta_) and 
                                        (faidate_fatture_pagamenti_ > assistenza_info_disdetta_ or 
                                        assistenza_moduli_contrattuali_ > assistenza_info_disdetta_) and 
                                        (assistenza_contatta_ > assistenza_info_disdetta_ or 
                                        app_assistenza_contatta_ > assistenza_info_disdetta_) and 
                                        (app_i_miei_dati_ > assistenza_info_disdetta_ or 
                                        faidate_gestisci_dati_servizi_ > assistenza_info_disdetta_) and 
                                        (app_assistenza_ricerca_ > assistenza_info_disdetta_ or 
                                        assistenza_ricerca_ > assistenza_info_disdetta_) and 
                                        (assistenza_sky_expert_ > assistenza_info_disdetta_ or 
                                        assistenza_trova_sky_service_ > assistenza_info_disdetta_)")
View(head(filt_2_top_id_4,1000))
nrow(filt_2_top_id_4)
# 2


###################################################################################################################################################################################
## Focus su "faidate_fatture_pagamenti"
###################################################################################################################################################################################

output_model_focus_fatture <- filter(output_model_5, "faidate_fatture_pagamenti_ > assistenza_info_disdetta_")
View(head(output_model_focus_fatture,1000))
nrow(output_model_focus_fatture)
# 3.016 (rispetto ai 10.963 record totali)

focus_fatture <- summarize(output_model_focus_fatture, media = mean(output_model_focus_fatture$Prediction_1_class_))
View(head(focus_fatture))
# 0.3706219



###################################################################################################################################################################################
## Focus su "assistenza_moduli_contrattuali"
###################################################################################################################################################################################

output_model_focus_mod_contrattuali <- filter(output_model_5, "assistenza_moduli_contrattuali_ > assistenza_info_disdetta_")
View(head(output_model_focus_mod_contrattuali,1000))
nrow(output_model_focus_mod_contrattuali)
# 818 (rispetto ai 10.963 record totali)

focus_mod_contrattuali <- summarize(output_model_focus_mod_contrattuali, media = mean(output_model_focus_mod_contrattuali$Prediction_1_class_))
View(head(focus_mod_contrattuali))
# 0.4400114



###################################################################################################################################################################################
## Focus su "assistenza_contatta" & "app_assistenza_contatta"
###################################################################################################################################################################################

output_model_focus_ass_contatta <- filter(output_model_5, "assistenza_contatta_ > assistenza_info_disdetta_ or app_assistenza_contatta_ > assistenza_info_disdetta_")
View(head(output_model_focus_ass_contatta,1000))
nrow(output_model_focus_ass_contatta)
# 2.953 (rispetto ai 10.963 record totali)

focus_ass_contatta <- summarize(output_model_focus_ass_contatta, media = mean(output_model_focus_ass_contatta$Prediction_1_class_))
View(head(focus_ass_contatta))
# 0.384251



###################################################################################################################################################################################
## Focus su "app_i_miei_dati" & "faidate_gestisci_dati_servizi"
###################################################################################################################################################################################

output_model_focus_dati <- filter(output_model_5, "app_i_miei_dati_ > assistenza_info_disdetta_ or faidate_gestisci_dati_servizi_ > assistenza_info_disdetta_")
View(head(output_model_focus_dati,1000))
nrow(output_model_focus_dati)
# 4.490 (rispetto ai 10.963 record totali)

focus_dati <- summarize(output_model_focus_dati, media = mean(output_model_focus_dati$Prediction_1_class_))
View(head(focus_dati))
# 0.3593071



###################################################################################################################################################################################
## Focus su "app_assistenza_ricerca" & "assistenza_ricerca"
###################################################################################################################################################################################

output_model_focus_ricerca <- filter(output_model_5, "app_assistenza_ricerca_ > assistenza_info_disdetta_ or assistenza_ricerca_ > assistenza_info_disdetta_")
View(head(output_model_focus_ricerca,1000))
nrow(output_model_focus_ricerca)
# 916 (rispetto ai 10.963 record totali)

focus_ricerca <- summarize(output_model_focus_ricerca, media = mean(output_model_focus_ricerca$Prediction_1_class_))
View(head(focus_ricerca))
# 0.3203568



###################################################################################################################################################################################
## Focus su "assistenza_sky_expert" & "assistenza_trova_sky_service"
###################################################################################################################################################################################

output_model_focus_service <- filter(output_model_5, "assistenza_sky_expert_ > assistenza_info_disdetta_ or assistenza_trova_sky_service_ > assistenza_info_disdetta_")
View(head(output_model_focus_service,1000))
nrow(output_model_focus_service)
# 344 (rispetto ai 10.963 record totali)

focus_service <- summarize(output_model_focus_service, media = mean(output_model_focus_service$Prediction_1_class_))
View(head(focus_service))
# 0.295355








#chiudi sessione
sparkR.stop()
