
## Estrazione campione per Pierpaolo


#apri sessione
source("connection_R.R")
options(scipen = 1000)

source("Utils_estrazione.R")



clienti_nopdisc_18 <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin.parquet")
View(head(clienti_nopdisc_18,100))
nrow(clienti_nopdisc_18)
# 3.943.566


# SELEZIONO COLONNE --------------------------------------------------------
campione_uniq <- distinct(select(clienti_nopdisc_18, "COD_CLIENTE_CIFRATO"))

# IMPORTO MAPPATURA ID ----------------------------------------------------
id_coo_campione <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_CookiesADOBE.parquet")
id_coo_campione_1 <- distinct(select(id_coo_campione, c("COD_CLIENTE_CIFRATO", "post_visid_concatenated")))
id_coo_campione_2 <- filter(id_coo_campione_1, "COD_CLIENTE_CIFRATO is NOT NULL")
id_coo_campione_3 <- filter(id_coo_campione_2, "post_visid_concatenated is NOT NULL")

# IMPORTO NAVIGAZIONE SITO -----------------------------------------------------
nav_campione <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_navigazSkyITDev_ok.parquet") 
nav_campione_1 <- filter(nav_campione,"date_time_dt >= '2017-05-01'")
nav_campione_2 <- filter(nav_campione_1,"date_time_dt <= '2018-04-30'")

nav_campione_3 <- filter(nav_campione_2, "post_channel = 'corporate' or post_channel = 'Guidatv'")
nrow(nav_campione_3)
# 37.565.912

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
nav_campione_4 <- remove_too_short(nav_campione_3, 10)
cache(nav_campione_4)

# KPI SITO ----------------------------------------------------------------
campione_info_nav <- start_kpi_sito(nav_campione_4, campione_uniq)


write.parquet(campione_info_nav, "/user/stefano.mazzucca/campione_sito_pierpaolo.parquet", mode = "overwrite")



# IMPORTO NAVIGAZIONE APP ------------------------------------------------------
nav_app_campione <- read.parquet("/user/valentina/scarico_base_xTest_Attivi_ad_Apr2018_fin_navigazAPP.parquet") # 22.780.179
#nav_app_campione_1 <- join_COD_CLIENTE_CIFRATO(campione_uniq, nav_app_campione) # 25.987.382

# RIMUOVO HIT CON DURATA TROPPO CORTA -------------------------------------
nav_app_campione_2 <- withColumnRenamed(nav_app_campione, "date_time", "date_time_ts")
nav_app_campione_3 <- withColumn(nav_app_campione_2, "date_time_ts", cast(nav_app_campione_2$date_time_ts, "timestamp"))
nav_app_campione_4 <- remove_too_short(nav_app_campione_3, 5)

# KPI APP -----------------------------------------------------------------
nav_app_campione_5 <- start_kpi_app(nav_app_campione_4, campione_uniq)


write.parquet(nav_app_campione_5, "/user/stefano.mazzucca/campione_app_pierpaolo.parquet", mode = "overwrite")



nav_sito <- read.parquet("/user/stefano.mazzucca/campione_sito_pierpaolo.parquet")
nav_app <- read.parquet("/user/stefano.mazzucca/campione_app_pierpaolo.parquet")

# MERGE KPI ---------------------------------------------------------------
nav_tot_campione <- join_COD_CLIENTE_CIFRATO(nav_sito, nav_app)
nav_tot_campione_1 <- melt_kpi(nav_tot_campione)

vars <- names(nav_tot_campione_1)
vars <- vars[str_detect(vars, pattern = "_sito_1w")]
vars <- str_replace(vars, "_sito_1w", "")

nav_tot_campione_2 <- differential_kpi(nav_tot_campione_1, vars)


write.parquet(nav_tot_campione_2, "/user/stefano.mazzucca/campione_tot_pierpaolo.parquet", mode = "overwrite")


################################################################################################################################################################


campione_info_nav <- read.parquet("/user/stefano.mazzucca/campione_tot_pierpaolo.parquet")
# View(head(campione_info_nav,100))
# nrow(campione_info_nav)
# # 3.841.121

campione_info_nav_filt <- select(campione_info_nav, "COD_CLIENTE_CIFRATO", 
                                 "visite_totali_sito_2m",
                                 "visite_Fatture_sito_2m",
                                 "visite_FaiDaTeExtra_sito_2m",
                                 "visite_ArricchisciAbbonamento_sito_2m",
                                 "visite_Gestione_sito_2m",
                                 "visite_Contatti_sito_2m",
                                 "visite_Comunicazione_sito_2m",
                                 "visite_Assistenza_sito_2m",
                                 "visite_Trasloco_sito_2m",
                                 "visite_TrasparenzaTariffaria_sito_2m",
                                 "visite_GuidaTv_sito_2m",
                                 "visite_PaginaDiServizio_sito_2m",
                                 "visite_Extra_sito_2m",
                                 "visite_Tecnologia_sito_2m",
                                 "visite_PacchettiOfferte_sito_2m",
                                 "visite_InfoDisdetta_sito_2m",
                                 "visite_TrovaSkyService_sito_2m",
                                 "visite_SearchDisdetta_sito_2m",
                                 "visite_SkyExpert_sito_2m",
                                 "secondi_totali_sito_2m",
                                 "secondi_Fatture_sito_2m",
                                 "secondi_FaiDaTeExtra_sito_2m",
                                 "secondi_ArricchisciAbbonamento_sito_2m",
                                 "secondi_Gestione_sito_2m",
                                 "secondi_Contatti_sito_2m",
                                 "secondi_Comunicazione_sito_2m",
                                 "secondi_Assistenza_sito_2m",
                                 "secondi_Trasloco_sito_2m",
                                 "secondi_TrasparenzaTariffaria_sito_2m",
                                 "secondi_GuidaTv_sito_2m",
                                 "secondi_PaginaDiServizio_sito_2m",
                                 "secondi_Extra_sito_2m",
                                 "secondi_Tecnologia_sito_2m",
                                 "secondi_PacchettiOfferte_sito_2m",
                                 "secondi_InfoDisdetta_sito_2m",
                                 "secondi_TrovaSkyService_sito_2m",
                                 "secondi_SearchDisdetta_sito_2m",
                                 "secondi_SkyExpert_sito_2m",
                                 "visite_totali_app_2m",
                                 "visite_fatture_app_2m",
                                 "visite_extra_app_2m",
                                 "visite_arricchisci_abbonamento_app_2m",
                                 "visite_stato_abbonamento_app_2m",
                                 "visite_contatta_sky_app_2m",
                                 "visite_assistenza_e_supporto_app_2m",
                                 "visite_comunicazioni_app_2m",
                                 "visite_gestisci_servizi_app_2m",
                                 "visite_mio_abbonamento_app_2m",
                                 "visite_info_app_2m",
                                 "secondi_totali_app_2m",
                                 "secondi_fatture_app_2m",
                                 "secondi_extra_app_2m",
                                 "secondi_arricchisci_abbonamento_app_2m",
                                 "secondi_stato_abbonamento_app_2m",
                                 "secondi_contatta_sky_app_2m",
                                 "secondi_assistenza_e_supporto_app_2m",
                                 "secondi_comunicazioni_app_2m",
                                 "secondi_gestisci_servizi_app_2m",
                                 "secondi_mio_abbonamento_app_2m",
                                 "secondi_info_app_2m",
                                 "visite_totali_2m",
                                 "visite_fatture_2m",
                                 "visite_extra_2m",
                                 "visite_arricchisciAbbonamento_2m",
                                 "visite_gestione_2m",
                                 "visite_contatti_2m",
                                 "visite_comunicazione_2m",
                                 "visite_assistenza_2m",
                                 "secondi_totali_2m",
                                 "secondi_fatture_2m",
                                 "secondi_extra_2m",
                                 "secondi_arricchisciAbbonamento_2m",
                                 "secondi_gestione_2m",
                                 "secondi_contatti_2m",
                                 "secondi_comunicazione_2m",
                                 "secondi_assistenza_2m")
# View(head(campione_info_nav_filt,100))
# nrow(campione_info_nav_filt)
# # 3.841.121

createOrReplaceTempView(campione_info_nav_filt, "campione_info_nav_filt")
campione_info_nav_1 <- sql("select *
                           from campione_info_nav_filt
                           where visite_totali_sito_2m <> 0 and visite_totali_app_2m <> 0 and visite_totali_2m <> 0")
# View(head(campione_info_nav_1,1000))
# nrow(campione_info_nav_1)
# # 381.139

campione_info_nav_2 <- sample(campione_info_nav_1, withReplacement=FALSE, fraction=0.55, seed = seed)
# View(head(campione_info_nav_2,1000))
# nrow(campione_info_nav_2)
# # 209.357

# ## Test #############################################################################################################
# 
# prova <- summary(campione_info_nav_2)
# View(head(prova,100))
# 
# ####################################################################################################################


## Assegno un codice progrssivo
cod_cliente_progressivo <- seq(1,nrow(campione_info_nav_2),1)
vet1 <- as.data.frame(cod_cliente_progressivo)


final <- collect(campione_info_nav_2)
final_1 <- cbind(vet1, final)

final_1$COD_CLIENTE_CIFRATO <- NULL


final_2 <- createDataFrame(final_1)
# View(head(final_2,100))
# nrow(final_2)
# # 209.357


write.df(repartition( final_2, 1), path = "/user/stefano.mazzucca/campione_stratificato_pierpaolo.csv", "csv", sep=";", mode = "overwrite", header=TRUE)
write.parquet(final_2, "/user/stefano.mazzucca/campione_stratificato_pierpaolo.parquet", mode = "overwrite")




# campione_filt <- read.df("/user/stefano.mazzucca/campione_stratificato_pierpaolo.csv", source = "csv", header = "true", delimiter = ";")
campione_filt <- read.parquet("/user/stefano.mazzucca/campione_stratificato_pierpaolo.parquet")
View(head(campione_filt,100))
nrow(campione_filt)
# 209.357

prova <- summary(campione_filt)
View(head(prova,100))



#chiudi sessione
sparkR.stop()
