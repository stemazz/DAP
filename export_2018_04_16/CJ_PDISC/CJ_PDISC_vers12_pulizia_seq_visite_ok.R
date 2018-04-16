
# CJ_PDISC_vers12
# sequenze visite "pulite"


#apri sessione
source("connection_R.R")
options(scipen = 1000)


# pulizia basi ----


# skypubblico  #################################################################################################################################################################

CJ_PDISC_tab_sito_pub <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_tab_sito_pub.parquet")

filter_1 <- filter(CJ_PDISC_tab_sito_pub, "page_url_post_evar like '%www.sky.it%'")
filter_sito_pub <- filter(filter_1,
                          "secondo_livello_post_evar like '%guidatv%' or
                           secondo_livello_post_evar like '%Guidatv%' or
                           secondo_livello_post_evar like '%pagine di servizio%' or
                           secondo_livello_post_evar like '%assistenza%' or
                           (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar like '%conosci%') or
                           (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar like '%gestisci%') or
                           (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar   like '%contatta%') or
                           (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar   like '%home%') or
                           (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar   like '%ricerca%' ) or
                           (secondo_livello_post_evar like '%assistenza%' and terzo_livello_post_evar   like '%risolvi%') or
                           secondo_livello_post_evar like '%fai da te%' or
                           secondo_livello_post_evar like '%extra%' or
                           secondo_livello_post_evar like '%tecnologia%' or
                           secondo_livello_post_evar like '%pacchetti-offerte%' ")


createOrReplaceTempView(filter_sito_pub, "filter_sito_pub")

skypubblico <- sql("select COD_CLIENTE_CIFRATO, 
                            post_visid_concatenated, 
                            visit_num, 
                            concatena_chiave, 
                            ts, 
                            data_ingr_pdisc_formdate, 
                            digitalizzazione_bin, 
                            hit_source, 
                            exclude_hit,
                            secondo_livello_post_evar, 
                            terzo_livello_post_evar,
                            download_name_post_prop
                    from filter_sito_pub")

skypubblico$sequenza <- ifelse(skypubblico$secondo_livello_post_evar == 'assistenza', 
                               concat_ws(sep="_", skypubblico$secondo_livello_post_evar, skypubblico$terzo_livello_post_evar), skypubblico$secondo_livello_post_evar)

View(head(filter(skypubblico, "secondo_livello_post_evar == 'assistenza'"), 1000))
View(head(skypubblico,100))
nrow(skypubblico)
# 696.885 (vs 696.885 di filter_sito_pub)

createOrReplaceTempView(skypubblico,"skypubblico")

skypubblico <- sql("SELECT COD_CLIENTE_CIFRATO, 
                            post_visid_concatenated, 
                            visit_num, 
                            concatena_chiave, 
                            ts, 
                            data_ingr_pdisc_formdate, 
                            digitalizzazione_bin, 
                            hit_source, 
                            exclude_hit, 
                            sequenza,
                            download_name_post_prop
                   FROM skypubblico
                   ORDER BY COD_CLIENTE_CIFRATO")

skypubblico = withColumn(skypubblico, "canale", lit("sito_pubblico"))

View(head(skypubblico, 100))


# skywsc  #####################################################################################################################################################################

CJ_PDISC_tab_wsc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_tab_wsc.parquet")

filter_wsc <- filter(CJ_PDISC_tab_wsc,"
                    terzo_livello_post_evar   like '%faidate gestisci dati servizi%' or
                    terzo_livello_post_evar   like '%faidate arricchisci abbonamento%' or
                    terzo_livello_post_evar   like '%faidate fatture pagamenti%' or
                    terzo_livello_post_evar   like '%faidate webtracking%' or
                    terzo_livello_post_evar   like '%faidate extra%' ") 

createOrReplaceTempView(filter_wsc, "filter_wsc")

skywsc <- sql("select COD_CLIENTE_CIFRATO, 
                            post_visid_concatenated, 
                            visit_num, 
                            concatena_chiave, 
                            ts, 
                            data_ingr_pdisc_formdate, 
                            digitalizzazione_bin, 
                            hit_source, 
                            exclude_hit,
                            terzo_livello_post_evar,
                            download_name_post_prop
                    from filter_wsc")

skywsc$sequenza <- skywsc$terzo_livello_post_evar
nrow(skywsc)
# 424.839 (vs 424.839 di filter_wsc)

createOrReplaceTempView(skywsc,"skywsc")

skywsc <- sql("SELECT COD_CLIENTE_CIFRATO, 
                        post_visid_concatenated, 
                        visit_num, 
                        concatena_chiave, 
                        ts, 
                        data_ingr_pdisc_formdate, 
                        digitalizzazione_bin, 
                        hit_source, 
                        exclude_hit,
                        sequenza,
                        download_name_post_prop
              FROM skywsc
              ORDER BY COD_CLIENTE_CIFRATO")

skywsc = withColumn(skywsc, "canale", lit("sito_wsc"))

View(head(skywsc,100))


# skyapp #####################################################################################################################################################################

CJ_PDISC_tab_app <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_tab_app.parquet")

filter_app <- filter(CJ_PDISC_tab_app,"
                      site_section like '%dati e fatture%' or
                      site_section like '%il mio abbonamento%' or
                      site_section like '%sky extra%' or
                      site_section like '%comunicazioni%' or
                      site_section like '%impostazioni%' or
                      site_section like '%i miei dati%' or
                      site_section like '%assistenza%' or
                      page_name_post_evar like '%assistenza:home%' or
                      page_name_post_evar like '%assistenza:contatta%' or
                      page_name_post_evar like '%assistenza:conosci%' or
                      page_name_post_evar like '%assistenza:ricerca%' or
                      page_name_post_evar like '%assistenza:gestisci%' or
                      page_name_post_evar like '%assistenza:risolvi%' or
                      site_section like '%widget%' or
                      page_name_post_evar like '%widget:dispositivi%' or
                      page_name_post_evar like '%widget:ultimafattura%' or
                      page_name_post_evar like '%widget:contatta sky%' or
                      page_name_post_evar like '%widget:gestisci%' or
                      page_name_post_evar = 'arricchisci abbonamento:home'") # controllare!

createOrReplaceTempView(filter_app, "filter_app")

skyapp <- sql("SELECT COD_CLIENTE_CIFRATO, 
                      post_visid_concatenated, 
                      visit_num, 
                      concatena_chiave, 
                      ts, 
                      data_ingr_pdisc_formdate, 
                      digitalizzazione_bin, 
                      hit_source, 
                      exclude_hit,
                      site_section,
                      page_name_post_evar
              FROM filter_app")

skyapp = withColumn(skyapp, "secondo_livello_post_evar", lit("app_faidate"))
skyapp$site_section_app <- concat_ws(sep=" ", skyapp$secondo_livello_post_evar, skyapp$site_section)
skyapp$page_name_app <- concat_ws(sep=" ", skyapp$secondo_livello_post_evar, skyapp$page_name_post_evar)

skyapp$sequenza <- ifelse(skyapp$site_section_app == 'app_faidate assistenza', skyapp$page_name_app, skyapp$site_section_app)
skyapp$sequenza <- ifelse(skyapp$site_section_app == 'app_faidate widget', skyapp$page_name_app, skyapp$site_section_app)

skyapp$sequenza = ifelse(contains(skyapp$page_name_app, c("conosci")), 'app_faidate assistenza conosci', 
                         ifelse(contains(skyapp$page_name_app, c("gestisci")), 'app_faidate assistenza gestisci', 
                                ifelse(contains(skyapp$page_name_app, c("home")), 'app_faidate assistenza home',
                                       ifelse(contains(skyapp$page_name_app, c("contatta")), 'app_faidate assistenza contatta',
                                              ifelse(contains(skyapp$page_name_app, c("risolvi")), 'app_faidate assistenza risolvi',
                                                     ifelse(contains(skyapp$page_name_app, c("ricerca")), 'app_faidate assistenza ricerca',skyapp$sequenza))))))

skyapp$sequenza = ifelse(contains(skyapp$page_name_app, c("dispositivi")), 'app_faidate widget dispositivi', 
                         ifelse(contains(skyapp$page_name_app, c("ultimafattura")), 'app_faidate widget ultimafattura', 
                                ifelse(contains(skyapp$page_name_app, c("contatta sky")), 'app_faidate widget contatta sky',
                                       ifelse(contains(skyapp$page_name_app, c("widget:gestisci")), 'app_faidate widget gestisci', skyapp$sequenza))))

View(head(skyapp,100))
nrow(skyapp)
# 2.844.660 (vs 2.844.660 di filter_app) # 2.278.582 (senza: page_name_post_evar = 'arricchisci abbonamento:home')
# 3.542.960

createOrReplaceTempView(skyapp,"skyapp")

skyapp <- sql("SELECT COD_CLIENTE_CIFRATO, 
                        post_visid_concatenated, 
                        visit_num, 
                        concatena_chiave, 
                        ts, 
                        data_ingr_pdisc_formdate, 
                        digitalizzazione_bin, 
                        hit_source, 
                        exclude_hit,
                        sequenza
              FROM skyapp")

#skyapp = withColumn(skyapp, "download_name_post_prop", lit(""))
createOrReplaceTempView(skyapp,"skyapp")

skyapp_1 <- sql("select *, 
                  NULL as download_name_post_prop
                from skyapp")

skyapp_1 = withColumn(skyapp_1, "canale", lit("app"))

View(head(skyapp_1,100))


###############################################################################################################################################################################

sky_pubbl_wsc_app <- rbind(skywsc, skypubblico, skyapp_1)

createOrReplaceTempView(sky_pubbl_wsc_app, "sky_pubbl_wsc_app")
sky_pubbl_wsc_app$sequenza <- regexp_replace(sky_pubbl_wsc_app$sequenza, " ", "_")

sky_pubbl_wsc_app <- withColumn(sky_pubbl_wsc_app, "giorno_visualizzaz", cast(sky_pubbl_wsc_app$ts, "date"))
# sky_pubbl_wsc_app <- withColumn(sky_pubbl_wsc_app, "giorno_visualizzaz", cast(sky_pubbl_wsc_app$giorno_visualizzaz, "timestamp"))


createOrReplaceTempView(sky_pubbl_wsc_app,"sky_pubbl_wsc_app")

sky_pubbl_wsc_app_1 <- sql("select *
                           from sky_pubbl_wsc_app
                           order by COD_CLIENTE_CIFRATO, ts, concatena_chiave")

View(head(sky_pubbl_wsc_app_1, 1000))
str(sky_pubbl_wsc_app_1)

nrow(skypubblico) + nrow(skyapp) + nrow(skywsc)
# 4.253.420 # 3.687.342
# 4.951.720
nrow(sky_pubbl_wsc_app_1)
# 4.253.420 #  3.687.342
# 4.951.720


# ver <- filter(sky_pubbl_wsc_app, "download_name_post_prop is NOT NULL")
# View(head(ver,1000))
# nrow(ver)
# # 19.210
# 
# ver_3 <- filter(sky_pubbl_wsc_app, "download_name_post_prop is NULL")
# View(head(ver_3,100))
# nrow(ver_3)
# # 4.234.210

valdist_cleinti_nav <- distinct(select(sky_pubbl_wsc_app_1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cleinti_nav)
# 21.702 # 21.859


write.parquet(sky_pubbl_wsc_app_1,"/user/stefano.mazzucca/CJ_PDISC_sequenze_nav.parquet")




#chiudi sessione
sparkR.stop()
