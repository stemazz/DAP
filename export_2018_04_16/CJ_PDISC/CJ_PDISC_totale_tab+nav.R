
# CJ_PDISC_totale
# tabellone visualizzazioni e seuqenze navigazione

#apri sessione
source("connection_R.R")
options(scipen = 1000)



##############################################################################################################################################################################
## Nuovi scarichi pagine visitate di properties Sky #########################################################################################################################
##############################################################################################################################################################################


CJ_PDISC_pag_sito <- read.parquet("/user/valentina/CJ_PDISC_scarico_pag_sito.parquet")
View(head(CJ_PDISC_pag_sito,100))
nrow(CJ_PDISC_pag_sito)
# 4.492.336

CJ_PDISC_app <- read.parquet("/user/valentina/CJ_PDISC_scarico_app.parquet")
View(head(CJ_PDISC_app,100))
nrow(CJ_PDISC_app)
# 918.833


## Analisi sui clienti che navigano sulle pagine Sky ###########################################################################################################################

valdist_pag_sito <- distinct(select(CJ_PDISC_pag_sito, "COD_CLIENTE_CIFRATO"))
nrow(valdist_pag_sito)
# 22.045

valdist_app <- distinct(select(CJ_PDISC_app, "COD_CLIENTE_CIFRATO"))
nrow(valdist_app)
# 10.966 clienti che naviagno su APP

sky_tot <- union(valdist_pag_sito, valdist_app)

valdist_sky_tot <- distinct(select(sky_tot, "COD_CLIENTE_CIFRATO"))
nrow(valdist_sky_tot)
# 23.094 cleinti distinti che navigano tra MARZO e DICEMBRE 2017 tra SITO SKY (Skyitdev) e APP 




sky_sito_pub <- filter(CJ_PDISC_pag_sito,"terzo_livello_post_evar NOT LIKE '%faidate%' ")

valdist_clienti_pag_pub <- distinct(select(sky_sito_pub, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pag_pub)
# 21.638 clienti che naviagno su sito pubblico Sky


sky_wsc <- filter(CJ_PDISC_pag_sito,"terzo_livello_post_evar LIKE '%faidate%' ")

valdist_clienti_wsc <- distinct(select(sky_wsc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_wsc)
# 17.982 clienti che naviagno su sito WSC




##############################################################################################################################################################################
## Analisi ultimo mese #######################################################################################################################################################
##############################################################################################################################################################################


sky_sito_pub_1 <- withColumn(sky_sito_pub, "dat_pdisc_meno_31gg", date_sub(sky_sito_pub$data_ingr_pdisc_formdate,31))

sky_sito_pub_1$flg_31gg <- ifelse(sky_sito_pub_1$date_time_dt >= sky_sito_pub_1$dat_pdisc_meno_31gg,1,0)
View(head(sky_sito_pub_1,1000))

sky_sito_pub_flg1 <- filter(sky_sito_pub_1, "flg_31gg == 1")
valdist_flg1_pag_pub <- distinct(select(sky_sito_pub_flg1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_flg1_pag_pub)
# 16.542 clienti che navigano su sito pubblico Sky nell'ultimo mese prima del PDISC




sky_wsc_1 <- withColumn(sky_wsc, "dat_pdisc_meno_31gg", date_sub(sky_wsc$data_ingr_pdisc_formdate,31))

sky_wsc_1$flg_31gg <- ifelse(sky_wsc_1$date_time_dt >= sky_wsc_1$dat_pdisc_meno_31gg,1,0)
View(head(sky_wsc_1,1000))

sky_wsc_flg1 <- filter(sky_wsc_1, "flg_31gg == 1")
valdist_flg1_wsc <- distinct(select(sky_wsc_flg1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_flg1_wsc)
# 11.502 clienti che navigano su sito WSC Sky nell'ultimo mese prima del PDISC





sky_app_1 <- withColumn(CJ_PDISC_app, "dat_pdisc_meno_31gg", date_sub(CJ_PDISC_app$data_ingr_pdisc_formdate,31))

sky_app_1$flg_31gg <- ifelse(sky_app_1$date_time_dt >= sky_app_1$dat_pdisc_meno_31gg,1,0)
View(head(sky_app_1,1000))

sky_app_flg1 <- filter(sky_app_1, "flg_31gg == 1")
valdist_flg1_app <- distinct(select(sky_app_flg1, "COD_CLIENTE_CIFRATO"))
nrow(valdist_flg1_app)
# 6.889 clienti che navigano su APP Sky nell'ultimo mese prima del PDISC




createOrReplaceTempView(valdist_flg1_wsc, "valdist_flg1_wsc")
createOrReplaceTempView(valdist_flg1_app, "valdist_flg1_app")

ver <- sql("select distinct t1.COD_CLIENTE_CIFRATO
           from valdist_flg1_wsc t1
           inner join valdist_flg1_app t2
            on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
nrow(ver)
# 3.153

# 6889-3153
# 3736 + 11502
# 15.238 ultimo mese (wsc + app)




navigatori_ultimo_mese <- rbind(valdist_flg1_pag_pub, valdist_flg1_wsc, valdist_flg1_app)

valdist_navigatori_ultimo_mese <- distinct(select(navigatori_ultimo_mese, "COD_CLIENTE_CIFRATO"))
nrow(valdist_navigatori_ultimo_mese)
# 18.570 clienti totali che navigano nell'ultimo mese prima del PDISC





##############################################################################################################################################################################
## Analisi pagina DISDETTA ####################################################################################################################################################
##############################################################################################################################################################################


scarico_pag_disdetta <- filter(CJ_PDISC_pag_sito, "page_url_post_evar like '%/assistenza/info-disdetta%' or  
                               page_url_post_prop LIKE '%moduli%' or 
                               download_name_post_prop LIKE '%modulo_disdetta%'")
View(head(scarico_pag_disdetta,100))

valdist_clienti_pag_disdetta <- distinct(select(scarico_pag_disdetta, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_pag_disdetta)
# 11.311

createOrReplaceTempView(scarico_pag_disdetta,"scarico_pag_disdetta")
count_distr_pag_disdetta <- sql("select COD_CLIENTE_CIFRATO, 
                                    min(date_time_dt) as min_data,
                                    max(date_time_dt) as max_data,
                                    count(distinct(visit_num)) as count_visit,
                                    count(download_name_post_prop) as dwnl,
                                    count(distinct(download_name_post_prop)) as valdist_dwnl
                                 from scarico_pag_disdetta
                                 group by COD_CLIENTE_CIFRATO")
View(head(count_distr_pag_disdetta,100))

createOrReplaceTempView(count_distr_pag_disdetta, "count_distr_pag_disdetta")
count_tot_pag_disdetta <- sql("select sum(count_visit) as visite_totali_pag_disdetta,
                                      sum(dwnl) as tot_dwnl
                              from count_distr_pag_disdetta")
View(head(count_tot_pag_disdetta,100))
# 14.944 download totali di modulo disdetta

clienti_filter_dwnl <- filter(scarico_pag_disdetta, "download_name_post_prop is NOT NULL")
valdist_clienti_filter_dwnl <- distinct(select(clienti_filter_dwnl, "COD_CLIENTE_CIFRATO"))
nrow(valdist_clienti_filter_dwnl)
# 6.799 clienti che scaricano il download del modulo disdetta






##############################################################################################################################################################################
##############################################################################################################################################################################
## Creazione sequenze navigazione ############################################################################################################################################
##############################################################################################################################################################################
##############################################################################################################################################################################

filter_1 <- filter(sky_sito_pub_1, "page_url_post_evar like '%www.sky.it%'")
filter_sito_pub <- filter(filter_1,
                          "secondo_livello_post_evar like '%homepage-sky%' or 
                           secondo_livello_post_evar like '%homepage sky%' or 
                           secondo_livello_post_evar like '%guidatv%' or
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
nrow(filter_sito_pub)
# 835.140 (vs 1.112.542 prima delle modifiche al page_url)

filter_wsc <- filter(sky_wsc_1,"
                    terzo_livello_post_evar   like '%faidate home%' or
                    terzo_livello_post_evar   like '%faidate gestisci dati servizi%' or
                    terzo_livello_post_evar   like '%faidate arricchisci abbonamento%' or
                    terzo_livello_post_evar   like '%faidate fatture pagamenti%' or
                    terzo_livello_post_evar   like '%faidate webtracking%' or
                    terzo_livello_post_evar   like '%faidate extra%' ") 
nrow(filter_wsc)
# 671.001

filter_app <- filter(sky_app_1,"
                      site_section like '%homepage%' or
                      site_section like '%home%' or
                      site_section like '%intro%' or
                      site_section like '%login%' or
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
                      page_name_post_evar = 'arricchisci abbonamento:home' ")
nrow(filter_app)
# 656.842


#### Pulizia sequenze di navigazione

## Sito pubblico

filter_sito_pub <- withColumn(filter_sito_pub,"concatena_chiave",
                              concat(filter_sito_pub$post_visid_concatenated, filter_sito_pub$visit_num))

createOrReplaceTempView(filter_sito_pub, "filter_sito_pub")
skypubblico <- sql("select COD_CLIENTE_CIFRATO, 
                            post_visid_concatenated, 
                            visit_num, 
                            concatena_chiave, 
                            date_time_dt, 
                            date_time_ts,
                            data_ingr_pdisc_formdate, 
                            hit_source, 
                            exclude_hit,
                            secondo_livello_post_evar, 
                            terzo_livello_post_evar,
                            download_name_post_prop,
                            page_url_post_evar
                    from filter_sito_pub")

skypubblico$sequenza <- ifelse(skypubblico$secondo_livello_post_evar == 'assistenza', 
                               concat_ws(sep="_", skypubblico$secondo_livello_post_evar, skypubblico$terzo_livello_post_evar), skypubblico$secondo_livello_post_evar)

#View(head(filter(skypubblico, "secondo_livello_post_evar == 'assistenza'"), 1000))
View(head(skypubblico,100))
nrow(skypubblico)
# 835.140 (vs 1.112.542 di filter_sito_pub di prima delle modifiche)

createOrReplaceTempView(skypubblico,"skypubblico")

skypubblico <- sql("SELECT COD_CLIENTE_CIFRATO, 
                            post_visid_concatenated, 
                            visit_num, 
                            concatena_chiave, 
                            date_time_dt, 
                            date_time_ts, 
                            data_ingr_pdisc_formdate, 
                            hit_source, 
                            exclude_hit, 
                            sequenza,
                            download_name_post_prop,
                            page_url_post_evar
                   FROM skypubblico
                   ORDER BY COD_CLIENTE_CIFRATO")

skypubblico = withColumn(skypubblico, "canale", lit("sito_pubblico"))

View(head(skypubblico, 100))


## AGGIUNTA per "disdetta" su URL in "assistenza_conosci" #####################################################################################################################

skypubblico$sequenza2 <- ifelse(contains(skypubblico$page_url_post_evar, 
                              c('assistenza/conosci/informazioni-sull-abbonamento/termini-e-condizioni/come-posso-dare-la-disdetta-del-mio-contratto.html')), 
                              'assistenza_conosci_disdetta', skypubblico$sequenza)
# a <- filter(skypubblico, "sequenza2 LIKE '%assistenza_conosci_disdetta%'")
# View(head(a,1000))
# nrow(a)
# # 8.063
# View(head(skypubblico,1000))
# nrow(skypubblico)
# # 835.140 (ok!)
skypubblico_1 <- select(skypubblico, "COD_CLIENTE_CIFRATO", 
                            "post_visid_concatenated", 
                            "visit_num", 
                            "concatena_chiave", 
                            "date_time_dt", 
                            "date_time_ts", 
                            "data_ingr_pdisc_formdate", 
                            "hit_source", 
                            "exclude_hit", 
                            "sequenza2",
                            "download_name_post_prop", 
                            "canale")
skypubblico_2 <- withColumnRenamed(skypubblico_1, "sequenza2", "sequenza")
View(head(skypubblico_2,1000))
nrow(skypubblico_2)
# 835.140

###############################################################################################################################################################################


## Sito WSC

filter_wsc <- withColumn(filter_wsc,"concatena_chiave",
                              concat(filter_wsc$post_visid_concatenated, filter_wsc$visit_num))

createOrReplaceTempView(filter_wsc, "filter_wsc")
skywsc <- sql("select COD_CLIENTE_CIFRATO, 
                            post_visid_concatenated, 
                            visit_num, 
                            concatena_chiave, 
                            date_time_dt, 
                            date_time_ts, 
                            data_ingr_pdisc_formdate, 
                            hit_source, 
                            exclude_hit,
                            terzo_livello_post_evar,
                            download_name_post_prop
                    from filter_wsc")

skywsc$sequenza <- skywsc$terzo_livello_post_evar
nrow(skywsc)
# 671.001 (vs 671.001 di filter_wsc)

createOrReplaceTempView(skywsc,"skywsc")

skywsc <- sql("SELECT COD_CLIENTE_CIFRATO, 
                        post_visid_concatenated, 
                        visit_num, 
                        concatena_chiave, 
                        date_time_dt, 
                        date_time_ts, 
                        data_ingr_pdisc_formdate, 
                        hit_source, 
                        exclude_hit,
                        sequenza,
                        download_name_post_prop
              FROM skywsc
              ORDER BY COD_CLIENTE_CIFRATO")

skywsc = withColumn(skywsc, "canale", lit("sito_wsc"))



## APP

filter_app <- withColumn(filter_app,"concatena_chiave",
                         concat(filter_app$post_visid_concatenated, filter_app$visit_num))

createOrReplaceTempView(filter_app, "filter_app")
skyapp <- sql("SELECT COD_CLIENTE_CIFRATO, 
              post_visid_concatenated, 
              visit_num, 
              concatena_chiave, 
              date_time_dt, 
              date_time_ts,
              data_ingr_pdisc_formdate, 
              hit_source, 
              exclude_hit,
              site_section,
              page_name_post_evar
              FROM filter_app")

skyapp = withColumn(skyapp, "secondo_livello_post_evar", lit("app"))
skyapp$site_section_app <- concat_ws(sep=" ", skyapp$secondo_livello_post_evar, skyapp$site_section)
skyapp$page_name_app <- concat_ws(sep=" ", skyapp$secondo_livello_post_evar, skyapp$page_name_post_evar)

skyapp$sequenza <- ifelse(skyapp$site_section_app == 'app assistenza', skyapp$page_name_app, skyapp$site_section_app)
skyapp$sequenza <- ifelse(skyapp$site_section_app == 'app widget', skyapp$page_name_app, skyapp$site_section_app)

skyapp$sequenza <- ifelse(contains(skyapp$site_section_app, 'intro'), 'app home', 
                          ifelse(skyapp$site_section_app == 'app login', 'app home',
                                 ifelse(skyapp$site_section_app == 'app homepage', 'app home',
                                    skyapp$site_section_app)))

View(head(skyapp,1000))

skyapp$sequenza = ifelse(contains(skyapp$page_name_app, c("conosci")), 'app assistenza conosci', 
                         ifelse(contains(skyapp$page_name_app, c("gestisci")), 'app assistenza gestisci', 
                                ifelse(contains(skyapp$page_name_app, c("assistenza:home")), 'app assistenza home',
                                       ifelse(contains(skyapp$page_name_app, c("contatta")), 'app assistenza contatta',
                                              ifelse(contains(skyapp$page_name_app, c("risolvi")), 'app assistenza risolvi',
                                                     ifelse(contains(skyapp$page_name_app, c("ricerca")), 'app assistenza ricerca',skyapp$sequenza))))))

skyapp$sequenza = ifelse(contains(skyapp$page_name_app, c("dispositivi")), 'app widget dispositivi', 
                         ifelse(contains(skyapp$page_name_app, c("ultimafattura")), 'app widget ultimafattura', 
                                ifelse(contains(skyapp$page_name_app, c("contatta sky")), 'app widget contatta sky',
                                       ifelse(contains(skyapp$page_name_app, c("widget:gestisci")), 'app widget gestisci', skyapp$sequenza))))

View(head(skyapp,100))
nrow(skyapp)
# 656.842 (vs 656.842 di filter_app) 

createOrReplaceTempView(skyapp,"skyapp")
skyapp <- sql("SELECT COD_CLIENTE_CIFRATO, 
                        post_visid_concatenated, 
                        visit_num, 
                        concatena_chiave, 
                        date_time_dt, 
                        date_time_ts,
                        data_ingr_pdisc_formdate, 
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



## Unisco le 3 basi

sky_pubbl_wsc_app <- rbind(skywsc, skypubblico_2, skyapp_1)

createOrReplaceTempView(sky_pubbl_wsc_app, "sky_pubbl_wsc_app")
sky_pubbl_wsc_app$sequenza <- regexp_replace(sky_pubbl_wsc_app$sequenza, " ", "_")

createOrReplaceTempView(sky_pubbl_wsc_app,"sky_pubbl_wsc_app")
sky_pubbl_wsc_app_1 <- sql("select *
                           from sky_pubbl_wsc_app
                           order by COD_CLIENTE_CIFRATO, date_time_ts, concatena_chiave")

View(head(sky_pubbl_wsc_app_1, 1000))
str(sky_pubbl_wsc_app_1)

nrow(skypubblico_2) + nrow(skyapp_1) + nrow(skywsc)
# 2.162.983 (vs 1.980.013 di prima delle modifiche)
nrow(sky_pubbl_wsc_app_1)
# 2.162.983 (vs 1.980.013 di prima delle modifiche)


write.parquet(sky_pubbl_wsc_app_1,"/user/stefano.mazzucca/CJ_PDISC_sequenze_nav_20180406.parquet")





##############################################################################################################################################################################
## Analisi pagine ASSISTENZA e FATTURE #######################################################################################################################################
##############################################################################################################################################################################


CJ_PDISC_visite_seq <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav.parquet")
View(head(CJ_PDISC_visite_seq,100))
nrow(CJ_PDISC_visite_seq)
# 2.162.983


CJ_PDISC_visite_seq_1 <- withColumn(CJ_PDISC_visite_seq, "one_month_before_pdisc", date_sub(CJ_PDISC_visite_seq$data_ingr_pdisc_formdate, 31))

CJ_PDISC_visite_seq_2 <- filter(CJ_PDISC_visite_seq_1, "sequenza LIKE '%assistenza%' or sequenza LIKE '%fatture%' or sequenza LIKE '%fattura%'")
View(head(CJ_PDISC_visite_seq_2,1000))
nrow(CJ_PDISC_visite_seq_2)
# 510.113 

# prova <- distinct(select(CJ_PDISC_visite_seq_2, "sequenza"))
# View(head(prova,100))

CJ_PDISC_visite_seq_2$flg_31gg <- ifelse(CJ_PDISC_visite_seq_2$date_time_dt <= CJ_PDISC_visite_seq_2$one_month_before_pdisc, 0, 1)
View(head(CJ_PDISC_visite_seq_2,1000))

CJ_PDISC_visite_seq_2 <- withColumn(CJ_PDISC_visite_seq_2, "start", lit("2017-03-01"))
CJ_PDISC_visite_seq_3 <- withColumn(CJ_PDISC_visite_seq_2, "start", cast(CJ_PDISC_visite_seq_2$start, 'date'))

CJ_PDISC_visite_seq_4 <- withColumn(CJ_PDISC_visite_seq_3, "n_mesi", datediff(CJ_PDISC_visite_seq_3$one_month_before_pdisc, CJ_PDISC_visite_seq_3$start)/30)
View(head(CJ_PDISC_visite_seq_4,100))
nrow(CJ_PDISC_visite_seq_4)
# 510.113

createOrReplaceTempView(CJ_PDISC_visite_seq_4,"CJ_PDISC_visite_seq_4")


### Visite alla pagina ASSISTENZA 

CJ_PDISC_visite_seq_assistenza <- sql("select COD_CLIENTE_CIFRATO, flg_31gg, n_mesi, 
                                      count(*) as tot_viz_pag_assistenza
                                      from CJ_PDISC_visite_seq_4
                                      where sequenza LIKE '%assistenza%' and sequenza NOT LIKE '%assistenza_fattura_elettronica%'
                                      group by COD_CLIENTE_CIFRATO, flg_31gg, n_mesi")
View(head(CJ_PDISC_visite_seq_assistenza,100))

CJ_PDISC_visite_seq_assistenza$count_visite_pag_assistenza_ok <- ifelse(CJ_PDISC_visite_seq_assistenza$flg_31gg == 0, 
                                                                        CJ_PDISC_visite_seq_assistenza$tot_viz_pag_assistenza/CJ_PDISC_visite_seq_assistenza$n_mesi, 
                                                                        CJ_PDISC_visite_seq_assistenza$tot_viz_pag_assistenza)

CJ_PDISC_visite_seq_assistenza_1 <- arrange(CJ_PDISC_visite_seq_assistenza, CJ_PDISC_visite_seq_assistenza$COD_CLIENTE_CIFRATO)
View(head(CJ_PDISC_visite_seq_assistenza_1,100))

###############
#prova <- reshape(CJ_PDISC_visite_seq_assistenza_1, idvar = "COD_CLIENTE_CIFRATO", timevar = "flg_31gg", direction = "wide", sep = "_")
#############

CJ_PDISC_visite_seq_assistenza_flg1 <- filter(CJ_PDISC_visite_seq_assistenza_1, "flg_31gg == 1")
CJ_PDISC_visite_seq_assistenza_flg0 <- filter(CJ_PDISC_visite_seq_assistenza_1, "flg_31gg == 0")


tot_clienti <- distinct(union(distinct(select(CJ_PDISC_visite_seq_assistenza_flg1, "COD_CLIENTE_CIFRATO")), 
                              distinct(select(CJ_PDISC_visite_seq_assistenza_flg0, "COD_CLIENTE_CIFRATO"))))
nrow(tot_clienti)
# 20.396 <<<<-----------------------------------------------------------------------------------------------------------------------------------------------


createOrReplaceTempView(CJ_PDISC_visite_seq_assistenza_flg1,"CJ_PDISC_visite_seq_assistenza_flg1")
createOrReplaceTempView(CJ_PDISC_visite_seq_assistenza_flg0,"CJ_PDISC_visite_seq_assistenza_flg0")
createOrReplaceTempView(tot_clienti,"tot_clienti")

CJ_PDISC_visite_seq_assistenza_2 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, 
                                        t1.count_visite_pag_assistenza_ok as viz_post_1_mese
                                        from tot_clienti t0
                                        left join CJ_PDISC_visite_seq_assistenza_flg1 t1
                                        on t0.COD_CLIENTE_CIFRATO = t1.COD_CLIENTE_CIFRATO
                                        ")
nrow(CJ_PDISC_visite_seq_assistenza_2) # 20.400
createOrReplaceTempView(CJ_PDISC_visite_seq_assistenza_2, "CJ_PDISC_visite_seq_assistenza_2")
CJ_PDISC_visite_seq_assistenza_3 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, t0.viz_post_1_mese,
                                        t2.count_visite_pag_assistenza_ok as viz_pre_1_mese
                                        from CJ_PDISC_visite_seq_assistenza_2 t0
                                        left join CJ_PDISC_visite_seq_assistenza_flg0 t2
                                        on t0.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                                        ")
View(head(CJ_PDISC_visite_seq_assistenza_3,1000))
nrow(CJ_PDISC_visite_seq_assistenza_3)
# 20.428 (vs 20.396 clienti totali) 
ver <- filter(CJ_PDISC_visite_seq_assistenza_3, "COD_CLIENTE_CIFRATO is NULL")
nrow(ver) # 0
ver_2 <- filter(CJ_PDISC_visite_seq_assistenza_3, "viz_post_1_mese is NULL and viz_pre_1_mese is NULL")
nrow(ver_2) # 0


write.df(repartition( CJ_PDISC_visite_seq_assistenza_3, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_viz_assistenza.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



####################################
### Visite alla pagina delle FATTURE  

CJ_PDISC_visite_seq_fatture <- sql("select COD_CLIENTE_CIFRATO, flg_31gg, n_mesi, 
                                   count(*) as tot_viz_fatture
                                   from CJ_PDISC_visite_seq_4
                                   where sequenza LIKE '%fatture%' or sequenza LIKE '%fattura%'
                                   group by COD_CLIENTE_CIFRATO, flg_31gg, n_mesi")
View(head(CJ_PDISC_visite_seq_fatture,100))

CJ_PDISC_visite_seq_fatture$count_visite_fatture_ok <- ifelse(CJ_PDISC_visite_seq_fatture$flg_31gg == 0, 
                                                              CJ_PDISC_visite_seq_fatture$tot_viz_fatture/CJ_PDISC_visite_seq_fatture$n_mesi, 
                                                              CJ_PDISC_visite_seq_fatture$tot_viz_fatture)

CJ_PDISC_visite_seq_fatture_1 <- arrange(CJ_PDISC_visite_seq_fatture, CJ_PDISC_visite_seq_fatture$COD_CLIENTE_CIFRATO)
View(head(CJ_PDISC_visite_seq_fatture_1,100))

CJ_PDISC_visite_seq_fatture_flg1 <- filter(CJ_PDISC_visite_seq_fatture_1, "flg_31gg == 1")
CJ_PDISC_visite_seq_fatture_flg0 <- filter(CJ_PDISC_visite_seq_fatture_1, "flg_31gg == 0")


tot_clienti <- distinct(union(distinct(select(CJ_PDISC_visite_seq_fatture_flg1, "COD_CLIENTE_CIFRATO")), 
                              distinct(select(CJ_PDISC_visite_seq_fatture_flg0, "COD_CLIENTE_CIFRATO"))))
nrow(tot_clienti)
# 13.003 <<<<-----------------------------------------------------------------------------------------------------------------------------------------------


createOrReplaceTempView(CJ_PDISC_visite_seq_fatture_flg1,"CJ_PDISC_visite_seq_fatture_flg1")
createOrReplaceTempView(CJ_PDISC_visite_seq_fatture_flg0,"CJ_PDISC_visite_seq_fatture_flg0")
createOrReplaceTempView(tot_clienti,"tot_clienti")

CJ_PDISC_visite_seq_fatture_2 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, 
                                     count_visite_fatture_ok as viz_post_1_mese
                                     from tot_clienti t0
                                     left join CJ_PDISC_visite_seq_fatture_flg1 t1
                                     on t0.COD_CLIENTE_CIFRATO = t1.COD_CLIENTE_CIFRATO
                                     ")
nrow(CJ_PDISC_visite_seq_fatture_2) # 13.004
createOrReplaceTempView(CJ_PDISC_visite_seq_fatture_2, "CJ_PDISC_visite_seq_fatture_2")
CJ_PDISC_visite_seq_fatture_3 <- sql("select distinct t0.COD_CLIENTE_CIFRATO, t0.viz_post_1_mese,
                                     t2.count_visite_fatture_ok as viz_pre_1_mese
                                     from CJ_PDISC_visite_seq_fatture_2 t0
                                     left join CJ_PDISC_visite_seq_fatture_flg0 t2
                                     on t0.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                                     ")
View(head(CJ_PDISC_visite_seq_fatture_3,100))
nrow(CJ_PDISC_visite_seq_fatture_3)
# 13.018 (vs 13.003 clienti totali) 
ver <- filter(CJ_PDISC_visite_seq_fatture_3, "COD_CLIENTE_CIFRATO is NULL")
nrow(ver) # 0
ver_2 <- filter(CJ_PDISC_visite_seq_fatture_3, "viz_post_1_mese is NULL and viz_pre_1_mese is NULL")
nrow(ver_2) # 0


write.df(repartition( CJ_PDISC_visite_seq_fatture_3, 1),path = "/user/stefano.mazzucca/CJ_PDISC_count_viz_fatture.csv", "csv", sep=";", mode = "overwrite", header=TRUE)



## Analisi   #################################################################################################################################################################

# utenti attivi nella navigazione nei 31gg precedenti al PDISC ###############################################################################################################

# CJ_PDISC_visite_seq <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_sequenze_nav.parquet")
# CJ_PDISC_visite_seq_1 <- withColumn(CJ_PDISC_visite_seq, "one_month_before_pdisc", date_sub(CJ_PDISC_visite_seq$data_ingr_pdisc_formdate, 31))

base_pdisc <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
nrow(base_pdisc)
# 37.976

valdist_base <- distinct(select(base_pdisc, "COD_CLIENTE_CIFRATO"))
nrow(valdist_base)
# 37.753

base_pdisc_coo <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_chiavi_cookies_base_pdisc.parquet")
nrow(base_pdisc_coo)
# 161.766
ver_clienti <- distinct(select(base_pdisc_coo, "COD_CLIENTE_CIFRATO"))
nrow(ver_clienti)
# 24.340 clienti pdisc on line (su device)

createOrReplaceTempView(base_pdisc, "base_pdisc")
createOrReplaceTempView(base_pdisc_coo, "base_pdisc_coo")

base_pdisc_coo_2 <- sql("select t1.*, t2.post_visid_concatenated
                        from base_pdisc t1
                        left join base_pdisc_coo t2
                        on t1.COD_CLIENTE_CIFRATO = t2.external_id_post_evar")
nrow(base_pdisc_coo_2)
# 176.511

createOrReplaceTempView(CJ_PDISC_visite_seq_1, "CJ_PDISC_visite_seq_1")
createOrReplaceTempView(base_pdisc_coo_2, "base_pdisc_coo_2")

utenti_attivi_31gg <- sql("select distinct t1.COD_CLIENTE_CIFRATO
                          from base_pdisc_coo_2 t1
                          inner join CJ_PDISC_visite_seq_1 t2
                          on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t2.date_time_dt >= t2.one_month_before_pdisc 
                          and t2.date_time_dt <= t2.data_ingr_pdisc_formdate ")
View(head(utenti_attivi_31gg,100))
nrow(utenti_attivi_31gg)
# 16.183 utenti attivi nei 31gg precedenti al PDISC ##########################################################################################################################
# (vs 18.570 clienti che navigano sui siti Sky dall'ultimo mese prima del PDISC fino al 1 Gennaio. ATTENZIONE!)

createOrReplaceTempView(CJ_PDISC_visite_seq_1, "CJ_PDISC_visite_seq_1")
createOrReplaceTempView(base_pdisc_coo_2, "base_pdisc_coo_2")

utenti_attivi_sito_pub_31gg <- sql("select distinct t1.COD_CLIENTE_CIFRATO
                                    from base_pdisc_coo_2 t1
                                    inner join CJ_PDISC_visite_seq_1 t2
                                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t2.date_time_dt >= t2.one_month_before_pdisc 
                          and t2.date_time_dt <= t2.data_ingr_pdisc_formdate and t2.canale LIKE '%sito_pubblico%'")
View(head(utenti_attivi_sito_pub_31gg,100))
nrow(utenti_attivi_sito_pub_31gg)
# 13.625 utenti attivi negli ultimi 31gg che navigano sul sito pubblico CON I FILTRI SULLE PAGINE###############################################################################
# (vs 16.542 clienti che navigano su sito pubblico Sky dall'ultimo mese prima del PDISC fino al 1 Gennaio. ATTENZIONE!)

createOrReplaceTempView(CJ_PDISC_visite_seq_1, "CJ_PDISC_visite_seq_1")
createOrReplaceTempView(base_pdisc_coo_2, "base_pdisc_coo_2")

utenti_attivi_sito_wsc_31gg <- sql("select distinct t1.COD_CLIENTE_CIFRATO
                                    from base_pdisc_coo_2 t1
                                    inner join CJ_PDISC_visite_seq_1 t2
                                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t2.date_time_dt >= t2.one_month_before_pdisc 
                          and t2.date_time_dt <= t2.data_ingr_pdisc_formdate and t2.canale LIKE '%sito_wsc%'")
View(head(utenti_attivi_sito_wsc_31gg,100))
nrow(utenti_attivi_sito_wsc_31gg)
# 9.965 utenti attivi negli ultimi 31gg che navigano su WSC CON I FILTRI SULLE PAGINE ##########################################################################################
# (vs 11.502 clienti che navigano su sito WSC Sky dall'ultimo mese prima del PDISC fino al 1 Gennaio. ATTENZIONE!)

createOrReplaceTempView(CJ_PDISC_visite_seq_1, "CJ_PDISC_visite_seq_1")
createOrReplaceTempView(base_pdisc_coo_2, "base_pdisc_coo_2")

utenti_attivi_app_31gg <- sql("select distinct t1.COD_CLIENTE_CIFRATO
                                    from base_pdisc_coo_2 t1
                                    inner join CJ_PDISC_visite_seq_1 t2
                                      on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t2.date_time_dt >= t2.one_month_before_pdisc 
                          and t2.date_time_dt <= t2.data_ingr_pdisc_formdate and t2.canale LIKE '%app%'")
View(head(utenti_attivi_app_31gg,100))
nrow(utenti_attivi_app_31gg)
# 5.788 utenti attivi negli ultimi 31gg che navigano su APP CON I FILTRI SULLE PAGINE ##########################################################################################
# (vs 6.889 clienti che navigano su APP Sky dall'ultimo mese prima del PDISC fino al 1 Gennaio. ATTENZIONE!)


union_clienti_navigatori_31gg <- rbind(utenti_attivi_sito_pub_31gg, utenti_attivi_sito_wsc_31gg, utenti_attivi_app_31gg)
union_clienti_navigatori_31gg_2 <- distinct(select(union_clienti_navigatori_31gg, "COD_CLIENTE_CIFRATO"))
nrow(union_clienti_navigatori_31gg_2)
# 16.183 cleinti navigatori nell'ultimo mese prima del PDISC!!



#################################################################################################################################################################################
#################################################################################################################################################################################
#################################################################################################################################################################################
#################################################################################################################################################################################

createOrReplaceTempView(valdist_navigatori_ultimo_mese, "valdist_navigatori_ultimo_mese")
createOrReplaceTempView(union_clienti_navigatori_31gg_2, "union_clienti_navigatori_31gg_2")

clienti_estratti_post_pdisc <- sql("select t1.COD_CLIENTE_CIFRATO,
                                        case when t2.COD_CLIENTE_CIFRATO is NULL then 1
                                        else 0 end as flg_post_pdisc
                                   from valdist_navigatori_ultimo_mese t1
                                   left join union_clienti_navigatori_31gg_2 t2 
                                    on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO")
nrow(clienti_estratti_post_pdisc)
# 18570
View(head(clienti_estratti_post_pdisc, 100))

filt_1 <- filter(clienti_estratti_post_pdisc, "flg_post_pdisc = 1")
nrow(filt_1)
# 2387
View(filt_1)
filt_2 <- select(filt_1, "COD_CLIENTE_CIFRATO")
nrow(filt_2)
# 2387

write.df(repartition( filt_2, 1),path = "/user/stefano.mazzucca/export_clienti_post_pdisc", "csv", sep=";", mode = "overwrite", header=TRUE)



#################################################################################################################################################################################
#################################################################################################################################################################################
#################################################################################################################################################################################
#################################################################################################################################################################################




#chiudi sessione
sparkR.stop()
