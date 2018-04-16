


#extra_attiv_skygo <- view_df(extra_attiv_skygo)


rm(list=ls())


#prova <- read.df("---.parquet","parquet")


distVals <- collect(distinct(select(prova, 'DES_INTERESSE_LIVELLO_2')))
# per cercare coppie uniche: distinct(select(df, list('col1', 'col2')))


cb_sel_dist <- SparkR::groupBy(cb_selezionata, cb_selezionata$COD_CONTRATTO, cb_selezionata$COD_ISTAT)


a <- subset(cm_anagr_cb, cm_anagr_cb$FLG_CODICE_FISCALE_VALIDO >= "2")


#lapply(ls_cm, print_str, './str_df/CMDM.csv')
#explore_df('/STAGE/SkyExtra/AttivazioniSKYGO/full/AttivazioniSKYGO.parquet',TRUE, 'MARKET_CLASS_CD')


#ORDINA IL DF IN MANIERA CRESCENTE A SECONDA DEI PARAMTERI CHE METTI:
new <- arrange(cm_bus_distval, cm_bus_distval$DES_STATO_BUSINESS_DA, cm_bus_distval$DES_STATO_BUSINESS_A)
#collect(new)




## JOIN ##
c <- join(a, b, a$CODICE_CLIENTE == b$COD_CLIENTE)

count(c)

dim(c)


## Raggruppare e applicare una funzione ##
summarize(groupBy(df, df$col1), new_col2 = mean(df$col3)) # RAGGRUPPA LA COLONNA e applica la funzione "mean" ai gruppi creati con la groupBy



## TRASFORMAZIONE DA STRINGA A DATA:
str_to_date <- function(df, colname, format) {
  newdf <- withColumn(df, colname, cast(unix_timestamp(df[[colname]], format), 'timestamp')) 
}

palinsesto <- str_to_date(palinsesto,'DAT_FINE_EVENTO',"MM/dd/yyyy HH:mm:ss")

## OPPURE ##
list_cb_tech_dmp_DAT <- withColumn(list_cb_tech_dmp,"DAT_RILEVAZIONE", cast(cast(unix_timestamp(list_cb_tech_dmp$DAT_RILEVAZIONE, 'dd/MM/yyyy'), 'timestamp'), 'date'))

skyappwsc_2 <- withColumn(skyappwsc_1, "date_time_dt", cast(skyappwsc_1$date_time, "date"))

## oppure per mettere anche gli orari (formato timestamp):
vod_viewing_DAT <- withColumn(vod_viewing_DAT,"DAT_ACTION",cast(unix_timestamp(vod_viewing_DAT$DAT_ACTION, 'MM/dd/yyyy HH:mm:ss'), 'timestamp'))



# ## LETTURA DATI FORMATO di salvataggio SAS ##
# read.sas7bdat(file, debug=FALSE)  #.sas7bdat
# read_sas("file.sas7bdat") 
# 
# sasdata <- read.xport("C:/temp/sasfile.xpt") #file libreria sas


# per aggiungere un parametro a tutti i records
withColumn(df, "col", lit("BLABLA"))



## PER SCRIVERE o esportare un CSV ##
write.df(repartition( df, 1), path = "/user/stefano.mazzucca/CJ_PDISC_X_EXPORT_DOWN.csv", "csv", sep=";", mode = "overwrite", header=TRUE)

## PER LEGGERE un CSV ##
pdisc_path <- "/user/stefano.mazzucca/Ret_DIRECT_SCADENZE_GENNAIO_MKTGAUTO.txt"
pdisc_ma_1 <- read.df(pdisc_path, source = "csv", header = "true", delimiter = "\t")



## FILTRI sul df ##
filtro_pag_pack_off <- filter(skyitdev_df_6,"page_url_post_evar like '%/pacchetti-offerte/%' ")



#### Impostazioni per analisi delle VISITE sulle pagine on line:

filter_sito_pub <- withColumn(filter_sito_pub,"concatena_chiave", concat(filter_sito_pub$post_visid_concatenated, filter_sito_pub$visit_num))
# skyitdev_for_visit <- withColumn(skyitdev,"visit_uniq",concat("post_visid_low","post_visid_high", "visit_num"))

visits <- sql("select visit_uniq, count(*)
              from skyitdev_for_visit
              where hit_source  = 1 AND exclude_hit = 0
              group by visit_uniq")



# per RICAVRE il giorno della settimana: MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY ##
CJ_PDISC_scarico_pag_disdetta_moduli <- withColumn(CJ_PDISC_scarico_pag_disdetta_moduli, "weekday", date_format(CJ_PDISC_scarico_pag_disdetta_moduli$ts, 'EEEE'))



## CASE WHEN nelle query sql ##
filt_orari_dwnl_4 <- sql("select *,
    case when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                         and orario_dwnl >= 0 and orario_dwnl <= 8 then 'casa'
    when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                         and orario_dwnl >= 9 and orario_dwnl <= 18 then 'ufficio'
    when (weekday like '%Monday%' or weekday like '%Tuesday%' or weekday like '%Wednesday%' or weekday like '%Thursday%' or weekday like '%Friday%')
                         and orario_dwnl >= 19 and orario_dwnl <= 23 then 'casa'
    else 'casa' end as flg_dwnl_casa_ufficio
                         from filt_orari_dwnl_3")


## IFLESE ##
skyapp$sequenza = ifelse(contains(skyapp$page_name_app, c("conosci")), 'app assistenza conosci', 
                         ifelse(contains(skyapp$page_name_app, c("gestisci")), 'app assistenza gestisci', 
                                ifelse(contains(skyapp$page_name_app, c("assistenza:home")), 'app assistenza home',
                                       ifelse(contains(skyapp$page_name_app, c("contatta")), 'app assistenza contatta',
                                              ifelse(contains(skyapp$page_name_app, c("risolvi")), 'app assistenza risolvi',
                                                     ifelse(contains(skyapp$page_name_app, c("ricerca")), 'app assistenza ricerca',skyapp$sequenza))))))



## ANTI JOIN (dove t1 e' la tabella completa) ##
pdisc_no_agganciati <- sql("select t1.COD_CONTRATTO
                            from base_pdisc t1
                            left join pdisc_ma_tot_2 t2
                              on t1.COD_CONTRATTO = t2.COD_CONTRATTO where t2.COD_CONTRATTO is NULL")
View(head(pdisc_no_agganciati,100))


## COUNT numero di caratteri in una stringa
ver <-sql("select *
           from valdist_utenti_appwsc
           where length(external_id_post_evar) == 48")
nrow(ver)


## PER SOTTRARRE GIORNI da una data
sky_wsc_1 <- withColumn(sky_wsc, "dat_pdisc_meno_31gg", date_sub(sky_wsc$data_ingr_pdisc_formdate,31))



