
# Ricerca su adform del rate di aprtura delle dem

#apri sessione
source("connection_R.R")
options(scipen = 1000)


adform_tp_1 <- read.parquet('hdfs:///STAGE/adform/table=Trackingpoint/Trackingpoint.parquet')
View(head(adform_tp_1,1000))

createOrReplaceTempView(adform_tp_1, "adform_tp_1")

adform_tp_2 <- sql( "SELECT regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                                  CookieID, 
                                  `device-name`,
                                  `os-name`, 
                                  `browser-name`,
                                  TrackingPointId,
                                  CampaignId,
                                  `campaign-name`,
                                  page,
                                  Timestamp,
                                  ReferrerType,
                                  CookiesEnabled,
                                  VisitNumber,
                                  VisitPageNumber,
                                  PreviuosPageURL,
                                  PageURL
                    from adform_tp_1
                    where ((customvars.systemvariables like '%13%' and CookieID <> '0' ) and
                                        (page LIKE '%MAIL PREVENTION%' or
                                        page LIKE '%MAIL UPSELLING%' or
                                        page LIKE '%MAIL DOWNGRADE%' or
                                        page LIKE '%MAIL EXTRA%' or
                                        page LIKE '%MAIL SERVIZI FATTURA%' or
                                        page LIKE '%MAIL_COMUNICAZIONI_AUTO%' or
                                        page LIKE '%MAIL DISCONNESSI%' or
                                        page LIKE '%MAIL PDISC SONDAGGIO%' or
                                        page LIKE '%MAIL PDISC AMMINISTRATIVA%' or
                                        page LIKE '%MAIL PDISC COMMERCIALE%'))
                    having (skyid <> '' and length(skyid)= 48)
                    order by skyid")
#View(head(adform_tp_2,1000))
#nrow(adform_tp_2)
write.parquet(adform_tp_2,"/user/stefano.mazzucca/scarico_dem_view.parquet")


dem_adform <- read.parquet("/user/stefano.mazzucca/scarico_dem_view.parquet")
View(head(dem_adform,1000))
nrow(dem_adform)
# 16.024.976


createOrReplaceTempView(dem_adform, "dem_adform")

dem_adform_2 <- sql("select *,
                          case when page LIKE '%MAIL PREVENTION%' then 'prevention'
                              when page LIKE '%MAIL UPSELLING%' then 'upselling' 
                              when page LIKE '%MAIL DOWNGRADE%' then 'downgrade' 
                              when page LIKE '%MAIL EXTRA%' then 'extra' 
                              when page LIKE '%MAIL SERVIZI FATTURA%' then 'fattura'
                              when page LIKE '%MAIL_COMUNICAZIONI_AUTO%' then 'comunicazione_auto' 
                              when page LIKE '%MAIL DISCONNESSI%' then 'disconnessi' 
                              when page LIKE '%MAIL PDISC%' then 'pdisc' 
                          else NULL end as des_mail
                    from dem_adform
                    order by skyid, Timestamp")
View(head(dem_adform_2,1000))
nrow(dem_adform_2)
# 16.024.976



#################################################################################################################################################################################
#################################################################################################################################################################################
#### Alla ricerca delle info disdetta disdetta Sky nel web ######################################################################################################################
#################################################################################################################################################################################
#################################################################################################################################################################################


## Pagina di reindirizzamento paid di google:
# http://www.sky.it/assistenza/info-disdetta/sky.html?cmp=search_disdetta_google_null_null

## Prima pagina paid di google:
# https://www.guidafisco.it/modulo-disdetta-sky-719

## Prima pagina no-paid di google:
# http://www.sky.it/assistenza/conosci/informazioni-sull-abbonamento/termini-e-condizioni/come-posso-dare-la-disdetta-del-mio-contratto.html

## Seconda pagina no-paid di google:
# https://www.sky.it/assistenza/info-disdetta/sky.html

## Terza pagina no-paid di google:
# https://www.sky.it/assistenza/info-disdetta.html

## Quarta pagina (ARANZULLA) no-paid di google:
# https://www.aranzulla.it/disdetta-sky-50455.html
## Prima pagina paid (ARANZULLA) di google con ricerca "come disdire..":
#https://www.aranzulla.it/come-disdire-sky-26733.html
## Seconda pagina no-paid (ARANZULLA) di google con ricerca "come disdire..":
# https://www.aranzulla.it/disdetta-sky-indirizzo-1019550.html

## Quinta pagina no-paid di google:
# https://www.disdette360.it/come-fare-disdetta-sky/

## Sesta pagina no-paid di google:
# https://www.altroconsumo.it/landing/disdetta-sky

## Altra pagina di reindirizzamento:
# http://www.sky.it/assistenza/conosci/informazioni-sull-abbonamento/modificare-l-abbonamento/cosa-devo-fare-per-ridurre-la-composizione-del-mio-abbonamento.html






## Pagina di atterraggio dal banner sky
# http://www.sky.it/landing/clienti/ups/aggiungi-pacchetto.html?cmp=RTGT_UPSELLING-Clienti-retargeting_criteo_300x600_Ret_promozioni_criteo


#################################################################################################################################################################################
#################################################################################################################################################################################


adform_click_1 <- read.parquet('/STAGE/adform/table=Click/Click.parquet')
View(head(adform_click_1,1000))

createOrReplaceTempView(adform_click_1, "adform_click_1")

adform_info_esterne_disdetta <- sql( "SELECT regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                                  CookieID, 
                                  `device-name`,
                                  `os-name`, 
                                  `browser-name`,
                                  page,
                                  Timestamp,
                                  ReferrerType,
                                  CookiesEnabled,
                                  VisitNumber,
                                  VisitPageNumber,
                                  PreviuosPageURL,
                                  PageURL
                                  from adform_click_1
                                  where ((customvars.systemvariables like '%13%' and CookieID <> '0' ) and
                                        (PageURL LIKE '%https://www.guidafisco.it/modulo-disdetta-sky-719%' or
                                        PageURL LIKE '%https://www.aranzulla.it/disdetta-sky-50455.html%' or
                                        PageURL LIKE '%https://www.aranzulla.it/come-disdire-sky-26733.html%' or
                                        PageURL LIKE '%https://www.aranzulla.it/disdetta-sky-indirizzo-1019550.html%' or
                                        PageURL LIKE '%https://www.disdette360.it/come-fare-disdetta-sky/%' or
                                        PageURL LIKE '%https://www.altroconsumo.it/landing/disdetta-sky%'))
                                  having (skyid <> '' and length(skyid)= 48)")
#View(head(adform_info_esterne_disdetta,1000))
#nrow(adform_info_esterne_disdetta)
write.parquet(adform_info_esterne_disdetta,"/user/stefano.mazzucca/scarico_info_esterne_disdetta_sky.parquet")


adform_info_esterne_disdetta_sky <- read.parquet("/user/stefano.mazzucca/scarico_info_esterne_disdetta_sky.parquet")
View(head(adform_info_esterne_disdetta_sky,1000))
nrow(adform_info_esterne_disdetta_sky)
# 10







#chiudi sessione
sparkR.stop()
