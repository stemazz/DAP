ls_cm <- list(
cm_kpi_vod <- "/STAGE/CMDM/DettaglioServizioVODSTB/full/fat_kpi_servizio_vod_sto.parquet",
cm_ansto <- "/STAGE/CMDM/AmbienteAnalisiStoricizzato/delta/fat_contratto_sto.parquet",
cm_geo <- "/STAGE/CMDM/DatiGEO/full/dim_kpi_codsez2011.parquet",
cm_inb_fall <- "/STAGE/CMDM/DettaglioChiamateInbound/delta/fat_call_track_curva_caduta_20170321.parquet",
cm_dett_camp <- "/STAGE/CMDM/DettaglioContattiResponseCampagneAM/delta/fat_contact_response_with_timestamp",
cm_resp_rtd <- "/STAGE/CMDM/DettaglioContattiResponseRTD/delta/vs_fat_rtd_contact_response.parquet",
cm_dmp <- "/STAGE/CMDM/DettaglioDMPDataWeb/delta/dim_dmp_interessi.parquet",
cm_esigenze <- "/STAGE/CMDM/DettaglioEsigenze/delta/vs_esigenze_dett.parquet",
cm_mgm <- "/STAGE/CMDM/DettaglioMovimentiMGM/vs_presentato_presentatore.parquet",
cm_mov_business <- "/STAGE/CMDM/DettaglioMovimentiStatoBusiness/delta/vs_cambio_stato_business.parquet",
cm_mov_tec <- "/STAGE/CMDM/DettaglioMovimentiTecnologici/delta/fat_movimenti_tecnologici.parquet",
cm_ppv <- "/STAGE/CMDM/DettaglioPPV/full/fat_consumo_ppv_dett.parquet",
cm_pullvod <- "/STAGE/CMDM/DettaglioPULLLVOD/delta/fat_consumo_pullvod_dett.parquet",
cm_promo <- "/STAGE/CMDM/DettaglioPromozioni/delta/vs_fat_tracking_promo_sky_act.parquet",
cm_kpi_skygo <- "/STAGE/CMDM/DettaglioServizioSkyGo/full/fat_kpi_servizio_skygo_sto.parquet",
cm_anagr_cb <- "/STAGE/CMDM/Anagrafica_clienti/full/VS_FAT_CLIENTE.parquet",
cm_iscr_skyextra <- "/STAGE/CMDM/DettaglioSkyExtraReward/delta/fat_iscrizioni_skyextra_sto.parquet",
cm_usageskygo <- "/STAGE/CMDM/DettaglioUsageSkyGO/full/fat_kpi_usage_skygo_sto.parquet",
cm_usagevod <- "/STAGE/CMDM/DettaglioUsageVOD/full/fat_kpi_usage_vod_sto.parquet",
cm_login_QMI <- "/STAGE/CMDM/Dettaglio_Iniziative_QMI/delta/FAT_LOGIN_INIZIATIVE_QMI_STO.parquet",
cm_partecip_QMI <- "/STAGE/CMDM/Dettaglio_Iniziative_QMI/delta/FAT_PARTEC_INIZIATIVE_QMI_STO.parquet",
cm_quest <- "/STAGE/CMDM/Dettaglio_Questionari_NPS_RTD/delta/VS_FAT_QUEST_NPS_RTD.parquet",
cm_updown <- "/STAGE/CMDM/Dettaglio_attivazioni_Up_Downgrade/full/vs_attiv_up_downgrade.parquet",
cm_updownreal <- "/STAGE/CMDM/Dettaglio_richieste_Up_Downgrade_Reali/full/vs_rich_up_downgrade.parquet",
cm_scoring <- "/STAGE/CMDM/Dettaglio_Questionari_NPS_RTD/delta/VS_FAT_QUEST_NPS_RTD.parquet",
cm_mov_arpu_dev <- "/STAGE/CMDM/Movimentazione_Cluster_Arpu_Development/full/fat_mov_cluster_arpu_dev.parquet",
cm_cod_contratto <- "/STAGE/CMDM/cod_contratto_cod_cliente.parquet",
cm_mov_arpu <- "/STAGE/CMDM/Dettaglio_movimenti_Arpu_Teorico_e_Apu_Net_Discount/full/fat_mov_contratto_arpu.parquet",
cm_sto_pack <- "/STAGE/CMDM/StoricoStatoPacchetti/full/FAT_CON_MOV_PACK_TECNOLOGIA.parquet"
)

cm_resp_camp <- "/STAGE/CMDM/DettaglioContattiResponseCampagneAM/delta/fat_contact_response_NEW.parquet"

ppv_ippv <- "/STAGE/PPV/IPPV/full/IPPV.parquet"
ppv_oppv <- "/STAGE/PPV/OPPV/full/OPPV.parquet"
ppv_vod <- "/STAGE/PPV/VOD/full/VOD.parquet"

schedule <- "/STAGE/Schedule/LinearSchedule/full/Palinsesto_Lineare.parquet"

ls_ppv <- list(ppv_ippv, ppv_oppv, ppv_vod)


ls_extra <- list(
extra_attiv_skygo <- "/STAGE/SkyExtra/AttivazioniSKYGO/full/AttivazioniSKYGO.parquet",
extra_foto <- "/STAGE/SkyExtra/FotoExtra/full/FotoExtra.parquet",
extra_mov <- "/STAGE/SkyExtra/MovimentazioneSkyExtra/full/MovimentazioneSkyExtra.parquet",
extra_premired <- "/STAGE/SkyExtra/PremiRedenti/full/PremiRedenti.parquet",
extra_superhd <- "/STAGE/SkyExtra/SuperHD/full/SuperHD.parquet"
)

ls_skygo <- list(
skygo_downplay <- "/STAGE/SkyGo/Download_and_Play/full/SKYGO_dwnl_play.parquet",
skygo_view <- "/STAGE/SkyGo/Viewing/full/SKYGO_viewing.parquet"
)

ls_vod <- list(
vod_skyod_view <- "/STAGE/VoD/SkyOnDemand_viewing/full/SkyOnDemand_viewing.parquet",
vod_schedule <- "/STAGE/VoD/VodSchedule/full/Palinsesto_VOD.parquet"
)

ls_adform <- list(
adform_click <- "/STAGE/adform/table=Click/Click.parquet",
adform_event <- "/STAGE/adform/table=Event/Event.parquet",
adform_meta_geo <- "/STAGE/adform/table=meta/geolocations.parquet",
adform_meta_camp <- "/STAGE/adform/table=meta/campaigns.parquet",
adform_meta_event <- "/STAGE/adform/table=meta/events.parquet",
adform_meta_trackp <- "/STAGE/adform/table=meta/trackingpoints.parquet"
)


ls_SpeechAnalytics <- list(

speech_arg <- "/STAGE/SpeechAnalytics/Almawave/full/ArgomentiDellaChiamata_2017.parquet",
speech_class <- "/STAGE/SpeechAnalytics/Almawave/full/ClassificazioneConversazione_2017.parquet",
speech_cont <- "/STAGE/SpeechAnalytics/Almawave/full/ContenutoSemanticoDellaChiamata_2017.parquet",
speech_trans <- "/STAGE/SpeechAnalytics/Almawave/full/TrascrizioneConversazione_2017.parquet",

decod <- "/STAGE/SpeechAnalytics/bi/full/Decodifica_cod_contratto.parquet",

speech <- "/STAGE/SpeechAnalytics/bi/full/speech_analytics_2017.parquet"

)




vod <- "/STAGE/PPV/VOD/full/VOD.parquet"
skygo_view <- "/STAGE/SkyGo/Viewing/full/SKYGO_viewing.parquet"
down_n_play <- "/STAGE/SkyGo/Download_and_Play/full/SKYGO_dwnl_play.parquet"
vod_view <- "/STAGE/VoD/SkyOnDemand_viewing/full/SkyOnDemand_viewing.parquet"

#ls_url <- list(vod, skygo_view, down_n_play, vod_view, dmp, geo, inbound_fallcurve, esigenze,
#               mov_business, mov_tec, ppv, vod_consumo, kpi_skygo, anagr_cb)





#--------------FUNZIONI:----------------------


#view head df
view_df <- function(url) {
  if (typeof(url)=="character"){
    df <- read.df(url, 'parquet')
    View(head(df))
    df
  } else{
    View(head(url))
    url
  }
}
#extra_attiv_skygo <- view_df(extra_attiv_skygo)



# stampa la struttura di un parquet, e con showval TRUE dando una colonna ne mostra i valori distinti
explore_df <- function(url, showval=FALSE, colonn) {
  if (showval == FALSE) {
  df <- read.df(url, 'parquet')
  str(df)
  } else {
    if (typeof(colonn)=="character") {
      df <- read.df(url, 'parquet')
      print(str(df))
      print(collect(distinct(select(df, colonn))))
    } else {
    df <- read.df(url, 'parquet')
    print(str(df)) 
    print(collect(distinct(select(df, colnames(df)[[colonn]]))))
    }
  }
}



# mostra i valori distinti di una colonna IMPO: FUNZIONA SOLO CON URL E NON CON DF!!!
dist_Vals <- function(url, ncol) {
  if (typeof(url)=="character"){
    
  }
  if (typeof(url)=="character" & typeof(ncol)=="character"){
    df <- read.df(url, 'parquet')
    print(collect(distinct(select(df, ncol))))
  } 
  else if (typeof(url)=="character" & typeof(ncol)=="double"){
    df <- read.df(url, 'parquet')
    print(collect(distinct(select(df, colnames(df)[[ncol]]))))
  }
  else if (typeof(url)=="S4" & typeof(ncol)=="character"){
    print(collect(distinct(select(url, ncol))))
  }
  else{
    print(collect(distinct(select(url, colnames(url)[[ncol]]))))
  }
}
#dist_Vals(extra_attiv_skygo, 2)



# salva in un .csv la struttura di un url
print_str <- function(url, filename) {
  out <- capture.output(print(explore_df(url)))
  write.table("===============",filename, append=TRUE, quote=FALSE)
  write.table(url,filename, append=TRUE, quote=FALSE)
  write.table("===============",filename, append=TRUE, quote=FALSE)
  write.table(out, filename, append=TRUE, quote=FALSE)
  write.table("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++",filename, append=TRUE, quote=FALSE)
}



print_colnames <- function(url, filename) {
  df <- read.df(url, 'parquet')
  out <- colnames(df)
  write.csv(out, filename)
  print(out)
}



#cambio formato data
str_to_data <- function(df, colname, format) {
  dff <- withColumn(df,colname,cast(cast(unix_timestamp(df[[colname]], format), 'timestamp'), 'date'))
  
}
#str_to_data(extra_attiv_skygo,"DATA_EVENTO","dd-MMM-yy")
#oppure("dd/MM/yyyy")





