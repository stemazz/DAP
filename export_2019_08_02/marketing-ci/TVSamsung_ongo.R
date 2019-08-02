#TVSamsung_ongo

cm_kpi_skygo <- view_df(cm_kpi_skygo)

#FLG_ACCESSO_SKYGO
#distval_kpi_skygo <- distinct(select(cm_kpi_skygo, "FLG_ACCESSO_SKYGO"))

distval_kpi_skygo  <- summarize(groupBy(cm_kpi_skygo, cm_kpi_skygo$FLG_ACCESSO_SKYGO), count = n(cm_kpi_skygo$FLG_ACCESSO_SKYGO))
View(distval_kpi_skygo )


cm_kpi_vod <- view_df(cm_kpi_vod)


