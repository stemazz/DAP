
prova_skyappwsc <- read.parquet("/STAGE/adobe/reportsuite=skyappwsc.prod")

View(head(prova_skyappwsc, 100))
nrow(prova_skyappwsc)
# [1] 322.298.506

prova_skyappwsc_2 <- withColumn(prova_skyappwsc, "ts", cast(prova_skyappwsc$date_time, "timestamp"))

View(head(prova_skyappwsc_2))
nrow(prova_skyappwsc_2)
# [1] 322.298.506

DAT_prova2 <- withColumn(prova2,"date_time",cast(cast(unix_timestamp(prova2$DAT_RILEVAZIONE, 'yyy/MM/dd'), 'timestamp'), 'date'))




prova_skyappwsc_3 <- filter(prova_skyappwsc_2,"ts>='2017-03-01 00:00:00' and  ts<='2017-09-30 00:00:00' ")

View(head(prova_skyappwsc_3))
nrow(prova_skyappwsc_3)
# [1] 90.858.584

