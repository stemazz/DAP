

## Analisi (da impostare) sui device associati a skygo


#apri sessione
source("connection_R.R")
options(scipen = 1000)



device_skygo <- read.parquet("hdfs:///user/riccardo.motta/devices_sky_go.parquet")
View(head(device_skygo,100))
nrow(device_skygo)
# 3.667.339





#chiudi sessione
sparkR.stop()
