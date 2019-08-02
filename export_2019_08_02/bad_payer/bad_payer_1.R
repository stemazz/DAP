
## Bad payer

source("connection_R.R")
options(scipen = 10000)


giu <- read.df("/user/stefano.mazzucca/bad_payer/Candidati_Giugno_2018_def.txt", source = "csv", header = "true", delimiter = "\t")
View(head(giu,100))
nrow(giu)
# 121.962

lug <- read.df("/user/stefano.mazzucca/bad_payer/Candidati_Luglio_2018_def.txt", source = "csv", header = "true", delimiter = "\t")
View(head(lug,100))
nrow(lug)
# 124.354









#chiudi sessione
sparkR.stop()
