
# Verifica di ANALISI sugli INTERESSI

#apri sessione
source("connection_R.R")
options(scipen = 1000)




calcio_senza_calcio <- read.parquet("/user/alessandro/interessati_calcio_no_pacchetto_calcio.parquet")
# hdfs:///user/alessandro/interessati_calcio_no_pacchetto_calcio.parquet
View(head(calcio_senza_calcio,100))
nrow(calcio_senza_calcio)
# 49.731



sport_no_sport <- read.parquet("/user/alessandro/interessati_sport_no_pacchetto_sport.parquet")
# hdfs:///user/alessandro/interessati_sport_no_pacchetto_sport.parquet
View(head(sport_no_sport,100))
nrow(sport_no_sport)
# 64.143



cinema_no_cinema <- read.parquet("/user/alessandro/interessati_cinema_no_pacchetto_cinema.parquet")
# hdfs:///user/alessandro/interessati_cinema_no_pacchetto_cinema.parquet
View(head(cinema_no_cinema,100))
nrow(cinema_no_cinema)
# 13.837



calcio_sport_no_calcio_o_sport <- read.parquet("/user/alessandro/interessati_calcio_o_sport_no_pacchetto_calcio_o_sport.parquet")
# hdfs:///user/alessandro/interessati_calcio_o_sport_no_pacchetto_calcio_o_sport.parquet
View(head(calcio_sport_no_calcio_o_sport,100))
nrow(calcio_sport_no_calcio_o_sport)
# 121.774



calcio_sport_no_calcio_e_sport <- read.parquet("/user/alessandro/interessati_calcio_o_sport_no_pacchetto_calcio_e_sport.parquet")
# hdfs:///user/alessandro/interessati_calcio_o_sport_no_pacchetto_calcio_e_sport.parquet
View(head(calcio_sport_no_calcio_e_sport,100))
nrow(calcio_sport_no_calcio_e_sport)
# 27.692







#chiudi sessione
sparkR.stop()
