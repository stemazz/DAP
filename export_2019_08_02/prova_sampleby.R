
# Prova SAMPLEBY

#apri sessione
source("connection_R.R")
options(scipen = 1000)



df <- read.parquet("/user/stefano.mazzucca/CJ_PDISC_base_cifr_dig.parquet")
View(head(df, 100))
nrow(df)
# 37.976

df2 <- withColumn(df, "digitalizzazione_bin_string", cast(df$digitalizzazione_bin, "string"))


# fractions <- setNames(as.list(c(0.0,0.0,0.1,0.01,0.02,0.05,0.07,0.14,0.17,0.18,0.35)),c(0,1,2,3,4,5,6,7,8,9,10))
# View(fractions)

fractions2 <- list(
  "0" = c(0.0),
  "1" = c(0.0),
  "2" = c(0.01),
  "3" = c(0.01),
  "4" = c(0.02),
  "5" = c(0.05),
  "6" = c(0.07),
  "7" = c(0.14),
  "8" = c(0.17),
  "9" = c(0.18),
  "10" = c(0.35)
)
View(fractions2)

prova_sample <- sampleBy(df2, "digitalizzazione_bin_string", fractions2, 36)
View(head(prova_sample))
nrow(prova_sample)
# 3.382


## Verifiche #####################################################################

createOrReplaceTempView(df, "df")
createOrReplaceTempView(prova_sample, "prova_sample")


distr_dig <- sql("select digitalizzazione_bin, count(*)
                            from df
                            group by digitalizzazione_bin")
View(head(distr_dig,100))


distr_dig_str <- sql("select digitalizzazione_bin_string, count(*) 
                            from prova_sample
                            group by digitalizzazione_bin_string")
View(head(distr_dig_str,100))













#chiudi sessione
sparkR.stop()
