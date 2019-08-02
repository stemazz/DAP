

#CJ_UP vers02

#apri sessione
source("connection_R.R")
options(scipen = 1000)


base_up_filtrata <- read.parquet("/user/valentina/CJ_UP_base_filtrata_5.parquet")
View(head(base_up_filtrata,100))
nrow(base_up_filtrata)
# 3.694.583


## Distribuzione flg_up_3m

createOrReplaceTempView(base_up_filtrata,"base_up_filtrata")
distr_flg_base_up <- sql("select flg_up_3m, count(*)
                            from base_up_filtrata
                            group by flg_up_3m")
View(head(distr_flg_base_up,100))

##Distribuzione flg_up_3m + digitalizzazione_bin

distr_flg_dig <- sql("select flg_up_3m, digitalizzazione_bin, count(*)
                            from base_up_filtrata
                            group by flg_up_3m, digitalizzazione_bin")
View(head(distr_flg_dig,100))



## CAMPOIONAMENTO STRATIFICATO

base_up_0 <- filter(base_up_filtrata, "flg_up_3m = 0")
nrow(base_up_0)
# 3.663.456

base_up_1 <- filter(base_up_filtrata, "flg_up_3m = 1")
nrow(base_up_1)
# 31.127

# 
# fractions <- setNames(as.list(c(0.0,0.0,0.1,0.01,0.02,0.05,0.07,0.14,0.17,0.18,0.35)),c(0,1,2,3,4,5,6,7,8,9,10))
# View(fractions)
# 
# fractions2 <- list(
#   "0" = c(0.0), 
#   "1" = c(0.0),
#   "2" = c(0.01),
#   "3" = c(0.01),
#   "4" = c(0.02),
#   "5" = c(0.05),
#   "6" = c(0.07),
#   "7" = c(0.14),
#   "8" = c(0.17),
#   "9" = c(0.18),
#   "10" = c(0.35)
# )
# View(fractions2)
# 
# prova_sample <- sampleBy(base_up_0, "digitalizzazione_bin", fractions2, 7)
# View(head(prova_sample))
# #nrow(prova_sample)
# 

sub_up_0_sample_2 <- filter(base_up_0, "digitalizzazione_bin = 2")
View(head(sub_up_0_sample_2,100))
nrow(sub_up_0_sample_2)
# 205.290
sample_up_2 <- sample(sub_up_0_sample_2,FALSE,0.01)
View(head(sample_up_2,100))
nrow(sample_up_2)
# 2.039
sample_up_2 <- limit(sample_up_2, 622)
View(head(sample_up_2))
nrow(sample_up_2)
# 622
#sample_up_2 <- as.DataFrame(sample_up_2)

sub_up_0_sample_3 <- filter(base_up_0, "digitalizzazione_bin = 3")
nrow(sub_up_0_sample_3)
# 350.416
sample_up_3 <- sample(sub_up_0_sample_3,FALSE,0.01)
nrow(sample_up_3)
# 3.552
sample_up_3 <- limit(sample_up_3, 622)
nrow(sample_up_3)
# 622
#sample_up_3 <- as.DataFrame(sample_up_3)

sub_up_0_sample_4 <- filter(base_up_0, "digitalizzazione_bin = 4")
nrow(sub_up_0_sample_4)
# 193.163
sample_up_4 <- sample(sub_up_0_sample_4,FALSE,0.02)
nrow(sample_up_4)
# 3.781
sample_up_4 <- limit(sample_up_4, 1245)
nrow(sample_up_4)
# 1.245
#sample_up_4 <- as.DataFrame(sample_up_4)

sub_up_0_sample_5 <- filter(base_up_0, "digitalizzazione_bin = 5")
nrow(sub_up_0_sample_5)
# 281.486
sample_up_5 <- sample(sub_up_0_sample_5,FALSE,0.05)
nrow(sample_up_5)
# 14.188
sample_up_5 <- limit(sample_up_5, 3112)
nrow(sample_up_5)
# 3.112
#sample_up_5 <- as.DataFrame(sample_up_5)

sub_up_0_sample_6 <- filter(base_up_0, "digitalizzazione_bin = 6")
nrow(sub_up_0_sample_6)
# 295.238
sample_up_6 <- sample(sub_up_0_sample_6,FALSE,0.07)
nrow(sample_up_6)
# 20.487
sample_up_6 <- limit(sample_up_6, 4357)
nrow(sample_up_6)
# 4.357
#sample_up_6 <- as.DataFrame(sample_up_6)

sub_up_0_sample_7 <- filter(base_up_0, "digitalizzazione_bin = 7")
nrow(sub_up_0_sample_7)
# 361.958
sample_up_7 <- sample(sub_up_0_sample_7,FALSE,0.14)
nrow(sample_up_7)
# 50.758
sample_up_7 <- limit(sample_up_7, 8715)
nrow(sample_up_7)
# 8.715
#sample_up_7 <- as.DataFrame(sample_up_7)

sub_up_0_sample_8 <- filter(base_up_0, "digitalizzazione_bin = 8")
nrow(sub_up_0_sample_8)
# 341.084
sample_up_8 <- sample(sub_up_0_sample_8,FALSE,0.17)
nrow(sample_up_8)
# 58.131
sample_up_8 <- limit(sample_up_8, 10582)
nrow(sample_up_8)
# 10.582
#sample_up_8 <- as.DataFrame(sample_up_8)

sub_up_0_sample_9 <- filter(base_up_0, "digitalizzazione_bin = 9")
nrow(sub_up_0_sample_9)
# 315.639
sample_up_9 <- sample(sub_up_0_sample_9,FALSE,0.18)
nrow(sample_up_9)
# 56.387
sample_up_9 <- limit(sample_up_9, 11205)
nrow(sample_up_9)
# 11.205
#sample_up_9 <- as.DataFrame(sample_up_9)

sub_up_0_sample_10 <- filter(base_up_0, "digitalizzazione_bin = 10")
nrow(sub_up_0_sample_10)
# 442.813
sample_up_10 <- sample(sub_up_0_sample_10,FALSE,0.35)
nrow(sample_up_10)
# 155.089
sample_up_10 <- limit(sample_up_10, 21787)
nrow(sample_up_10)
# 21.787
#sample_up_10 <- as.DataFrame(sample_up_10)

createOrReplaceTempView(sample_up_2,"sample_up_2")
createOrReplaceTempView(sample_up_3,"sample_up_3")
createOrReplaceTempView(sample_up_4,"sample_up_4")
createOrReplaceTempView(sample_up_5,"sample_up_5")
createOrReplaceTempView(sample_up_6,"sample_up_6")
createOrReplaceTempView(sample_up_7,"sample_up_7")
createOrReplaceTempView(sample_up_8,"sample_up_8")
createOrReplaceTempView(sample_up_9,"sample_up_9")
createOrReplaceTempView(sample_up_10,"sample_up_10")

dataset_sample_up_0 <- sql("select *
                           from sample_up_2
                           union 
                           select *
                           from sample_up_3
                           union
                           select *
                           from sample_up_4
                           union
                           select *
                           from sample_up_5
                           union
                           select *
                           from sample_up_6
                           union
                           select *
                           from sample_up_7
                           union
                           select *
                           from sample_up_8
                           union
                           select *
                           from sample_up_9
                           union
                           select *
                           from sample_up_10")
View(head(dataset_sample_up_0,100))
nrow(dataset_sample_up_0)
# 62.247

createOrReplaceTempView(dataset_sample_up_0,"dataset_sample_up_0")
ver <- sql("select digitalizzazione_bin, count(digitalizzazione_bin)
           from dataset_sample_up_0
           group by digitalizzazione_bin")
View(head(ver,100))

# Salvataggio sample_up_0_stratified

write.parquet(dataset_sample_up_0,"/user/stefano.mazzucca/CJ_UP_sample_stratified_flg_0.parquet")

# prova <- read.parquet("/user/stefano.mazzucca/CJ_UP_sample_stratified_flg_0.parquet")
# nrow(prova)
# View(head(prova,100))






#chiudi sessione
sparkR.stop()
