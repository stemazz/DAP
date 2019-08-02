
## Verifica delle liste

source("connection_R.R")
options(scipen = 10000)


lista_17lug <- read.df("/user/stefano.mazzucca/churn_digital/output_true_modello_CD_20180717.csv", source = "csv" ,header = "true", delimiter = ";")
View(head(lista_17lug,100))
nrow(lista_17lug)
# 2.639


lista_24lug <- read.df("/user/stefano.mazzucca/churn_digital/output_true_modello_CD_20180724.csv", source = "csv" ,header = "true", delimiter = ";")
View(head(lista_24lug,100))
nrow(lista_24lug)
# 2.960


lista_31lug <- read.df("/user/stefano.mazzucca/churn_digital/output_true_modello_CD_20180731.csv", source = "csv" ,header = "true", delimiter = ";")
View(head(lista_31lug,100))
nrow(lista_31lug)
# 4.191



inner_join_17_24_luglio <- join(lista_17lug, lista_24lug, lista_17lug$COD_CLIENTE_CIFRATO == lista_24lug$COD_CLIENTE_CIFRATO, "inner")
View(head(inner_join_17_24_luglio,100))
nrow(inner_join_17_24_luglio)
# 185 COD_CLIENTE_CIFRATO che troviamo sia nella lista del 17 luglio che quella del 24 luglio!


inner_join_24_31_luglio <- join(lista_24lug, lista_31lug, lista_24lug$COD_CLIENTE_CIFRATO == lista_31lug$COD_CLIENTE_CIFRATO, "inner")
View(head(inner_join_24_31_luglio,100))
nrow(inner_join_24_31_luglio)
# 308 COD_CLIENTE_CIFRATO che troviamo sia nella lista del 24 luglio che quella del 31 luglio!


inner_join_17_31_luglio <- join(lista_17lug, lista_31lug, lista_17lug$COD_CLIENTE_CIFRATO == lista_31lug$COD_CLIENTE_CIFRATO, "inner")
View(head(inner_join_17_31_luglio,100))
nrow(inner_join_17_31_luglio)
# 169 COD_CLIENTE_CIFRATO che troviamo sia nella lista del 17 luglio che quella del 31 luglio!


union_liste <- rbind(lista_17lug, lista_24lug, lista_31lug)
nrow(union_liste)
# 9.790

trova_doppioni <- summarize(groupBy(union_liste, "COD_CLIENTE_CIFRATO"), n_volte = count(union_liste$COD_CLIENTE_CIFRATO))
trova_doppioni_1 <- arrange(trova_doppioni, desc(trova_doppioni$n_volte))
View(head(trova_doppioni_1,100))

conta_doppioni <- summarize(groupBy(trova_doppioni_1, "n_volte"), n_cod_clienti = count(trova_doppioni_1$COD_CLIENTE_CIFRATO))
conta_doppioni_1 <- arrange(conta_doppioni, desc(conta_doppioni$n_volte))
View(head(conta_doppioni_1,100))
# n_volte   n_cod_clienti
# 3             31
# 2             569
# 1             8559





#chiudi sessione
sparkR.stop()
