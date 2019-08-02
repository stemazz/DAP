
## 06_pulizia_code_Adform ##

source("connection_R.R")
options(scipen = 10000)

source("Digitalizzazione/NEW_kpi_digital/Production_Code/00_Utils_kpi_digital.R")



impression_finale = read.parquet(path_impression_join_finale)

createOrReplaceTempView(impression_finale, "finale")


# Impression_tot:
percentili_impression_tot <- sql("select percentile_approx(impression_tot, 0.99) as q99 from finale where impression_tot is NOT NULL")
# percentili_impression_tot = withColumn(percentili_impression_tot, 'pippo', cast(percentili_impression_tot$q99, 'int'))
# View(head(percentili_impression_tot,10))
novantanove_impression_tot <- first(percentili_impression_tot) # seleziono il p99
nn_impression_tot <- as.integer(novantanove_impression_tot) # lo trasformo in integer
impression_finale$impression_tot = ifelse(impression_finale$impression_tot > percentili_impression_tot$pippo, nn_impression_tot, impression_finale$impression_tot)


# Impression_mobile:
percentili_impression_mobile <- sql("select percentile_approx(impression_mobile, 0.99) as q99 from finale where impression_mobile is NOT NULL")
novantanove_impression_mobile <- first(percentili_impression_mobile) # seleziono il p99
nn_impression_mobile <- as.integer(novantanove_impression_mobile) # lo trasformo in integer
impression_finale$impression_mobile = ifelse(impression_finale$impression_mobile > nn_impression_mobile, nn_impression_mobile, impression_finale$impression_mobile)


# impression_desktop:
percentili_impression_desktop <- sql("select percentile_approx(impression_desktop, 0.99) as q99 from finale where impression_desktop is NOT NULL")
novantanove_impression_desktop <- first(percentili_impression_desktop) # seleziono il p99
nn_impression_desktop <- as.integer(novantanove_impression_desktop) # lo trasformo in integer
impression_finale$impression_desktop = ifelse(impression_finale$impression_desktop > nn_impression_desktop, nn_impression_desktop, impression_finale$impression_desktop)


# impression_tablet:
percentili_impression_tablet <- sql("select percentile_approx(impression_tablet, 0.99) as q99 from finale where impression_tablet is NOT NULL")
novantanove_impression_tablet <- first(percentili_impression_tablet) # seleziono il p99
nn_impression_tablet <- as.integer(novantanove_impression_tablet) # lo trasformo in integer
impression_finale$impression_tablet = ifelse(impression_finale$impression_tablet > nn_impression_tablet, nn_impression_tablet, impression_finale$impression_tablet)


# impression_week:
percentili_impression_week <- sql("select percentile_approx(impression_week, 0.99) as q99 from finale where impression_week is NOT NULL")
novantanove_impression_week <- first(percentili_impression_week) # seleziono il p99
nn_impression_week <- as.integer(novantanove_impression_week) # lo trasformo in integer
impression_finale$impression_week = ifelse(impression_finale$impression_week > nn_impression_week, nn_impression_week, impression_finale$impression_week)


# impression_weekend:
percentili_impression_weekend <- sql("select percentile_approx(impression_weekend, 0.99) as q99 from finale where impression_weekend is NOT NULL")
novantanove_impression_weekend <- first(percentili_impression_weekend) # seleziono il p99
nn_impression_weekend <- as.integer(novantanove_impression_weekend) # lo trasformo in integer
impression_finale$impression_weekend = ifelse(impression_finale$impression_weekend > nn_impression_weekend, nn_impression_weekend, impression_finale$impression_weekend)


# impression_mattina:
percentili_impression_mattina <- sql("select percentile_approx(impression_mattina, 0.99) as q99 from finale where impression_mattina is NOT NULL")
novantanove_impression_mattina <- first(percentili_impression_mattina) # seleziono il p99
nn_impression_mattina <- as.integer(novantanove_impression_mattina) # lo trasformo in integer
impression_finale$impression_mattina = ifelse(impression_finale$impression_mattina > nn_impression_mattina, nn_impression_mattina, impression_finale$impression_mattina)


# impression_pomeriggio:
percentili_impression_pomeriggio <- sql("select percentile_approx(impression_pomeriggio, 0.99) as q99 from finale where impression_pomeriggio is NOT NULL")
novantanove_impression_pomeriggio <- first(percentili_impression_pomeriggio) # seleziono il p99
nn_impression_pomeriggio <- as.integer(novantanove_impression_pomeriggio) # lo trasformo in integer
impression_finale$impression_pomeriggio = ifelse(impression_finale$impression_pomeriggio > nn_impression_pomeriggio, nn_impression_pomeriggio, impression_finale$impression_pomeriggio)


# impression_sera:
percentili_impression_sera <- sql("select percentile_approx(impression_sera, 0.99) as q99 from finale where impression_sera is NOT NULL")
novantanove_impression_sera <- first(percentili_impression_sera) # seleziono il p99
nn_impression_sera <- as.integer(novantanove_impression_sera) # lo trasformo in integer
impression_finale$impression_sera = ifelse(impression_finale$impression_sera > nn_impression_sera, nn_impression_sera, impression_finale$impression_sera)


# impression_notte:
percentili_impression_notte <- sql("select percentile_approx(impression_notte, 0.99) as q99 from finale where impression_notte is NOT NULL")
novantanove_impression_notte <- first(percentili_impression_notte) # seleziono il p99
nn_impression_notte <- as.integer(novantanove_impression_notte) # lo trasformo in integer
impression_finale$impression_notte = ifelse(impression_finale$impression_notte > nn_impression_notte, nn_impression_notte, impression_finale$impression_notte)


# avg_impression_mese:
percentili_avg_impression_mese <- sql("select percentile_approx(avg_impression_mese, 0.99) as q99 from finale where avg_impression_mese is NOT NULL")
novantanove_avg_impression_mese <- first(percentili_avg_impression_mese) # seleziono il p99
nn_avg_impression_mese <- as.integer(novantanove_avg_impression_mese) # lo trasformo in integer
impression_finale$avg_impression_mese = ifelse(impression_finale$avg_impression_mese > nn_avg_impression_mese, nn_avg_impression_mese, impression_finale$avg_impression_mese)


# avg_impression_week:
percentili_avg_impression_week <- sql("select percentile_approx(avg_impression_week, 0.99) as q99 from finale where avg_impression_week is NOT NULL")
novantanove_avg_impression_week <- first(percentili_avg_impression_week) # seleziono il p99
nn_avg_impression_week <- as.integer(novantanove_avg_impression_week) # lo trasformo in integer
impression_finale$avg_impression_week = ifelse(impression_finale$avg_impression_week > nn_avg_impression_week, nn_avg_impression_week, impression_finale$avg_impression_week)


# avg_impression_giorno:
percentili_avg_impression_giorno <- sql("select percentile_approx(avg_impression_giorno, 0.99) as q99 from finale where avg_impression_giorno is NOT NULL")
novantanove_avg_impression_giorno <- first(percentili_avg_impression_giorno) # seleziono il p99
nn_avg_impression_giorno <- as.integer(novantanove_avg_impression_giorno) # lo trasformo in integer
impression_finale$avg_impression_giorno = ifelse(impression_finale$avg_impression_giorno > nn_avg_impression_giorno, nn_avg_impression_giorno, impression_finale$avg_impression_giorno)


# numero_device:
percentili_numero_device <- sql("select percentile_approx(numero_device, 0.99) as q99 from finale where numero_device is NOT NULL")
novantanove_numero_device <- first(percentili_numero_device) # seleziono il p99
nn_numero_device <- as.integer(novantanove_numero_device) # lo trasformo in integer
impression_finale$numero_device = ifelse(impression_finale$numero_device > nn_numero_device, nn_numero_device, impression_finale$numero_device)


# numero_mobile:
percentili_numero_mobile <- sql("select percentile_approx(numero_mobile, 0.99) as q99 from finale where numero_mobile is NOT NULL")
novantanove_numero_mobile <- first(percentili_numero_mobile) # seleziono il p99
nn_numero_mobile <- as.integer(novantanove_numero_mobile) # lo trasformo in integer
impression_finale$numero_mobile = ifelse(impression_finale$numero_mobile > nn_numero_mobile, nn_numero_mobile, impression_finale$numero_mobile)


# numero_desktop:
percentili_numero_desktop <- sql("select percentile_approx(numero_desktop, 0.99) as q99 from finale where numero_desktop is NOT NULL")
novantanove_numero_desktop <- first(percentili_numero_desktop) # seleziono il p99
nn_numero_desktop <- as.integer(novantanove_numero_desktop) # lo trasformo in integer
impression_finale$numero_desktop = ifelse(impression_finale$numero_desktop > nn_numero_desktop, nn_numero_desktop, impression_finale$numero_desktop)


# numero_impression_social:
percentili_numero_impression_social <- sql("select percentile_approx(numero_impression_social, 0.99) as q99 from finale where numero_impression_social is NOT NULL")
novantanove_numero_impression_social <- first(percentili_numero_impression_social) # seleziono il p99
nn_numero_impression_social <- as.integer(novantanove_numero_impression_social) # lo trasformo in integer
impression_finale$numero_impression_social = ifelse(impression_finale$numero_impression_social > nn_numero_impression_social, nn_numero_impression_social, impression_finale$numero_impression_social)



# Pulizia code numero_cookies:
percentili_numero_cookies <- sql("select percentile_approx(numero_cookies, 0.99) as q99
                                 from finale
                                 where numero_cookies is NOT NULL")

novantanove_numero_cookies <- first(percentili_numero_cookies) # seleziono il p99
nn_numero_cookies <- as.integer(novantanove_numero_cookies) # lo trasformo in integer
impression_finale$numero_cookies = ifelse(impression_finale$numero_cookies > nn_numero_cookies, nn_numero_cookies, impression_finale$numero_cookies)





write.parquet(impression_finale, path_impression_finale_pulito)
impression_pulito = read.parquet(path_impression_finale_pulito)
#nrow(impression_pulito) #2941450
#colnames(impression_pulito)


