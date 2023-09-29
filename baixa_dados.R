library(tidyverse)
library(httr)
library(janitor)
library(getPass)
library(repr)
library(data.table)
library(readr)
library(openxlsx)

token = getPass()  #Token de acesso à API da PCDaS (todos os arquivos gerados se encontram na pasta "Databases", no Google Drive)

url_base = "https://bigdata-api.fiocruz.br"

convertRequestToDF <- function(request){
  variables = unlist(content(request)$columns)
  variables = variables[names(variables) == "name"]
  column_names <- unname(variables)
  values = content(request)$rows
  df <- as.data.frame(do.call(rbind,lapply(values,function(r) {
    row <- r
    row[sapply(row, is.null)] <- NA
    rbind(unlist(row))
  } )))
  names(df) <- column_names
  return(df)
}

estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
endpoint <- paste0(url_base,"/","sql_query")

##Óbitos de grávidas e puérperas desconsiderados dos anos de 2011 a 2020
df_obitos_externos_aux <- dataframe <- data.frame()

for (estado in estados){
  
  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT ano_obito, DTOBITO, DTNASC, def_sexo, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, TPMORTEOCO, def_assist_med, def_necropsia, causabas_capitulo, causabas_categoria, def_circ_obito, def_acid_trab, def_fonte, COUNT(1)',
                  ' FROM \\"datasus-sim_final_1996-2020_preliminar_2021_2022\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND (ano_obito >= 2011 AND ano_obito <= 2020) AND (OBITOGRAV = 1 OR OBITOPUERP = 1 OR OBITOPUERP = 2) AND causabas_capitulo = \'XX.  Causas externas de morbidade e mortalidade\') ',
                  ' GROUP BY ano_obito, DTOBITO, DTNASC, def_sexo, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, TPMORTEOCO, def_assist_med, def_necropsia, causabas_capitulo, causabas_categoria, def_circ_obito, def_acid_trab, def_fonte",
                  "fetch_size": 65000}
                  }
                  }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c("ano_obito", "data_obito", "data_nasc", "sexo", "raca_cor", "est_civil", "escolaridade", "ocupacao", "res_municipio", "res_sigla_uf", "local_ocorrencia_obito", "ocor_municipio", "ocor_sigla_uf", "idade_obito", "periodo_do_obito", "assistencia_med", "necropsia", "causabas_capitulo", "causabas_categoria", "circunstancia_obito", "acidente_trab", "fonte", "obitos")
  df_obitos_externos_aux <- rbind(df_obitos_externos_aux, dataframe)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":" SELECT ano_obito, DTOBITO, DTNASC, def_sexo, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, TPMORTEOCO, def_assist_med, def_necropsia, causabas_capitulo, causabas_categoria, def_circ_obito, def_acid_trab, def_fonte, COUNT(1)',
                    ' FROM \\"datasus-sim_final_1996-2020_preliminar_2021_2022\\"',
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND (ano_obito >= 2011 AND ano_obito <= 2020) AND (OBITOGRAV = 1 OR OBITOPUERP = 1 OR OBITOPUERP = 2) AND causabas_capitulo = \'XX.  Causas externas de morbidade e mortalidade\')',
                    ' GROUP BY ano_obito, DTOBITO, DTNASC, def_sexo, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, "TPMORTEOCO", def_assist_med, def_necropsia, causabas_capitulo, causabas_categoria, def_circ_obito, def_acid_trab, def_fonte",
                    "fetch_size": 65000, "cursor": "',cursor,'"}
                    }
                    }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    request <- POST(url = endpoint, body = params, encode = "form")
    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c("ano_obito", "data_obito", "data_nasc", "sexo", "raca_cor", "est_civil", "escolaridade", "ocupacao", "res_municipio", "res_sigla_uf", "local_ocorrencia_obito", "ocor_municipio", "ocor_sigla_uf", "idade_obito", "periodo_do_obito", "assistencia_med", "necropsia", "causabas_capitulo", "causabas_categoria", "circunstancia_obito", "acidente_trab", "fonte", "obitos")
    df_obitos_externos_aux <- rbind(df_obitos_externos_aux, dataframe)
  }
}

head(df_obitos_externos_aux)

df_obitos_externos_aux$data_obito <- format(as.Date(df_obitos_externos_aux$data_obito, format = "%d%m%Y"), "%d/%m/%Y")
df_obitos_externos_aux$data_nasc <- format(as.Date(df_obitos_externos_aux$data_nasc, format = "%d%m%Y"), "%d/%m/%Y")

df_obitos_externos <- df_obitos_externos_aux |>
  mutate(
    ano_obito = as.numeric(ano_obito),
      escolaridade = case_when(
        escolaridade == "0" ~ "Sem escolaridade",
        escolaridade == "1" ~ "Fundamental I",
        escolaridade == "2" ~ "Fundamental II",
        escolaridade == "3" ~ "Médio",
        escolaridade == "4" ~ "Superior incompleto",
        escolaridade == "5" ~ "Superior completo",
        escolaridade == "9" ~ "Ignorado"
      ),
      obito_em_idade_fertil = if_else(
        condition = as.numeric(idade_obito) >= 10 & as.numeric(idade_obito) <= 49,
        true = "Sim",
        false = "Não"
      ),
      .after = ocor_sigla_uf,
    periodo_do_obito = case_when(
      periodo_do_obito == "1" ~ "Na gravidez",
      periodo_do_obito == "2" ~ "No parto",
      periodo_do_obito == "3" ~ "No aborto",
      periodo_do_obito == "4" ~ "Até 42 dias após o parto",
      periodo_do_obito == "5" ~ "De 43 dias a 1 ano após o parto",
      periodo_do_obito == "8" ~ "Não ocorreu nestes períodos",
      periodo_do_obito == "9" ~ "Ignorado",
    ),
    obitos = as.numeric(obitos)
  ) |>
  select(!c(idade_obito)) |>
  group_by_all() |>
  summarise(obitos = sum(obitos)) |>
  ungroup()

#df_obitos_externos[is.na(df_obitos_externos)] <- "Ignorado"

janitor::get_dupes(df_obitos_externos)

unique(df_obitos_externos$obitos)

df_obitos_externos <- df_obitos_externos |>
  select(!obitos)

##Exportando os dados 
write.table(df_obitos_externos, 'Obitos_gest_puerp_causas_externas_2011_2020.csv', sep = ",", dec = ".", row.names = FALSE)


# wb <- createWorkbook()
# planilha <- 1
# for (i in 2011:2020) {
#   addWorksheet(wb, as.character(i))
#   writeData(wb, sheet = planilha, df_obitos_externos[which(df_obitos_externos$ano_obito == i), -1])
#   planilha <- planilha + 1
# }
# openxlsx::saveWorkbook(wb, "Obitos_maternos_causas_externas_2011_2020.xlsx", overwrite = TRUE)



##Óbitos que não são de grávidas e puérperas dos anos de 2011 a 2020
df_obitos_externos_nao_gest_puerp_aux <- dataframe <- data.frame()

for (estado in estados){
  
  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT ano_obito, DTOBITO, DTNASC, def_sexo, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, def_assist_med, def_necropsia, causabas_capitulo, causabas_categoria, def_circ_obito, def_acid_trab, def_fonte, COUNT(1)',
                  ' FROM \\"datasus-sim_final_1996-2020_preliminar_2021_2022\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND (ano_obito >= 2011 AND ano_obito <= 2020) AND idade_obito_anos >= 10 AND idade_obito_anos <= 55 AND NOT (OBITOGRAV = 1 OR OBITOPUERP = 1 OR OBITOPUERP = 2) AND causabas_capitulo = \'XX.  Causas externas de morbidade e mortalidade\') ',
                  ' GROUP BY ano_obito, DTOBITO, DTNASC, def_sexo, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, def_assist_med, def_necropsia, causabas_capitulo, causabas_categoria, def_circ_obito, def_acid_trab, def_fonte",
                  "fetch_size": 65000}
                  }
                  }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c("ano_obito", "data_obito", "data_nasc", "sexo", "raca_cor", "est_civil", "escolaridade", "ocupacao", "res_municipio", "res_sigla_uf", "local_ocorrencia_obito", "ocor_municipio", "ocor_sigla_uf", "assistencia_med", "necropsia", "causabas_capitulo", "causabas_categoria", "circunstancia_obito", "acidente_trab", "fonte", "obitos")
  df_obitos_externos_nao_gest_puerp_aux <- rbind(df_obitos_externos_nao_gest_puerp_aux, dataframe)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":" SELECT ano_obito, DTOBITO, DTNASC, def_sexo, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, def_assist_med, def_necropsia, causabas_capitulo, causabas_categoria, def_circ_obito, def_acid_trab, def_fonte, COUNT(1)',
                    ' FROM \\"datasus-sim_final_1996-2020_preliminar_2021_2022\\"',
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND (ano_obito >= 2011 AND ano_obito <= 2020) AND idade_obito_anos >= 10 AND idade_obito_anos <= 55 AND NOT (OBITOGRAV = 1 OR OBITOPUERP = 1 OR OBITOPUERP = 2) AND causabas_capitulo = \'XX.  Causas externas de morbidade e mortalidade\') ',
                    ' GROUP BY ano_obito, DTOBITO, DTNASC, def_sexo, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, def_assist_med, def_necropsia, causabas_capitulo, causabas_categoria, def_circ_obito, def_acid_trab, def_fonte",
                    "fetch_size": 65000, "cursor": "',cursor,'"}
                    }
                    }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    request <- POST(url = endpoint, body = params, encode = "form")
    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c("ano_obito", "data_obito", "data_nasc", "sexo", "raca_cor", "est_civil", "escolaridade", "ocupacao", "res_municipio", "res_sigla_uf", "local_ocorrencia_obito", "ocor_municipio", "ocor_sigla_uf", "assistencia_med", "necropsia", "causabas_capitulo", "causabas_categoria", "circunstancia_obito", "acidente_trab", "fonte", "obitos")
    df_obitos_externos_nao_gest_puerp_aux <- rbind(df_obitos_externos_nao_gest_puerp_aux, dataframe)
  }
}

head(df_obitos_externos_nao_gest_puerp_aux)

df_obitos_externos_nao_gest_puerp_aux$data_obito <- format(as.Date(df_obitos_externos_nao_gest_puerp_aux$data_obito, format = "%d%m%Y"), "%d/%m/%Y")
df_obitos_externos_nao_gest_puerp_aux$data_nasc <- format(as.Date(df_obitos_externos_nao_gest_puerp_aux$data_nasc, format = "%d%m%Y"), "%d/%m/%Y")

df_obitos_externos_nao_gest_puerp_aux2 <- df_obitos_externos_nao_gest_puerp_aux |>
  mutate(
    ano_obito = as.numeric(ano_obito),
    escolaridade = case_when(
      escolaridade == "0" ~ "Sem escolaridade",
      escolaridade == "1" ~ "Fundamental I",
      escolaridade == "2" ~ "Fundamental II",
      escolaridade == "3" ~ "Médio",
      escolaridade == "4" ~ "Superior incompleto",
      escolaridade == "5" ~ "Superior completo",
      escolaridade == "9" ~ "Ignorado"
    ),
    obitos = as.numeric(obitos)
  ) |>
  group_by_all() |>
  summarise(obitos = sum(obitos)) |>
  ungroup()

#df_obitos_externos_nao_gest_puerp[is.na(df_obitos_externos_nao_gest_puerp)] <- "Ignorado"

janitor::get_dupes(df_obitos_externos_nao_gest_puerp_aux2)

unique(df_obitos_externos_nao_gest_puerp_aux2$obitos)

df_obitos_externos_nao_gest_puerp <- rbind(
  df_obitos_externos_nao_gest_puerp_aux2,
  df_obitos_externos_nao_gest_puerp_aux2[rep(which(df_obitos_externos_nao_gest_puerp_aux2$obitos == 2), times = 1),],
  df_obitos_externos_nao_gest_puerp_aux2[rep(which(df_obitos_externos_nao_gest_puerp_aux2$obitos == 3), times = 2),],
  df_obitos_externos_nao_gest_puerp_aux2[rep(which(df_obitos_externos_nao_gest_puerp_aux2$obitos == 7), times = 6),]
  ) |>
  select(!obitos)

sum(df_obitos_externos_nao_gest_puerp_aux2$obitos[which(df_obitos_externos_nao_gest_puerp_aux2$ano_obito == 2011)])
nrow(df_obitos_externos_nao_gest_puerp[which(df_obitos_externos_nao_gest_puerp$ano_obito == 2011), ])

##Exportando os dados 
write.table(df_obitos_externos_nao_gest_puerp, 'Obitos_nao_gest_puerp_causas_externas_2011_2020.csv', sep = ",", dec = ".", row.names = FALSE)

# wb <- createWorkbook()
# planilha <- 1
# for (i in 2011:2020) {
#   addWorksheet(wb, as.character(i))
#   writeData(wb, sheet = planilha, df_obitos_externos_nao_gest_puerp[which(df_obitos_externos_nao_gest_puerp$ano_obito == i), -1])
#   planilha <- planilha + 1
# }
# openxlsx::saveWorkbook(wb, "Obitos_nao_maternos_causas_externas_2011_2020.xlsx", overwrite = TRUE)



