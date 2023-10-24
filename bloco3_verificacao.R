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

df_bloco3_verificacao <- read.csv("Indicadores dados/indicadores_bloco3_assistencia_pre-natal_2012-2020.csv") |>
  filter(ANO >= 2017)

df_aux_municipios <- read.csv("Indicadores dados/tabela_aux_municipios.csv") |>
  rename(codigo = codmunres)

# Total de nascidos vivos -------------------------------------------------
df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2017)',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'total_de_nascidos_vivos')
  df <- rbind(df, dataframe)

  repeat {

    cursor <- content(request)$cursor

    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                    ' FROM \\"datasus-sinasc\\"',
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2017)',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'total_de_nascidos_vivos')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nascidos <- df |>
  mutate_if(is.character, as.numeric) |>
  filter(codigo %in% df_aux_municipios$codigo)

df_verificacao <- filter(df_nascidos, ano < 2021)

sum(df_verificacao$total_de_nascidos_vivos) - sum(df_bloco3_verificacao$TOTAL_DE_NASCIDOS_VIVOS)

df_bloco3_atualizado <- filter(df_nascidos, ano == 2021)


# Cobertura de assistência Pré-natal ----------------------
df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2017)',
                  ' AND (CONSPRENAT >= 1) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'MULHERES_COM_PELO_MENOS_UMA_CONSULTA_PRENATAL')
  df <- rbind(df, dataframe)

  repeat {

    cursor <- content(request)$cursor

    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                    ' FROM \\"datasus-sinasc\\"',
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2017)',
                    ' AND (CONSPRENAT >= 1) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'MULHERES_COM_PELO_MENOS_UMA_CONSULTA_PRENATAL')
    df <- rbind(df, dataframe)
  }
}

head(df)

df_cob_assis_pre_natal <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_cob_assis_pre_natal$MULHERES_COM_PELO_MENOS_UMA_CONSULTA_PRENATAL[which(is.na(df_cob_assis_pre_natal$MULHERES_COM_PELO_MENOS_UMA_CONSULTA_PRENATAL))] <- 0

df_verificacao <- filter(df_cob_assis_pre_natal, ano < 2021)

sum(df_verificacao$MULHERES_COM_PELO_MENOS_UMA_CONSULTA_PRENATAL) - sum(df_bloco3_verificacao$MULHERES_COM_PELO_MENOS_UMA_CONSULTA_PRENATAL)

df_bloco3_atualizado <- left_join(df_bloco3_atualizado, df_cob_assis_pre_natal)

# Proporção de mulheres com início precoce do pré-natal (até o 3 mês de gestação) ---------------

df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2017)',
                  ' AND (MESPRENAT =  1 OR MESPRENAT =2 OR MESPRENAT =  3)  ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'MULHERES_COM_INICIO_PRECOCE_DO_PRENATAL')
  df <- rbind(df, dataframe)

  repeat {

    cursor <- content(request)$cursor

    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                    ' FROM \\"datasus-sinasc\\"',
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >=2017)',
                    ' AND (MESPRENAT =  1 OR MESPRENAT =2 OR MESPRENAT =  3) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'MULHERES_COM_INICIO_PRECOCE_DO_PRENATAL')
    df <- rbind(df, dataframe)
  }
}


head(df)

df_inicio_precoce_prenatal <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_inicio_precoce_prenatal$MULHERES_COM_INICIO_PRECOCE_DO_PRENATAL[which(is.na(df_inicio_precoce_prenatal$MULHERES_COM_INICIO_PRECOCE_DO_PRENATAL))] <- 0

df_verificacao <- filter(df_inicio_precoce_prenatal, ano < 2021)

sum(df_verificacao$MULHERES_COM_INICIO_PRECOCE_DO_PRENATAL) - sum(df_bloco3_verificacao$MULHERES_COM_INICIO_PRECOCE_DO_PRENATAL)

df_bloco3_atualizado <- left_join(df_bloco3_atualizado, df_inicio_precoce_prenatal)

# Proporção de mulheres com mais de sete consultas de pré-natal ------------------

df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2017)',
                  ' AND (CONSPRENAT > 7)  ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'MULHERES_COM_MAIS_DE_SETE_CONSULTAS_PRENATAL')
  df <- rbind(df, dataframe)

  repeat {

    cursor <- content(request)$cursor

    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                    ' FROM \\"datasus-sinasc\\"',
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2017)',
                    ' AND (CONSPRENAT > 7 ) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'MULHERES_COM_MAIS_DE_SETE_CONSULTAS_PRENATAL')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_mais_de_sete_consultas <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_mais_de_sete_consultas$MULHERES_COM_MAIS_DE_SETE_CONSULTAS_PRENATAL[which(is.na(df_mais_de_sete_consultas$MULHERES_COM_MAIS_DE_SETE_CONSULTAS_PRENATAL))] <- 0

df_verificacao <- filter(df_mais_de_sete_consultas, ano < 2021)

sum(df_verificacao$MULHERES_COM_MAIS_DE_SETE_CONSULTAS_PRENATAL) - sum(df_bloco3_verificacao$MULHERES_COM_MAIS_DE_SETE_CONSULTAS_PRENATAL)

df_bloco3_atualizado <- left_join(df_bloco3_atualizado, df_mais_de_sete_consultas)

# Placeholder para indicadores que não são do SINASC ----------------------
df_bloco3_atualizado$CASOS_SC <- NA
names(df_bloco3_atualizado) <- tolower(names(df_bloco3_atualizado))
# Concatenando as duas bases ----------------------------------------------
df_bloco3_antigo <- read.csv("Indicadores dados/indicadores_bloco3_assistencia_pre-natal_2012-2020.csv") |>
  janitor::clean_names()
names(df_bloco3_antigo) <- tolower(names(df_bloco3_antigo))
df_bloco3_atualizado <- rename(df_bloco3_atualizado, codmunres = codigo)

df_bloco3_completo <- rbind(df_bloco3_antigo, df_bloco3_atualizado) |>
  arrange(codmunres)

write.csv(df_bloco3_completo, "Indicadores dados/indicadores_bloco3_assistencia_pre-natal_2012-2021.csv",row.names=F)
