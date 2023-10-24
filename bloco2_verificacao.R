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

df_bloco2_verificacao <- read.csv("Indicadores dados/indicadores_bloco2_planejamento_reprodutivo_SUS_ANS_2012_2020.csv",sep = ';') |>
  filter(ANO >= 2017)

df_aux_municipios <- read.csv("Indicadores dados/tabela_aux_municipios.csv") |>
  rename(codigo = codmunres)
df_bloco2_verificacao <- df_bloco2_verificacao |>
  filter(CODMUNRES  %in% df_aux_municipios$codigo)
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

sum(df_verificacao$total_de_nascidos_vivos) - sum(df_bloco2_verificacao$TOTAL_DE_NASCIDOS_VIVOS)

df_bloco2_atualizado <- filter(df_nascidos, ano == 2021)



# Proporção de nascidos vivos de mulheres com mais de 3 partos anteriores ----------------------
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
                  ' AND (QTDPARTNOR > 3 OR (QTDPARTNOR > 2 AND QTDPARTCES > 0) ',
                  ' OR (QTDPARTNOR > 1 AND QTDPARTCES > 1) ',
                  ' OR (QTDPARTNOR > 0 AND QTDPARTCES > 2) ',
                  ' OR QTDPARTCES > 3)',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'MULHERES_COM_MAIS_DE_TRES_PARTOS_ANTERIORES')
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
                    ' AND (QTDPARTNOR > 3 OR (QTDPARTNOR > 2 AND QTDPARTCES > 0) ',
                             ' OR (QTDPARTNOR > 1 AND QTDPARTCES > 1) ',
                             ' OR (QTDPARTNOR > 0 AND QTDPARTCES > 2) ',
                             ' OR QTDPARTCES > 3)',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'MULHERES_COM_MAIS_DE_TRES_PARTOS_ANTERIORES')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_mais_de_tres_partos <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_mais_de_tres_partos$MULHERES_COM_MAIS_DE_TRES_PARTOS_ANTERIORES[which(is.na(df_mais_de_tres_partos$MULHERES_COM_MAIS_DE_TRES_PARTOS_ANTERIORES))] <- 0

df_verificacao <- filter(df_mais_de_tres_partos, ano < 2021)

sum(df_verificacao$MULHERES_COM_MAIS_DE_TRES_PARTOS_ANTERIORES) - sum(df_bloco2_verificacao$MULHERES_COM_MAIS_DE_TRES_PARTOS_ANTERIORES)

df_bloco2_atualizado <- left_join(df_bloco2_atualizado, df_mais_de_tres_partos)
# Proporção de nascidos vivos de mulheres com idade inferior a 20 anos (gestação na adolescência) ----------------------
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
                  ' AND (IDADEMAE>=10 AND IDADEMAE<=19) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_menor_que_20_anos')
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
                    ' AND (IDADEMAE>=10 AND IDADEMAE<=19) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_menor_que_20_anos')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_menor_20 <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_menor_20$nvm_menor_que_20_anos[which(is.na(df_nvm_menor_20$nvm_menor_que_20_anos))] <- 0

df_verificacao <- filter(df_nvm_menor_20, ano < 2021)
sum(df_verificacao$nvm_menor_que_20_anos) - sum(df_bloco2_verificacao$NVM_MENOR_QUE_20)

df_nvm_menor_20 <- df_nvm_menor_20 |> rename(
  NVM_MENOR_QUE_20 = nvm_menor_que_20_anos
)


df_bloco2_atualizado <- left_join(df_bloco2_atualizado, df_nvm_menor_20)



# Placeholder para indicadores que não são do SINASC ----------------------
df_bloco2_atualizado$POP_FEMININA_10_A_19 <- NA
df_bloco2_atualizado$ABORTOS_SUS_MENOR_30 <- NA
df_bloco2_atualizado$ABORTOS_SUS_30_A_39 <- NA
df_bloco2_atualizado$ABORTOS_SUS_40_A_49 <- NA
df_bloco2_atualizado$POP_FEM_10_49  <- NA
df_bloco2_atualizado$ABORTOS_ANS_MENOR_30 <- NA
df_bloco2_atualizado$ABORTOS_ANS_30_A_39 <- NA
df_bloco2_atualizado$ABORTOS_ANS_40_A_49 <- NA




names(df_bloco2_atualizado) <- tolower(names(df_bloco2_atualizado))
# Concatenando as duas bases ----------------------------------------------
df_bloco2_antigo <- read.csv("Indicadores dados/indicadores_bloco2_planejamento_reprodutivo_SUS_ANS_2012_2020.csv", sep = ';') |>
  janitor::clean_names()
names(df_bloco2_antigo) <- tolower(names(df_bloco2_antigo))
df_bloco2_atualizado <- rename(df_bloco2_atualizado, codmunres = codigo)

df_bloco2_completo <- rbind(df_bloco2_antigo, df_bloco2_atualizado) |>
  arrange(codmunres)

df_bloco1_completo <- read.csv("Indicadores dados/indicadores_bloco1_socioeconomicos_2012-2021.csv") |>
  janitor::clean_names()
df_bloco1_completo |> names()
aux <- df_bloco1_completo[c('ano','codmunres','nvm_menor_que_20_anos')]
df_bloco2_completo <- merge(df_bloco2_completo,aux,by = c('ano','codmunres'),all = T)
df_bloco2_completo$nvm_menor_que_20 <- df_bloco2_completo$nvm_menor_que_20_anos
df_bloco2_completo$nvm_menor_que_20_anos <- NULL
write.csv(df_bloco2_completo, "Indicadores dados/indicadores_bloco2_planejamento_reprodutivo_SUS_ANS_2012_2021.csv",row.names=F)
