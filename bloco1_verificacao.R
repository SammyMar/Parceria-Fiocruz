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

df_bloco1_verificacao <- read.csv("Indicadores dados/indicadores_bloco1_socioeconomicos_2012-2020.csv") |>
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
sum(df_verificacao$total_de_nascidos_vivos) - sum(df_bloco1_verificacao$TOTAL_DE_NASCIDOS_VIVOS)

df_bloco1_atualizado <- filter(df_nascidos, ano == 2021)


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
sum(df_verificacao$nvm_menor_que_20_anos) - sum(df_bloco1_verificacao$NVM_MENOR_QUE_20_ANOS)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_menor_20)


# Proporção de nascidos vivos de mulheres com idade de 20 a 34 anos -------------------------------
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
                  ' AND (IDADEMAE>=20 AND IDADEMAE<35) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_entre_20_e_34_anos')
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
                    ' AND (IDADEMAE>=20 AND IDADEMAE<35) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_entre_20_e_34_anos')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_entre_20_e_34_anos <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_entre_20_e_34_anos$nvm_entre_20_e_34_anos[which(is.na(df_nvm_entre_20_e_34_anos$nvm_entre_20_e_34_anos))] <- 0

df_verificacao <- filter(df_nvm_entre_20_e_34_anos, ano < 2021)
sum(df_verificacao$nvm_entre_20_e_34_anos) - sum(df_bloco1_verificacao$NVM_ENTRE_20_E_34_ANOS)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_entre_20_e_34_anos)


# Proporção de nascidos vivos de mulheres com idade de 35 ou mais anos -------------------
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
                  ' AND (IDADEMAE>=35 AND IDADEMAE<=55) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_maior_que_34_anos')
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
                    ' AND (IDADEMAE>=35 AND IDADEMAE<55) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_maior_que_34_anos')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_maior_34 <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_maior_34$nvm_maior_que_34_anos[which(is.na(df_nvm_maior_34$nvm_maior_que_34_anos))] <- 0

df_verificacao <- filter(df_nvm_maior_34, ano < 2021)
sum(df_verificacao$nvm_maior_que_34_anos) - sum(df_bloco1_verificacao$NVM_MAIOR_QUE_34_ANOS)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_maior_34)


# Proporção de nascidos vivos de mulheres brancas -------------------------
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
                  ' AND (RACACORMAE=1) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_com_cor_da_pele_branca')
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
                    ' AND (RACACORMAE=1) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_com_cor_da_pele_branca')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_mae_branca <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_mae_branca$nvm_com_cor_da_pele_branca[which(is.na(df_nvm_mae_branca$nvm_com_cor_da_pele_branca))] <- 0

df_verificacao <- filter(df_nvm_mae_branca, ano < 2021)
sum(df_verificacao$nvm_com_cor_da_pele_branca) - sum(df_bloco1_verificacao$NVM_COM_COR_DA_PELE_BRANCA)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_mae_branca)


#Proporção de nascidos vivos de mulheres pretas ------------------------
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
                  ' AND (RACACORMAE=2) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_com_cor_da_pele_preta')
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
                    ' AND (RACACORMAE=2) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_com_cor_da_pele_preta')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_mae_preta <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_mae_preta$nvm_com_cor_da_pele_preta[which(is.na(df_nvm_mae_preta$nvm_com_cor_da_pele_preta))] <- 0

df_verificacao <- filter(df_nvm_mae_preta, ano < 2021)
sum(df_verificacao$nvm_com_cor_da_pele_preta) - sum(df_bloco1_verificacao$NVM_COM_COR_DA_PELE_PRETA)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_mae_preta)


# Proporção de nascidos vivos de mulheres pardas ------------------------------
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
                  ' AND (RACACORMAE=4) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_com_cor_da_pele_parda')
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
                    ' AND (RACACORMAE=4) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_com_cor_da_pele_parda')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_mae_parda <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_mae_parda$nvm_com_cor_da_pele_parda[which(is.na(df_nvm_mae_parda$nvm_com_cor_da_pele_parda))] <- 0

df_verificacao <- filter(df_nvm_mae_parda, ano < 2021)
sum(df_verificacao$nvm_com_cor_da_pele_parda) - sum(df_bloco1_verificacao$NVM_COM_COR_DA_PELE_PARDA)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_mae_parda)


# Proporção de nascidos vivos de mulheres amarelas ------------------------------
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
                  ' AND (RACACORMAE=3) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_com_cor_da_pele_amarela')
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
                    ' AND (RACACORMAE=3) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_com_cor_da_pele_amarela')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_mae_amarela <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_mae_amarela$nvm_com_cor_da_pele_amarela[which(is.na(df_nvm_mae_amarela$nvm_com_cor_da_pele_amarela))] <- 0

df_verificacao <- filter(df_nvm_mae_amarela, ano < 2021)
sum(df_verificacao$nvm_com_cor_da_pele_amarela) - sum(df_bloco1_verificacao$NVM_COM_COR_DA_PELE_AMARELA)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_mae_amarela)


# Proporção de nascidos vivos de mulheres indígenas -------------------------------
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
                  ' AND (RACACORMAE=5) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_indigenas')
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
                    ' AND (RACACORMAE=5) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_indigenas')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_indigenas <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_indigenas$nvm_indigenas[which(is.na(df_nvm_indigenas$nvm_indigenas))] <- 0

df_verificacao <- filter(df_nvm_indigenas, ano < 2021)
sum(df_verificacao$nvm_indigenas) - sum(df_bloco1_verificacao$NVM_INDIGENAS)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_indigenas)


# Proporção de nascidos vivos de mulheres com menos de 4 anos de estudo -------------------
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
                  ' AND (ESCMAE=1 OR ESCMAE=2) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_com_escolaridade_ate_3')
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
                    ' AND (ESCMAE=1 OR ESCMAE=2) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_com_escolaridade_ate_3')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_escolaridade_ate_3 <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_escolaridade_ate_3$nvm_com_escolaridade_ate_3[which(is.na(df_nvm_escolaridade_ate_3$nvm_com_escolaridade_ate_3))] <- 0

df_verificacao <- filter(df_nvm_escolaridade_ate_3, ano < 2021)
sum(df_verificacao$nvm_com_escolaridade_ate_3) - sum(df_bloco1_verificacao$NVM_COM_ESCOLARIDADE_ATE_3)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_escolaridade_ate_3)


# Proporção de nascidos vivos de mulheres com 4 a 7 anos de estudo -----------------
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
                  ' AND (ESCMAE=3) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_com_escolaridade_de_4_a_7')
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
                    ' AND (ESCMAE=3) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_com_escolaridade_de_4_a_7')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_escolaridade_4_a_7 <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_escolaridade_4_a_7$nvm_com_escolaridade_de_4_a_7[which(is.na(df_nvm_escolaridade_4_a_7$nvm_com_escolaridade_de_4_a_7))] <- 0

df_verificacao <- filter(df_nvm_escolaridade_4_a_7, ano < 2021)
sum(df_verificacao$nvm_com_escolaridade_de_4_a_7) - sum(df_bloco1_verificacao$NVM_COM_ESCOLARIDADE_DE_4_A_7)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_escolaridade_4_a_7)


# Proporção de nascidos vivos de mulheres com 8 a 11 anos de estudo -----------------
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
                  ' AND (ESCMAE=4) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_com_escolaridade_de_8_a_11')
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
                    ' AND (ESCMAE=4) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_com_escolaridade_de_8_a_11')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_escolaridade_8_a_11 <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_escolaridade_8_a_11$nvm_com_escolaridade_de_8_a_11[which(is.na(df_nvm_escolaridade_8_a_11$nvm_com_escolaridade_de_8_a_11))] <- 0

df_verificacao <- filter(df_nvm_escolaridade_8_a_11, ano < 2021)
sum(df_verificacao$nvm_com_escolaridade_de_8_a_11) - sum(df_bloco1_verificacao$NVM_COM_ESCOLARIDADE_DE_8_A_11)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_escolaridade_8_a_11)


# Proporção de nascidos vivos de mulheres com mais de 11 anos de estudo  ------------
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
                  ' AND (ESCMAE=5) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nvm_com_escolaridade_acima_de_11')
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
                    ' AND (ESCMAE=5) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nvm_com_escolaridade_acima_de_11')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_escolaridade_acima_de_11 <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

df_nvm_escolaridade_acima_de_11$nvm_com_escolaridade_acima_de_11[which(is.na(df_nvm_escolaridade_acima_de_11$nvm_com_escolaridade_acima_de_11))] <- 0

df_verificacao <- filter(df_nvm_escolaridade_acima_de_11, ano < 2021)
sum(df_verificacao$nvm_com_escolaridade_acima_de_11) - sum(df_bloco1_verificacao$NVM_COM_ESCOLARIDADE_ACIMA_DE_11)

df_bloco1_atualizado <- left_join(df_bloco1_atualizado, df_nvm_escolaridade_acima_de_11)


# Placeholder para indicadores que não são do SINASC ----------------------
df_bloco1_atualizado$media_cobertura_esf <- NA
df_bloco1_atualizado$populacao_total <- NA
df_bloco1_atualizado$pop_fem_10_49_com_plano_saude <- NA
df_bloco1_atualizado$populacao_feminina_10_a_49 <- NA


# Concatenando as duas bases ----------------------------------------------
df_bloco1_antigo <- read.csv("Indicadores dados/indicadores_bloco1_socioeconomicos_2012-2020.csv") |>
  janitor::clean_names()

df_bloco1_atualizado <- rename(df_bloco1_atualizado, codmunres = codigo)

df_bloco1_completo <- rbind(df_bloco1_antigo, df_bloco1_atualizado) |>
  arrange(codmunres)

write.csv(df_bloco1_completo, "Indicadores dados/indicadores_bloco1_socioeconomicos_2012-2021.csv",row.names=F)

