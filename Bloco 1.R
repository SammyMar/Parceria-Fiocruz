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

df_aux_municipios <- read.csv("tabela_aux_municipios.csv") |>
  select(codigo = codmunres)

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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- df_nascidos

write.csv(df_nascidos, 'Indicadores dados/NASCIDOS_VIVOS.csv', row.names = FALSE)


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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_menor_20)

write.csv(df_nvm_menor_20, 'Indicadores dados/gestacao_na_adolescencia.csv', row.names = FALSE)


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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_entre_20_e_34_anos)

write.csv(df_nvm_entre_20_e_34_anos, 'Indicadores dados/MULHERES_COM_IDADE_ENTRE_20_E_34.csv', row.names = FALSE)


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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_maior_34)

write.csv(df_nvm_maior_34, 'Indicadores dados/MULHERES_COM_IDADE_MAIOR_34.csv')

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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_mae_branca)

write.csv(df_nvm_mae_branca, 'Indicadores dados/MULHERES_BRANCAS.csv')

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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_mae_preta)

write.csv(df_nvm_mae_preta, 'Indicadores dados/MULHERES_PRETAS.csv')

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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
                  ' AND (RACACORMAE=3) ',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
    names(dataframe) <- c('codigo', 'ano', 'nvm_com_cor_da_pele_parda')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_mae_parda <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_mae_parda)

write.csv(df_nvm_mae_parda, 'Indicadores dados/MULHERES_PARDAS.csv')

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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_mae_amarela)

write.csv(df_nvm_mae_amarela, 'Indicadores dados/MULHERES_AMARELAS.csv')

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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
                  ' AND (RACACORMAE=3) ',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
    names(dataframe) <- c('codigo', 'ano', 'nvm_indigenas')
    df <- rbind(df, dataframe)
  }
}
head(df)

df_nvm_indigenas <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_indigenas)

write.csv(df_nvm_indigenas, 'Indicadores dados/MULHERES_INDIGENAS.csv')


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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_escolaridade_ate_3)

write.csv(df_nvm_escolaridade_ate_3, 'Indicadores dados/MULHERES_MENOS_DE_4_ANOS_DE_ESTUDO.csv')


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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_escolaridade_4_a_7)

write.csv(df_nvm_escolaridade_4_a_7, 'Indicadores dados/MULHERES_COM_4_A_7_ANOS_DE_ESTUDO.csv')


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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_escolaridade_8_a_11)

write.csv(df_nvm_escolaridade_8_a_11, 'Indicadores dados/MULHERES_COM_8_A_11_ANOS_DE_ESTUDO.csv')


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
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc = 2021)',
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
  right_join(df_aux_municipios)

df_bloco1 <- left_join(df_bloco1, df_nvm_escolaridade_acima_de_11)

write.csv(df_bloco1, 'Indicadores dados/MULHERES_COM_MAIS_DE_11_ANOS_DE_ESTUDO.csv')

# Placeholder para indicadores que não são do SINASC ----------------------
df_bloco1$populacao_feminina_10_a_49 <- NA
df_bloco1$populacao_fem_10_a_49_com_plano_saude <- NA
df_bloco1$media_cobertura_esf <- NA
df_bloco1$populacao_total <- NA



