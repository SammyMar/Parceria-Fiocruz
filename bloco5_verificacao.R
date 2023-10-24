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

df_bloco5_verificacao <- read.csv("Indicadores dados/indicadores_bloco5_condicao_de_nascimento_2012-2020.csv") |>
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
sum(df_verificacao$total_de_nascidos_vivos) - sum(df_bloco5_verificacao$TOTAL_DE_NASCIDOS_VIVOS)

df_bloco5_atualizado <- filter(df_nascidos, ano == 2021)

# Proporção de baixo peso ao nascer ----------------------
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
                  ' AND (PESO < 2500 ) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'NASCIDOS_VIVOS_COM_BAIXO_PESO')
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
                    ' AND (PESO < 2500 ) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'NASCIDOS_VIVOS_COM_BAIXO_PESO')
    df <- rbind(df, dataframe)
  }
}
head(df)

baixo_peso <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

baixo_peso$NASCIDOS_VIVOS_COM_BAIXO_PESO[which(is.na(baixo_peso$NASCIDOS_VIVOS_COM_BAIXO_PESO))] <- 0

df_verificacao <- filter(baixo_peso, ano < 2021)
sum(df_verificacao$NASCIDOS_VIVOS_COM_BAIXO_PESO) - sum(df_bloco5_verificacao$NASCIDOS_VIVOS_COM_BAIXO_PESO)

df_bloco5_atualizado <- left_join(df_bloco5_atualizado, baixo_peso)
# Proporção de nascimentos prematuros -----------------
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
                  ' AND  (GESTACAO < 5 ) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'NASCIDOS_VIVOS_PREMATUROS')
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
                    ' AND (GESTACAO < 5 ) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'NASCIDOS_VIVOS_PREMATUROS')
    df <- rbind(df, dataframe)
  }
}
head(df)

prematuro <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

prematuro$NASCIDOS_VIVOS_PREMATUROS[which(is.na(prematuro$NASCIDOS_VIVOS_PREMATUROS))] <- 0

df_verificacao <- filter(prematuro, ano < 2021)
sum(df_verificacao$NASCIDOS_VIVOS_PREMATUROS) - sum(df_bloco5_verificacao$NASCIDOS_VIVOS_PREMATUROS)

df_bloco5_atualizado <- left_join(df_bloco5_atualizado, prematuro)
# Proporção de nascimentos termo precoce  --------------------
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
                  ' AND (SEMAGESTAC = 37 OR SEMAGESTAC = 38 ) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'NASCIDOS_VIVOS_TERMO_PRECOCE')
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
                    ' AND (SEMAGESTAC = 37 OR SEMAGESTAC = 38 ) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'NASCIDOS_VIVOS_TERMO_PRECOCE')
    df <- rbind(df, dataframe)
  }
}
head(df)

precoce <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

precoce$NASCIDOS_VIVOS_TERMO_PRECOCE[which(is.na(precoce$NASCIDOS_VIVOS_TERMO_PRECOCE))] <- 0

df_verificacao <- filter(precoce, ano < 2021)
sum(df_verificacao$NASCIDOS_VIVOS_TERMO_PRECOCE) - sum(df_bloco5_verificacao$NASCIDOS_VIVOS_TERMO_PRECOCE)

df_bloco5_atualizado <- left_join(df_bloco5_atualizado, precoce)

# Concatenando as duas bases ----------------------------------------------
df_bloco5_antigo <- read.csv("Indicadores dados/indicadores_bloco5_condicao_de_nascimento_2012-2020.csv") |>
  janitor::clean_names()

df_bloco5_atualizado <- rename(df_bloco5_atualizado, codmunres = codigo)
names(df_bloco5_atualizado) <- tolower(names(df_bloco5_atualizado))
df_bloco5_completo <- rbind(df_bloco5_antigo, df_bloco5_atualizado) |>
  arrange(codmunres)

# CRIACAO DOS NOVOS INDICADORES ---------------------------------------
# proporcao de nascidos vivos com menos de 1500g
df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND (ano_nasc >= 2012 AND ano_nasc <= 2021))',
                  ' AND (PESO < 1500) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_peso_menor_1500')
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND (ano_nasc >= 2012 AND ano_nasc <= 2021))',
                    ' AND (PESO < 1500 ) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_peso_menor_1500')
    df <- rbind(df, dataframe)
  }
}
head(df)

menos_1500 <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

menos_1500 <- menos_1500 |> rename(
  codmunres = codigo
)
df_bloco5_completo <- left_join(df_bloco5_completo, menos_1500)
df_bloco5_completo$nascidos_vivos_peso_menor_1500[which(is.na(df_bloco5_completo$nascidos_vivos_peso_menor_1500))] <- 0
# proporcao de nascidos vivos ENTRE 1500 E 1999g
df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND (ano_nasc >= 2012 AND ano_nasc <= 2021))',
                  ' AND (PESO <= 1999 AND PESO >= 1500) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_peso_1500_a_1999')
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND (ano_nasc >= 2012 AND ano_nasc <= 2021))',
                    ' AND (PESO <= 1999 AND PESO >= 1500) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_peso_1500_a_1999')
    df <- rbind(df, dataframe)
  }
}
head(df)

entre_1500_1999 <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

entre_1500_1999 <- entre_1500_1999 |> rename(
  codmunres = codigo
)
df_bloco5_completo <- left_join(df_bloco5_completo, entre_1500_1999)
df_bloco5_completo$nascidos_vivos_peso_1500_a_1999[which(is.na(df_bloco5_completo$nascidos_vivos_peso_1500_a_1999))] <- 0

# proporcao de nascidos vivos ENTRE 1500 E 1999g
df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND (ano_nasc >= 2012 AND ano_nasc <= 2021))',
                  ' AND (PESO < 2500 AND PESO >= 2000) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_peso_2000_a_2499')
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND (ano_nasc >= 2012 AND ano_nasc <= 2021))',
                    ' AND (PESO < 2500 AND PESO >= 2000) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_peso_2000_a_2499')
    df <- rbind(df, dataframe)
  }
}
head(df)

entre_2000_2499 <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

entre_2000_2499 <- entre_2000_2499 |> rename(
  codmunres = codigo
)
df_bloco5_completo <- left_join(df_bloco5_completo, entre_2000_2499)
df_bloco5_completo$nascidos_vivos_peso_2000_a_2499[which(is.na(df_bloco5_completo$nascidos_vivos_peso_2000_a_2499))] <- 0

# proporcao de nascidos vivos menos de 28 semanas
df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2012 AND ano_nasc <= 2021)',
                  ' AND (SEMAGESTAC < 28) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_menos_de_28_semanas')
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2012 AND ano_nasc <= 2021)',
                    ' AND (SEMAGESTAC < 28) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_menos_de_28_semanas')
    df <- rbind(df, dataframe)
  }
}
head(df)

menos_28_semanas <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

menos_28_semanas$nascidos_vivos_menos_de_28_semanas[which(is.na(menos_28_semanas$nascidos_vivos_menos_de_28_semanas))] <- 0
menos_28_semanas <- menos_28_semanas |> rename(
  codmunres = codigo
)
df_bloco5_completo <- left_join(df_bloco5_completo, menos_28_semanas)
df_bloco5_completo[is.na(df_bloco5_completo$nascidos_vivos_menos_de_28_semanas),'nascidos_vivos_menos_de_28_semanas'] <- 0

# proporcao de nascidos vivos ENTRE 28 e 32 semanas
df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2012 AND ano_nasc <= 2021)',
                  ' AND (SEMAGESTAC <= 32 AND SEMAGESTAC >= 28) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_28_a_32_semanas')
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2012 AND ano_nasc <= 2021)',
                    ' AND (SEMAGESTAC <= 32 AND SEMAGESTAC >= 28) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_28_a_32_semanas')
    df <- rbind(df, dataframe)
  }
}
head(df)

entre_28_e_32_semanas <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

entre_28_e_32_semanas$nascidos_vivos_28_a_32_semanas[which(is.na(entre_28_e_32_semanas$nascidos_vivos_28_a_32_semanas))] <- 0
entre_28_e_32_semanas <- entre_28_e_32_semanas |> rename(
  codmunres = codigo
)
df_bloco5_completo <- left_join(df_bloco5_completo, entre_28_e_32_semanas)
df_bloco5_completo[is.na(df_bloco5_completo$nascidos_vivos_28_a_32_semanas),'nascidos_vivos_28_a_32_semanas'] <- 0

# proporcao de nascidos vivos ENTRE 33 e 34 semanas
df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2012 AND ano_nasc <= 2021)',
                  ' AND (SEMAGESTAC <= 34 AND SEMAGESTAC >= 33) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_33_a_34_semanas')
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2012 AND ano_nasc <= 2021)',
                    ' AND (SEMAGESTAC <= 34 AND SEMAGESTAC >= 33) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_33_a_34_semanas')
    df <- rbind(df, dataframe)
  }
}
head(df)

entre_33_e_34_semanas <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

entre_33_e_34_semanas$nascidos_vivos_33_a_34_semanas[which(is.na(entre_33_e_34_semanas$nascidos_vivos_33_a_34_semanas))] <- 0
entre_33_e_34_semanas <- entre_33_e_34_semanas |> rename(
  codmunres = codigo
)
df_bloco5_completo <- left_join(df_bloco5_completo, entre_33_e_34_semanas)
df_bloco5_completo[is.na(df_bloco5_completo$nascidos_vivos_33_a_34_semanas),'nascidos_vivos_33_a_34_semanas'] <- 0

# proporcao de nascidos vivos ENTRE 35 e 36 semanas
df <- dataframe <- data.frame()

for (estado in estados){

  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_codigo_adotado, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2012 AND ano_nasc <= 2021)',
                  ' AND (SEMAGESTAC <= 36 AND SEMAGESTAC >= 35) ',
                  ' GROUP BY res_codigo_adotado, ano_nasc",
                        "fetch_size": 65000}
      }
    }')

  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_35_a_36_semanas')
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
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2012 AND ano_nasc <= 2021)',
                    ' AND (SEMAGESTAC <= 36 AND SEMAGESTAC >= 35) ',
                    ' GROUP BY res_codigo_adotado, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')


    request <- POST(url = endpoint, body = params, encode = "form")

    if (length(content(request)$rows) == 0)
      break
    else print("oi")

    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c('codigo', 'ano', 'nascidos_vivos_35_a_36_semanas')
    df <- rbind(df, dataframe)
  }
}
head(df)

entre_35_e_36_semanas <- df |>
  mutate_if(is.character, as.numeric) |>
  right_join(df_nascidos)

entre_35_e_36_semanas$nascidos_vivos_35_a_36_semanas[which(is.na(entre_35_e_36_semanas$nascidos_vivos_35_a_36_semanas))] <- 0
entre_35_e_36_semanas <- entre_35_e_36_semanas |> rename(
  codmunres = codigo
)
df_bloco5_completo <- left_join(df_bloco5_completo, entre_35_e_36_semanas)
df_bloco5_completo[is.na(df_bloco5_completo$nascidos_vivos_35_a_36_semanas),'nascidos_vivos_35_a_36_semanas'] <- 0


#CONFERINDO RESULTADO ------------

names(df_bloco5_completo)
sum(df_bloco5_completo$nascidos_vivos_com_baixo_peso) - colnames(df_bloco5_completo[,7:9])
sum(df_bloco5_completo$nascidos_vivos_prematuros) - sum(df_bloco5_completo[,10:13])

#ESCREVER O ARQUIVO ----

write.csv(df_bloco3_completo, "Indicadores dados/indicadores_bloco5_condicao_de_nascimento_2012-2021.csv",row.names=F)
