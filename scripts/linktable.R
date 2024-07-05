library(dpm)
library(data.table)
source("scripts/utils.R")
source('C:/projects/dpm-r/R/linktable.R')
source("C:/projects/dpm-r/R/read_datapackage.R")
source("C:/projects/dpm-r/R/utils.R")


siafi_2024 <- read_datapackage("datapackages/siafi_2024/datapackage.json")
siafi_2023 <- read_datapackage("datapackages/siafi_2023/datapackage.json")
siafi <- rbind(siafi_2023$execucao, siafi_2024$execucao)
siafi <- siafi[is_criterio_obz(siafi)]


obz_2025 <- read_datapackage("datapackages/obz_2025/datapackage.json")
programacoes_anuais <- fread("datapackages/programacoes-anuais.csv")
programacoes_anuais$ano <- 2024
programacoes_anuais <- programacoes_anuais[is_criterio_obz(programacoes_anuais)]


reestimativa <- read_datapackage("datapackages/reestimativa/datapackage.json")
reest_desp <- reestimativa$reest_desp[is_criterio_obz(reestimativa$reest_desp)]

aux_classificadores <- read_datapackage("datapackages/aux_classificadores/datapackage.json")

chaves <- list(
  chave_obz = c("ano", "uo_cod", "acao_cod", "grupo_cod", "fonte_cod", "elemento_item_cod"),
  chave_acao = c("ano", "acao_cod"),
  chave_elemento = c("ano", "elemento_item_cod"),
  chave_agrupamento = c("uo_cod", "elemento_item_cod"),
  chave_uo = c("ano", "uo_cod")
)

tables <- list(
  siafi = list(
    df = siafi,
    key_name = "chave_obz",
    key_columns = chaves$chave_obz,
    drop_columns = NULL
  ),
  obz_2025 = list(
    df = obz_2025$obz,
    key_name = "chave_obz",
    key_columns = chaves$chave_obz,
    drop_columns = NULL
  ),
  programacoes_anuais = list(
    df = programacoes_anuais,
    key_name = "chave_obz",
    key_columns = chaves$chave_obz,
    drop_columns = NULL
  ),
  reestimativa = list(
    df = reest_desp,
    key_name = "chave_obz",
    key_columns = chaves$chave_obz,
    drop_columns = NULL
  )
  )

link <- create_linktable(tables)
link[, chave_acao := paste(ano, acao_cod, sep = "|")]
link[, chave_elemento := paste(ano, elemento_item_cod, sep = "|")]
link[, chave_uo := paste(ano, uo_cod, sep = "|")]

aux_classificadores$acao[, chave_acao := paste(ano, acao_cod, sep = "|")]
aux_classificadores$elemento_item[, chave_elemento := paste(ano, elemento_item_cod, sep = "|")]
aux_classificadores$uo[, chave_uo := paste(ano, uo_cod, sep = "|")]

write_aux <- function(value, name, years) {
  filepath <- glue::glue("data/aux_{name}.csv.gz")
  fwrite(value[ano %in% years], filepath, eol = "\r\n")
}

purrr::iwalk(aux_classificadores, write_aux, unique(link$ano))
fwrite(link, "data/link.csv.gz")

facts <- create_fact_tables(tables)
purrr::iwalk(facts, \(x, idx) fwrite(x, glue::glue("data/{idx}.csv.gz"), eol = "\r\n"))
