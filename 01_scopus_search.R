##############################################################################
# Script: 01_scopus_search.R
# Propósito:
#   Consultar la API de Scopus (Search) para obtener todas las publicaciones
#   afiliadas a la UGR en el rango 2015-2024.
#
# Lógica:
#   1. Define la Query de búsqueda (AF-ID de la UGR + Años).
#   2. Consulta la API página a página y descarga los resultados.
#   3. Gestiona CACHÉ (guarda los JSON) para no repetir consultas caras.
#   4. Transforma los datos JSON en tablas CSV estructuradas:
#       - df_completo_API.csv (Bruto)
#       - df_au_UGR.csv (Lista de autores UGR)
#       - df_pub_comp.csv (Lista de publicaciones)
#   5. Inyecta los emails recuperados manualmente (Script 00).
#
# Entradas:
#   - Credenciales: ~/.mi_api_key
#   - Emails Manuales: data/interim/df_eids_email.csv (Output del Script 00)
#
# Salidas:
#   - data/interim/df_au_UGR.csv
#   - data/interim/df_pub_comp.csv
##############################################################################

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(purrr)
  library(rlang)
  library(digest)
  library(readr)
})

# --- 1. CONFIGURACIÓN Y CREDENCIALES ---

# Carga de API Key desde archivo oculto
lineas  <- readLines("~/.mi_api_key")
api_key <- sub("API_KEY=", "", lineas)
Sys.setenv(API_KEY = api_key)
inst_token <- NA

# Rango temporal (AJUSTABLE)
END_YEAR   <- as.numeric(format(Sys.Date(), "%Y"))
START_YEAR <- END_YEAR - 10
years <- START_YEAR:END_YEAR

# IDs de afiliación (AF-ID) que definen UGR en Scopus
ugr_ids <- c("60027844","60208599","60030188","60110413","60231070",
             "60189794","60086948","60014655","60016224","60003487")

# Construcción de la Query (OR lógico de todos los IDs)
affils    <- paste0("AF-ID(", ugr_ids, ")", collapse = " OR ")
regex_ugr <- regex(paste(ugr_ids, collapse = "|"))

# Campos a solicitar en Search
SCOPUS_SEARCH_URL <- "https://api.elsevier.com/content/search/scopus"
FIELDS <- "eid,prism:doi,dc:title,prism:coverDate,author,affiliation,dc:description,authkeywords"

# Generadores de Query (Segmentamos por año y tipo de acceso)
# 1. Incluye Affils UGR + Open Access
# 2. Incluye Affils UGR + NO Open Access
# 3. Excluye doctype articulo + Open Access
# 4. Excluye doctype articulo + NO Open Access
q_inc_inc <- function(y) sprintf("(%s) AND PUBYEAR = %d AND DOCTYPE(ar) AND OA(all)", affils, y)
q_inc_exc <- function(y) sprintf("(%s) AND PUBYEAR = %d AND DOCTYPE(ar) AND NOT OA(all)", affils, y)
q_exc_inc <- function(y) sprintf("(%s) AND PUBYEAR = %d AND DOCTYPE(re OR le OR bk OR ch) AND OA(all)", affils, y)
q_exc_exc <- function(y) sprintf("(%s) AND PUBYEAR = %d AND DOCTYPE(re OR le OR bk OR ch) AND NOT OA(all)", affils, y)

# Configuración de Rutas
DIR_INTERIM <- file.path("data", "interim")
DIR_CACHE   <- file.path("data", "interim", "cache", "scopus")
# Crear directorios
dir.create(DIR_INTERIM, showWarnings = FALSE, recursive = TRUE)
dir.create(DIR_CACHE, showWarnings = FALSE, recursive = TRUE)

# ==============================================================================
# --- FUNCIONES DE CACHÉ Y API ---
# ==============================================================================

# Genera un nombre de archivo único basado en la búsqueda
# Si la query es la misma, el nombre del archivo será el mismo.
cache_filename <- function(query, year = NULL) {
  hash <- digest::digest(query, algo = "md5")
  prefix <- if (!is.null(year)) paste0("year_", year, "_") else ""
  file.path(DIR_CACHE, paste0(prefix, hash, ".rds"))
}

# Si ya lo bajamos, lee del disco. Si no, llama a la API.
fetch_all_entries_cached <- function(query, count = 25, force_refresh = FALSE) {
  cache_file <- cache_filename(query)
  if (file.exists(cache_file) && !force_refresh) {
    message("Cargando desde caché: ", basename(cache_file))
    return(readRDS(cache_file))
  }
  message("Descargando desde API (Search)...")
  entries <- fetch_all_entries(query, count)
  saveRDS(entries, cache_file)
  message("Guardado en caché: ", cache_file)
  entries
}

# --- 2) HTTP Search ----------------------------------------------------------
# Prepara la request con cabeceras, querystring y campos
make_req <- function(query, cursor = "*", count = 25, fields = FIELDS, view = "COMPLETE") {
  req <- request(SCOPUS_SEARCH_URL) |>
    req_headers("X-ELS-APIKey" = api_key, "Accept" = "application/json") |>
    req_url_query(query = query, count = count, cursor = cursor, view = view, field = fields)
  if (!is.na(inst_token)) req <- req_headers(req, "X-ELS-Insttoken" = inst_token)
  req
}

# Descarga una página con reintentos si falla la red y sleep
fetch_one_page <- function(query, cursor = "*", sleep_s = 0.2) {
  req <- make_req(query = query, cursor = cursor)
  for (i in 1:5) {
    resp <- try(req_perform(req), silent = TRUE)
    if (!inherits(resp, "try-error") && resp_status(resp) == 200L) {
      json <- resp_body_json(resp, simplifyVector = FALSE)
      Sys.sleep(sleep_s) # Respetamos límites de velocidad de la API
      return(json)
    }
    Sys.sleep(min(60, 2^(i-1))) # Espera exponencial (1s, 2s, 4s...)
  }
  abort("No se pudo obtener la página tras reintentos (Search).")
}

# Bucle principal de paginación usando cursores (Scopus usa cursores (next/prev) 
# en lugar de número de página)
fetch_all_entries <- function(query, count = 25) {
  cursor <- "*"; all_entries <- list(); total <- NA_integer_; page <- 0L
  repeat {
    page <- page + 1L; message(sprintf("Query (página %d)...", page))
    json <- fetch_one_page(query = query, cursor = cursor)
    sr <- json[["search-results"]]; if (is.null(sr)) break
    if (is.na(total)) total <- as.integer(sr[["opensearch:totalResults"]] %||% NA)
    entries <- sr[["entry"]]; if (is.null(entries) || !length(entries)) break
    all_entries <- c(all_entries, entries)
    next_cursor <- sr[["cursor"]][["@next"]]
    if (is.null(next_cursor) || is.na(next_cursor) || next_cursor == cursor) break
    cursor <- next_cursor
  }
  all_entries
}

# --- 3) Parseo entry -> filas ----------------------------------------------
# Extrae IDs de afiliación de un autor específico dentro del JSON
extract_author_afids <- function(author_obj) {
  afid <- author_obj[["afid"]]; if (is.null(afid)) return(character(0))
  if (is.list(afid)) as.character(unlist(afid, use.names = FALSE)) else as.character(afid)
}

# Convierte el JSON anidado en formato largo
# -> Paper con 5 autores : genera 5 filas.
entry_to_rows <- function(entry) {
  eid        <- as.character(entry[["eid"]] %||% NA_character_)
  doi        <- as.character(entry[["prism:doi"]] %||% NA_character_)
  title      <- as.character(entry[["dc:title"]] %||% NA_character_)
  cover_date <- as.character(entry[["prism:coverDate"]] %||% NA_character_)
  subtype    <- as.character(entry[["subtype"]] %||% NA_character_)
  abstract <- as.character(entry[["dc:description"]] %||% NA_character_)
  if(!is.na(abstract)) abstract <- str_remove_all(abstract, "<[^>]+>") 
  keywords <- as.character(entry[["authkeywords"]] %||% NA_character_)
  
  authors <- entry[["author"]]; if (is.null(authors)) authors <- list()
  map_dfr(authors, function(a){
    au_id <- as.character(a[["authid"]] %||% NA_character_)
    name  <- as.character(a[["authname"]] %||% NA_character_)
    afids <- extract_author_afids(a)
    # Añadimos abstract y keywords al tibble
    if (!length(afids)) {
      tibble(eid, name, au_id, affil_id = NA_character_,
             doi, title, cover_date, subtype, abstract, keywords)
    } else {
      tibble(eid=rep(eid,length(afids)), name=rep(name,length(afids)),
             au_id=rep(au_id,length(afids)), affil_id=as.character(afids),
             doi=rep(doi,length(afids)), title=rep(title,length(afids)),
             cover_date=rep(cover_date,length(afids)), subtype=rep(subtype,length(afids)),
             abstract=rep(abstract,length(afids)), keywords=rep(keywords,length(afids)))
    }
  })
}

# Aplicamos la conversión a toda la lista de resultados
entries_to_long_df <- function(entries) if (!length(entries)) tibble() else map_dfr(entries, entry_to_rows)

# --- 4) Descarga por año (desde caché) y normalización ------------------------
# Ejecuta las 4 queries por año, concatena, quita duplicados y etiqueta Year
fetch_year_df <- function(y, force_refresh = FALSE) {

  anio_actual <- as.numeric(format(Sys.Date(), "%Y"))
  if (y >= (anio_actual - 1)) force_refresh <- TRUE
  
  queries <- list(q_inc_inc(y), q_inc_exc(y), q_exc_inc(y), q_exc_exc(y))
  parts <- map(queries, function(q) {
    entries <- fetch_all_entries_cached(query = q, force_refresh = force_refresh)
    entries_to_long_df(entries)
  })
  bind_rows(parts) %>%
    distinct() %>%
    distinct(eid, au_id, affil_id, .keep_all = TRUE) %>%
    mutate(Year = y)
}

# --- 5) Pipeline principal: sólo caché de Search ----------------------------
FORCE_REFRESH_SEARCH <- FALSE  # Poner a TRUE para forzar re-descarga de todo
df <- map_dfr(years, ~fetch_year_df(.x, FORCE_REFRESH_SEARCH)) %>%
  distinct() %>%
  distinct(eid, au_id, affil_id, .keep_all = TRUE)

# Limpieza rápida de basura que a veces devuelve la API en affil_id ("true", "1")
df <- df %>% filter(!tolower(coalesce(affil_id, "")) %in% c("true", "t", "1")) %>%
  distinct(eid, au_id, affil_id, .keep_all = TRUE)

# Guardamos la tabla completa en bruto
write_csv2(df, file.path(DIR_INTERIM, "df_completo_API.csv"))

# --- 6) Construcción de TABLAS intermedias ----------------------------------
# Aquí separamos según nos interesa (afiliado a nuestra universidad) y lo que no.

# Separación inicial
df_not_UGR <- df |> filter(!str_detect(coalesce(affil_id, ""), regex_ugr))

# TABLA DE PUBLICACIONES (df_pub_UGR): Agrupamos por EID
df_pub_UGR <- df |>
  filter(str_detect(coalesce(affil_id, ""), regex_ugr)) |>
  group_by(eid) |>
  summarise(
    doi      = first(na.omit(doi)),
    title    = first(na.omit(title)),
    Year     = first(na.omit(Year)),
    abstract = first(na.omit(abstract)), # Guardamos el abstract
    keywords = first(na.omit(keywords)), # Guardamos las keywords
    au_id    = paste(unique(au_id), collapse=";"), 
    name     = paste(unique(name), collapse=";"), 
    .groups="drop"
  )

# TABLA DE AUTORES (df_au_UGR): Agrupamos por Author ID
df_au_UGR <- df |>
  filter(str_detect(coalesce(affil_id, ""), regex_ugr)) |>
  select(name, eid, au_id) |>
  group_by(au_id) |>
  summarise(name = first(name), eid = paste(unique(eid), collapse=";"), .groups="drop")

# Lógica de Recuperación
# A veces un autor es de la univerdad, pero en una fila específica del CSV bruto no sale marcado como tal.
# Aquí buscamos esos casos perdidos cruzando IDs.
ref_ids <- df_au_UGR |> transmute(au_id=as.character(au_id)) |> filter(!is.na(au_id), au_id!="") |> distinct() |> pull(au_id)

df_perdidas <- df_not_UGR |>
  mutate(au_id = as.character(au_id)) |>
  separate_rows(au_id, sep=";\\s*") |>
  mutate(au_id = str_trim(au_id)) |>
  filter(!is.na(au_id), au_id!="") |>
  mutate(tiene_UGR = au_id %in% ref_ids)

df_recuperadas_eid <- df_perdidas |> filter(tiene_UGR) |> select(eid, doi, au_id, title, Year)

# Unión Final de Publicaciones (directas + recuperadas)
df_pub_comp <- bind_rows(
  df_pub_UGR |> select(eid, doi, au_id, title, Year, abstract, keywords),
  df_recuperadas_eid |> mutate(abstract = NA_character_, keywords = NA_character_)
) |>
  group_by(eid) |>
  summarise(
    doi      = first(na.omit(doi)),
    title    = first(na.omit(title)),
    year     = first(na.omit(Year)),
    abstract = first(na.omit(abstract)),
    keywords = first(na.omit(keywords)),
    au_id    = paste(unique(au_id), collapse=";"),
    .groups="drop"
  )

# --- 7) Join con emails locales (corresponding authors) ---------------------
# Lee el output del Script 00 (data/interim/df_eids_email.csv)
# Si existe, añade el email al paper correspondiente.
path_emails_locales <- file.path(DIR_INTERIM, "df_eids_email.csv")

if (file.exists(path_emails_locales)) {
  cat(paste("\nEncontrado archivo local:", path_emails_locales))
  tryCatch({
    df_emails_locales <- readr::read_csv(path_emails_locales)
    if ("EID" %in% names(df_emails_locales)) {
      df_emails_to_join <- df_emails_locales %>%
        rename(
          eid = EID,
          email = correo,
          autor_corr_local = corresp_auth
        ) %>%
        select(eid, autor_corr_local, email) %>%
        distinct(eid, .keep_all = TRUE)
      # JOIN FINAL
      df_pub_comp <- df_pub_comp %>%
        left_join(df_emails_to_join, by = "eid")

      cat("-> Join completado. Añadidas columnas 'autor_corr_local' y 'email'.\n")

    } else {
      warning(paste("El archivo", path_emails_locales, "existe, pero no contiene la columna 'EID'. No se pudo unir."))
    }

  }, error = function(e) {
    warning(paste("Error al leer", path_emails_locales, ":", e$message))
  })

} else {
  warning(paste("No se encontró el archivo:", path_emails_locales, ". 'df_pub_comp' no tendrá datos de email."))
}

# --- 8) Guardar intermedios ---------------------------------------------
write_csv2(df_au_UGR,  file.path(DIR_INTERIM, "df_au_UGR.csv"))
write_csv2(df_pub_comp, file.path(DIR_INTERIM, "df_pub_comp.csv"))

# --- 9) Resumen final -------------------------------------------------
cat("\nPROCESO FINALIZADO.\n")
cat("   - Autores UGR detectados:", nrow(df_au_UGR), "\n")
cat("   - Publicaciones procesadas:", nrow(df_pub_comp), "\n")
