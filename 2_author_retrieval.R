##############################################################################
# Script: 02_author_retrieval.R
# Propósito:
#   Recuperar datos de Scopus Author Retrieval para cada autor (au_id)
#   identificado previamente en la tabla de autores UGR (df_au_UGR.csv).
#
# Lógica del Algoritmo:
#   1. Lee la lista maestra de IDs de autores (df_au_UGR.csv).
#   2. Comprueba qué perfiles ya tenemos descargados en CACHÉ.
#   3. Descarga SOLO los perfiles nuevos que faltan (Delta).
#   4. Procesa todos los archivos de caché (JSON) y extrae:
#       - Nombre completo (Given + Surname)
#       - ORCID
#       - Afiliación actual y Departamento
#   5. Genera la tabla enriquecida final (df_au_UGR_complete.csv).
#
# Entradas:
#   - data/interim/df_au_UGR.csv (Lista de IDs a consultar)
#   - Credenciales: ~/.mi_api_key
#
# Salidas:
#   - data/interim/df_au_UGR_complete.csv
#   - data/interim/cache/scopus/author_<au_id>.rds
##############################################################################

suppressPackageStartupMessages({
  library(httr2); library(jsonlite); library(dplyr); library(purrr)
  library(stringr); library(readr); library(tidyr)
})

# Para valores nulos/vacíos
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

# --- 1. CONFIGURACIÓN Y RUTAS ------------------------------------------------
DIR_INTERIM   <- file.path("data", "interim")
DIR_CACHE          <- file.path("data", "interim", "cache", "scopus")

# Crear directorios si no existen
dir.create(DIR_INTERIM, showWarnings = FALSE, recursive = TRUE)
dir.create(DIR_CACHE, showWarnings = FALSE, recursive = TRUE)

# Carga de Credenciales (API Key oculta)
if(!file.exists("~/.mi_api_key")) stop("ERROR: Falta el archivo ~/.mi_api_key")
api_key <- sub("API_KEY=", "", readLines("~/.mi_api_key", warn=FALSE)[1])
inst_token <- NA

# --- Identificadores AF-ID UGR y regex de filtrado ---------------------------
ugr_ids <- c("60027844","60208599","60030188","60110413","60231070",
             "60189794","60086948","60014655","60016224","60003487")
regex_ugr <- regex(paste(ugr_ids, collapse="|"))

# Carga de TABLA 1: df_au_UGR (autores UGR)
au_path <- file.path(DIR_INTERIM, "df_au_UGR.csv")
stopifnot(file.exists(au_path))

# Leemos y aseguramos que el ID sea texto (para no perder ceros a la izquierda)
df_au_UGR <- suppressMessages(read.csv2(au_path, stringsAsFactors = FALSE) |> 
                                mutate(au_id=as.character(au_id)))

# ==============================================================================
# --- 2. MOTOR DE DESCARGA (API) ---
# ==============================================================================
AUTHOR_RETRIEVAL_URL <- "https://api.elsevier.com/content/author/author_id"

# Función que descarga la ficha de un autor
fetch_author_info <- function(au_id, sleep_s=0.2){
  url <- paste0(AUTHOR_RETRIEVAL_URL, "/", au_id)
  
  # Pedimos vista específica para tener todos los metadatos (ORCID, Dept...)
  req <- request(url) |>
    req_headers("X-ELS-APIKey"=api_key, "Accept"="application/json") |>
    req_url_query(view="ENHANCED")
  
  # Reintentos por si falla la red momentáneamente
  for(i in 1:3){
    resp <- try(req_perform(req), silent=TRUE)
    if(!inherits(resp,"try-error")){
      # Si OK (200) -> Devolvemos datos (el JSON parseado)
      if (resp_status(resp) == 200L){ 
        Sys.sleep(sleep_s)
        return(resp_body_json(resp, simplifyVector=FALSE)) 
      }
      # Si NO EXISTE (404) -> Devolvemos NULL explícito para guardar el error
      if (resp_status(resp) == 404L) return(list(error="404")) 
    }
    # Si falla la conexión, espera y reintenta
    Sys.sleep(1) 
  }
  return(NULL) # Fallo de red persistente
}

# ==============================================================================
# --- 3. LÓGICA DE DESCARGA ---
# ==============================================================================
# Objetivo: No descargar lo que ya tenemos descargado. 

# Lista total de IDs que necesitamos
all_ids <- unique(as.character(df_au_UGR$au_id))

# Funciones para gestionar caché
get_cache_path <- function(id) file.path(DIR_CACHE, paste0("author_", id, ".rds"))
is_cached      <- function(id) file.exists(get_cache_path(id))

# Calculamos la lista de IDs pendientes
pending_ids <- all_ids[!vapply(all_ids, is_cached, logical(1))]

cat("------------------------------------------------\n")
cat("Total Autores:      ", length(all_ids), "\n")
cat("Ya en caché:        ", length(all_ids) - length(pending_ids), "\n")
cat("Nuevos a descargar: ", length(pending_ids), "\n")
cat("------------------------------------------------\n")

# ==============================================================================
# --- 4. BUCLE DE DESCARGA ---
# ==============================================================================
if (length(pending_ids) > 0) {
  cat("Descargando nuevos...\n")
  pb <- txtProgressBar(min = 0, max = length(pending_ids), style = 3)
  
  for (i in seq_along(pending_ids)) {
    au <- pending_ids[i]
    
    # Llamada a la API
    data <- fetch_author_info(au)
    
    # GUARDADO EN DISCO:
    # Guardamos siempre, incluso si es un error 404, para no volver a 
    # intentarlo infinitamente en futuras ejecuciones.
    if (!is.null(data)) {
      saveRDS(data, get_cache_path(au))
    } else {
      # Si falló la red 3 veces, guardamos un error temporal
      saveRDS(list(error="NetworkFail"), get_cache_path(au))
    }
    setTxtProgressBar(pb, i)
  }
  close(pb)
} else {
  cat("Todo actualizado. No hace falta descargar nada.\n")
}

# ==============================================================================
# --- 5. CONSOLIDACIÓN ---
# ==============================================================================
cat("\nGenerando tabla...\n")

# Función para extraer datos limpios de la estructura del JSON
parse_author_info <- function(json, au_id){
  # Si el archivo tiene error guardado (404 o fallo red), fila vacía
  if (is.null(json) || !is.null(json$error)) {
    return(tibble(au_id, given_name=NA, surname=NA, orcid=NA, affiliation_current=NA, department=NA))
  }
  
  # Navegación por el árbol del JSON
  ar <- json$`author-retrieval-response`[[1]]
  prof <- ar$`author-profile`
  
  # Extracción segura (%||% NA)
  given <- prof$`preferred-name`$`given-name` %||% NA
  surn  <- prof$`preferred-name`$`surname`    %||% NA
  orcid <- ar$coredata$orcid %||% prof$orcid  %||% NA
  
  # Lógica para encontrar el Departamento actual
  aff_node <- prof$`affiliation-current`$affiliation
  if (is.list(aff_node) && is.null(aff_node$`ip-doc`) && length(aff_node)>0) aff_node <- aff_node[[1]]
  
  aff_name <- NA; dept <- NA; afid_curr <- ar$`affiliation-current`$`@id` %||% NA
  if (!is.null(aff_node) && !is.null(aff_node$`ip-doc`)) {
    ip <- aff_node$`ip-doc`
    aff_name <- ip$afdispname %||% NA
    dept <- aff_node$department %||% ip$`org-division` %||% NA
    afid_curr <- aff_node$`@affiliation-id` %||% afid_curr
  }
  
  # Construcción de la fila
  tibble(
    au_id = au_id,
    given_name = as.character(given),
    surname = as.character(surn),
    initials = as.character(prof$`preferred-name`$`initials` %||% NA),
    orcid = as.character(orcid),
    affiliation_current = as.character(afid_curr),
    affiliation_name = as.character(aff_name),
    department = as.character(dept),
    email = NA_character_ # para futuro relleno
  )
}

# Leemos TODOS los archivos de caché (viejos y nuevos)
cached_files <- list.files(DIR_CACHE, pattern="^author_.*\\.rds$", full.names=TRUE)
cached_ids   <- sub("author_(.*)\\.rds", "\\1", basename(cached_files))

# Aplicamos parseo a todos los archivos
df_parsed <- map2_dfr(cached_files, cached_ids, ~parse_author_info(readRDS(.x), .y))

# Unimos los metadatos a la tabla original de autores
df_final <- df_au_UGR %>%
  mutate(au_id = as.character(au_id)) %>%
  left_join(df_parsed, by="au_id")

# 4. Guardado final
write.csv2(df_final, file.path(DIR_INTERIM, "df_au_UGR_complete.csv"), row.names=FALSE)

cat("   - Archivo generado:", file.path(DIR_INTERIM, "df_au_UGR_complete.csv"), "\n")