##############################################################################
# Script: 07_fetch_author_publications.R
# Propósito:
#   Descargar el historial de publicaciones de los autores.
#   LÓGICA: Caché inteligente con caducidad de 30 días.
#   Si un lote de caché es antiguo, se borra y se vuelve a descargar.
#
# Entradas:
#   - data/output/final_matches_consolidated.csv
#   - ~/.mi_api_key
#
# Salidas:
#   - Caché CRUDA en: data/interim/cache/author_pubs_raw/
#   - CSV limpio en: data/output/author_filtered_publications.csv
##############################################################################

suppressPackageStartupMessages({
  library(tidyr); library(httr2); library(jsonlite); library(dplyr); 
  library(readr); library(stringr); library(purrr); library(digest)
})

# ==============================================================================
# --- 1. CONFIGURACIÓN ---
# ==============================================================================
lineas  <- readLines("~/.mi_api_key")
api_key <- sub("API_KEY=", "", lineas)

FILE_AUTHORS <- "data/output/final_matches_consolidated.csv"
FILE_OUT     <- "data/output/author_filtered_publications.csv"

END_YEAR   <- as.numeric(format(Sys.Date(), "%Y"))
START_YEAR <- END_YEAR - 10
MAX_CACHE_AGE_DAYS <- 30

# --- DIRECTORIO DE CACHÉ (Para datos crudos) ---
DIR_CACHE <- file.path("data", "interim", "author_pubs")
if(!dir.exists(DIR_CACHE)) dir.create(DIR_CACHE, recursive = TRUE)

FILE_REGISTRY <- file.path(DIR_CACHE, "_procesados_registry.rds")

SCOPUS_SEARCH_URL <- "https://api.elsevier.com/content/search/scopus"
DOCTYPES   <- "ar OR re OR le OR bk OR ch"

# ==============================================================================
# --- 2. LIMPIEZA INTELIGENTE DE CACHÉ (NUEVO) ---
# ==============================================================================
archivos_cache <- list.files(DIR_CACHE, pattern = "^batch_raw_.*\\.rds$", full.names = TRUE)

if (length(archivos_cache) > 0) {
  info_archivos <- file.info(archivos_cache)
  edades_dias <- as.numeric(difftime(Sys.time(), info_archivos$mtime, units = "days"))
  
  archivos_caducados <- archivos_cache[edades_dias > MAX_CACHE_AGE_DAYS]
  
  if (length(archivos_caducados) > 0) {
    file.remove(archivos_caducados)
    if(file.exists(FILE_REGISTRY)) file.remove(FILE_REGISTRY)
  }
}

# ==============================================================================
# --- 3. FUNCIÓN DE DESCARGA BATCH ---
# ==============================================================================
fetch_raw_entries_for_authors <- function(au_ids) {
  
  query_authors <- paste0("AU-ID(", au_ids, ")", collapse = " OR ")
  full_query <- sprintf("(%s) AND PUBYEAR > %d AND PUBYEAR < %d AND DOCTYPE(%s)", 
                        query_authors, (START_YEAR - 1), (END_YEAR + 1), DOCTYPES)
  
  all_entries <- list()
  cursor <- "*"
  
  repeat {
    req <- request(SCOPUS_SEARCH_URL) %>%
      req_headers("X-ELS-APIKey" = api_key, "Accept" = "application/json") %>%
      # view = "COMPLETE" nos trae TODOS los campos existentes en Scopus
      req_url_query(query = full_query, count = 25, cursor = cursor, view = "COMPLETE") %>%
      req_retry(max_tries = 3, backoff = ~ 2)
    
    resp <- try(req_perform(req), silent = TRUE)
    
    if (inherits(resp, "try-error") || resp_status(resp) != 200) return(NULL)
    
    json <- resp_body_json(resp, simplifyVector = FALSE)
    sr <- json[["search-results"]]
    
    if (is.null(sr) || is.null(sr[["entry"]]) || length(sr[["entry"]]) == 0) break
    
    # Acumulamos las listas crudas
    all_entries <- c(all_entries, sr[["entry"]])
    
    next_cursor <- sr[["cursor"]][["@next"]]
    if (is.null(next_cursor) || next_cursor == cursor) break
    cursor <- next_cursor
    
    Sys.sleep(0.2) 
  }
  
  # Devolvemos la lista en bruto de todos los papers (con TODOS sus campos)
  return(all_entries)
}

# ==============================================================================
# --- 4. GESTIÓN DE PENDIENTES ---
# ==============================================================================
cat("Cargando investigadores...\n")

# separate_rows para que rompa los IDs múltiples antes de hacer la lista
df_authors <- read_csv(FILE_AUTHORS, show_col_types = FALSE) %>%
  mutate(au_id = as.character(au_id)) %>%
  separate_rows(au_id, sep = ";\\s*") %>%
  mutate(au_id = str_trim(au_id)) %>%
  filter(!is.na(au_id) & au_id != "") %>%
  distinct(au_id)

au_ids_totales <- df_authors$au_id

if (file.exists(FILE_REGISTRY) && length(list.files(DIR_CACHE, pattern="batch_raw")) > 0) {
  au_ids_procesados <- readRDS(FILE_REGISTRY)
} else {
  au_ids_procesados <- character(0)
}

au_ids_pendientes <- setdiff(au_ids_totales, au_ids_procesados)
cat("Autores pendientes de descarga:", length(au_ids_pendientes), "\n")

# ==============================================================================
# --- 4. BUCLE DE DESCARGA ---
# ==============================================================================
BATCH_SIZE <- 15

if (length(au_ids_pendientes) > 0) {
  batches <- split(au_ids_pendientes, ceiling(seq_along(au_ids_pendientes)/BATCH_SIZE))
  total_batches <- length(batches)
  
  cat("Iniciando descarga a la caché (", total_batches, " lotes )...\n", sep="")
  
  for (i in seq_along(batches)) {
    current_batch <- batches[[i]]
    
    raw_entries <- fetch_raw_entries_for_authors(current_batch)
    
    if (!is.null(raw_entries)) {
      
      # Guardamos el JSON crudo (la lista de R) en el .rds
      if (length(raw_entries) > 0) {
        batch_hash <- digest::digest(current_batch, algo = "md5")
        file_fragment <- file.path(DIR_CACHE, paste0("batch_raw_", batch_hash, ".rds"))
        saveRDS(raw_entries, file_fragment)
      }
      
      au_ids_procesados <- unique(c(au_ids_procesados, current_batch))
      saveRDS(au_ids_procesados, FILE_REGISTRY)
      
      if (i %% 5 == 0 || i == 1) {
        cat(sprintf("\rProcesando lote %d/%d (%.1f%%)...", i, total_batches, (i/total_batches)*100))
      }
    } else {
      cat(sprintf("\n  Error de conexión en lote %d. Se reintentará luego.\n", i))
    }
    Sys.sleep(0.2)
  }
  cat("\nDescarga a caché finalizada.\n")
}

# ==============================================================================
# --- 6. PARSEO DESDE LA CACHÉ ---
# ==============================================================================
cat("Parseando datos crudos desde la caché local...\n")

cache_files <- list.files(DIR_CACHE, pattern = "^batch_raw_.*\\.rds$", full.names = TRUE)

if (length(cache_files) > 0) {
  
  # Cargamos todas las listas crudas de los .rds y las juntamos en una gran lista
  all_cached_entries <- unlist(lapply(cache_files, readRDS), recursive = FALSE)
  
  # Helper para valores nulos
  `%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
  
  # Extraemos solo lo que queremos para nuestro CSV final
  df_all_pubs <- map_dfr(all_cached_entries, function(x) {
    paper_authors <- map_chr(x[["author"]] %||% list(), ~ .x[["authid"]] %||% "")
    paper_authors <- paste(paper_authors[paper_authors != ""], collapse = ";")
    
    tibble(
      eid          = x[["eid"]] %||% NA_character_,
      doi          = x[["prism:doi"]] %||% NA_character_, # El DOI sale directo de la caché cruda
      title        = x[["dc:title"]] %||% NA_character_,
      year         = str_sub(x[["prism:coverDate"]] %||% NA_character_, 1, 4),
      type         = x[["subtypeDescription"]] %||% NA_character_,
      abstract     = x[["dc:description"]] %||% NA_character_,
      keywords     = x[["authkeywords"]] %||% NA_character_, 
      
      all_au_ids   = paper_authors
    )
  })
  
  # Limpiamos y guardamos el CSV
  df_all_pubs_clean <- df_all_pubs %>%
    distinct(eid, .keep_all = TRUE) %>%
    mutate(year = as.numeric(year)) %>%
    filter(!is.na(year), year >= START_YEAR, year <= END_YEAR) %>%
    mutate(abstract = str_remove_all(abstract, "<[^>]+>"))
  
  write_csv(df_all_pubs_clean, FILE_OUT)
  
  cat("=====================================================\n")
  cat(" PARSEO COMPLETADO DESDE CACHÉ\n")
  cat("   Años abarcados:", START_YEAR, "-", END_YEAR, "\n")
  cat("   Total de publicaciones recuperadas:", nrow(df_all_pubs_clean), "\n")
  cat("   Archivo limpio guardado en:", FILE_OUT, "\n")
  cat("=====================================================\n")
  
} else {
  cat("La caché está vacía. No hay publicaciones que parsear.\n")
}
