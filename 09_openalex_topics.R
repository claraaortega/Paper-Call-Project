##############################################################################
# Script: 09_openalex_topics.R
# Propósito: 
#   Enriquecer la tabla de publicaciones (df_pub_comp.csv) obteniendo los
#   "Topics" (temas de investigación) desde la API de OpenAlex.
#
# Lógica:
#   1. Lee los DOIs de nuestros papers.
#   2. Limpia los DOIs (quita https, doi:, espacios) para estandarizarlos.
#   3. Consulta la API de OpenAlex por lotes (50 DOIs a la vez) para ser rápido.
#   4. Extrae los Topics con alta confianza (score > 0.99).
#   5. Guarda los resultados en CACHÉ (topics_cache.rds) normalizada.
#
# Entradas:
#   - data/interim/df_pub_comp.csv (Lista de papers UGR)
#   - ~/.openalex_mail (Correo para usar el 'Polite Pool' de la API)
#
# Salidas:
#   - data/interim/df_pub_UGR_with_topics.csv (Tabla enriquecida final)
#   - data/interim/openalex_topics.csv (Tabla auxiliar solo topics)
##############################################################################

suppressPackageStartupMessages({
  library(httr2); library(dplyr); library(purrr); library(tidyr)
  library(jsonlite); library(stringr); library(digest); library(readr)
})

# --- 1. CONFIGURACIÓN ---
DIR_INTER     <- file.path("data", "interim")
DIR_CACHE_OA  <- file.path("data", "interim", "cache_openalex") 

# Crear directorios
dir.create(DIR_INTER,    showWarnings = FALSE, recursive = TRUE)
dir.create(DIR_CACHE_OA, showWarnings = FALSE, recursive = TRUE)

# Rutas de Archivos
FILE_PUBS    <- file.path(DIR_INTER, "df_pub_comp_ENRICHED.csv")
OUT_ENRICHED <- file.path(DIR_INTER, "df_pub_UGR_with_topics.csv")
OUT_TOPICS   <- file.path(DIR_INTER, "openalex_topics.csv")
CACHE_RDS    <- file.path(DIR_CACHE_OA, "topics_cache.rds")

# Credencial OpenAlex
OPENALEX_MAIL_FILE <- "~/.openalex_mail"
if (!file.exists(OPENALEX_MAIL_FILE)) stop("ERROR: Falta ~/.openalex_mail")
OPENALEX_MAIL <- sub("OPENALEX_MAIL=", "", readLines(OPENALEX_MAIL_FILE))

# ==============================================================================
# --- 2. FUNCIONES ---
# ==============================================================================

# Para valores nulos
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

# Normalizar DOIs
# Convierte "https://doi.org/10.1000/1" -> "10.1000/1"
# Convierte "DOI: 10.1000/1 " -> "10.1000/1"
norm_doi <- function(doi) {
  if (length(doi) == 0) return(character(0))
  d <- tolower(trimws(doi)) # Minúsculas y quitar espacios
  d[d %in% c("", "na")] <- NA_character_
  
  # Eliminar prefijos comunes para dejar solo el DOI limpio (10.xxx/yyy)
  d <- sub("^https?://(dx\\.)?doi\\.org/", "", d)
  d <- sub("^doi:", "", d)
  d
}

# Extractor de nombres
safe_dn <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_character_)
  if (is.list(x) && !is.null(x$display_name)) return(as.character(x$display_name))
  if (is.atomic(x)) return(as.character(x[1]))
  NA_character_
}

# Parsea la respuesta JSON de OpenAlex
parse_topics_simple <- function(work) {
  # Fila vacía por defecto
  empty_row <- tibble(doi = NA_character_, oa_id = NA_character_, hs_topics_all = NA_character_)
  if (!is.list(work) || is.null(names(work))) return(empty_row)
  
  # Extraemos IDs
  res <- list(
    doi = norm_doi(work$doi %||% NA_character_),
    oa_id = work$id %||% NA_character_,
    hs_topics_all = NA_character_
  )
  
  # Procesamos Topics
  to <- work$topics
  if (!is.null(to) && length(to) > 0) {
    if (is.list(to) && !is.list(to[[1]])) to <- list(to)
    
    # Filtro: solo topics con >99% de confianza
    topics_filtered <- purrr::keep(to, ~ (as.numeric(.x$score %||% 0) > 0.99))
    if (length(topics_filtered) > 0) {
      # Ordenar por puntuación descendente
      scores <- vapply(topics_filtered, function(x) as.numeric(x$score %||% 0), numeric(1))
      to_hs <- topics_filtered[order(-scores)]
      # Formatear string: "Inteligencia Artificial (0.99; CS; AI; ...)"
      labels <- map_chr(to_hs, function(t) {
        sprintf("%s (%.3f; %s; %s; %s)", t$display_name %||% "NA", as.numeric(t$score %||% 0),
                safe_dn(t$subfield), safe_dn(t$field), safe_dn(t$domain))
      })
      res$hs_topics_all <- paste(labels, collapse = " | ")
    }
  }
  as_tibble(res)
}

# ==============================================================================
# --- 3. GESTIÓN DE CACHÉ ---
# ==============================================================================
cat("Verificando caché...\n")

if (file.exists(CACHE_RDS)) {
  topics_cache <- readRDS(CACHE_RDS)
  
  # Validación de estructura (por si es una caché vieja incompatible)
  if ("primary_topic_name" %in% names(topics_cache)) {
    message("Caché antigua. Reiniciando.")
    topics_cache <- tibble(doi = character(), oa_id = character(), hs_topics_all = character())
  } else {
    # Normalización de la caché
    topics_cache <- topics_cache %>%
      mutate(doi = norm_doi(doi)) %>%
      filter(!is.na(doi), doi != "") %>%
      distinct(doi, .keep_all = TRUE)
    
    message("Caché cargada y normalizada: ", nrow(topics_cache), " topics.")
  }
} else {
  topics_cache <- tibble(doi = character(), oa_id = character(), hs_topics_all = character())
}

# ==============================================================================
# --- 4. PREPARACIÓN DE PUBLICACIONES ---
# ==============================================================================
cat("Cargando publicaciones...\n")
df_pub_comp <- read_csv2(FILE_PUBS, show_col_types = FALSE)

# Limpieza inicial de la tabla de entrada
df_with_doi <- df_pub_comp %>%
  mutate(
    eid = as.character(eid),
    doi_clean = norm_doi(doi), # Normalizamos aquí también
    email = na_if(tolower(trimws(email)), "")
  ) %>%
  filter(!is.na(doi_clean)) %>%
  distinct(eid, .keep_all = TRUE)

cat(" -> Publicaciones con DOI válido:", nrow(df_with_doi), "\n")

# ==============================================================================
# --- 5. DESCARGA ---
# ==============================================================================
# Calculamos qué nos falta
dois_all     <- unique(df_with_doi$doi_clean)
dois_cached  <- unique(topics_cache$doi)
dois_pending <- setdiff(dois_all, dois_cached)
 
cat(" -> En caché:", length(dois_cached), "| Pendientes:", length(dois_pending), "\n")

if (length(dois_pending) > 0) {

  fetch_batch_openalex <- function(batch_dois, ua) {
    
    filter_str <- paste(batch_dois, collapse = "|")
    url <- paste0("https://api.openalex.org/works?filter=doi:", URLencode(filter_str, reserved = TRUE),
                  "&per-page=", length(batch_dois), "&select=id,doi,topics")
    # Llamada a la API con User-Agent
    resp <- tryCatch({ 
      request(url) |> 
        req_user_agent(sprintf("UGR-Topics/1.0 (%s)", ua)) |> 
        req_timeout(60) |> 
        req_perform() 
      }, error = function(e) NULL)
    
    if (is.null(resp) || resp_status(resp) != 200) return(NULL)
    
    json <- tryCatch({ resp_body_json(resp, simplifyVector = FALSE) }, error = function(e) NULL)
    if (is.null(json) || is.null(json$results)) return(NULL)
    
    res <- json$results
    if (length(res) && !is.list(res[[1]])) res <- list(res)
    res
  }
  
  # Dividimos los pendientes en lotes de 50 (límite recomendado OpenAlex)
  batches <- split(dois_pending, ceiling(seq_along(dois_pending) / 50))
  n_batches <- length(batches)
  
  cat("Descargando", n_batches, "lotes de OpenAlex...\n")
  pb <- txtProgressBar(min = 0, max = n_batches, style = 3, width = 60)
  
  for (i in seq_along(batches)) {
    works <- fetch_batch_openalex(batches[[i]], OPENALEX_MAIL)
    
    if (!is.null(works) && length(works) > 0) {
      new_topics <- map_dfr(works, parse_topics_simple)
      
      # Añadimos lo nuevo a la caché y guardamos inmediatamente
      topics_cache <- bind_rows(topics_cache, new_topics) %>%
        mutate(doi = norm_doi(doi)) %>% 
        filter(!is.na(doi), doi != "") %>%
        distinct(doi, .keep_all = TRUE)
      
      saveRDS(topics_cache, CACHE_RDS)
    }
    setTxtProgressBar(pb, i)
    Sys.sleep(0.2) # Pausa
  }
  close(pb)
} else {
  cat("Todo actualizado.\n")
}

# ==============================================================================
# --- 6. UNIÓN Y GUARDADO ---
# ==============================================================================
cat("Guardando tablas finales...\n")

# Aseguramos que la caché tenga las columnas necesarias aunque esté vacía
for (col in c("doi", "oa_id", "hs_topics_all")) if (!col %in% names(topics_cache)) topics_cache[[col]] <- NA

# JOIN FINAL: Papers UGR + Topics
df_pub_enriched <- df_with_doi %>%
  left_join(topics_cache, by = c("doi_clean" = "doi")) %>%
  filter(!is.na(hs_topics_all) & hs_topics_all != "")

write_csv2(df_pub_enriched, OUT_ENRICHED)
write_csv2(topics_cache, OUT_TOPICS)

cat("\n PROCESO FINALIZADO.\n")
cat("   - Papers enriquecidos:", nrow(df_pub_enriched), "\n")
cat("   - Archivo:", OUT_ENRICHED, "\n")
