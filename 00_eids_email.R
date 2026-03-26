##############################################################################
# Script: 00_eids_email.R 
# Propósito:
#   1. Procesa las descargas manuales de Scopus con los autores de 
#      correspondencia. 
#   2. Guarda un fichero en el que, para cada publicación, pone el nombre
#      del autor y su correo (df_eids_email); y otro (emails_by_auid) en el 
#      que asigna dicho correo y nombre al au_id correspondiente.
##############################################################################

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr)
  library(stringr); library(purrr); library(stringi)
})

# --- 1. CONFIGURACIÓN ---
DIR_INPUT  <- file.path("data", "input", "ScopusManual")
DIR_INTER  <- file.path("data", "interim")
dir.create(DIR_INTER, showWarnings = FALSE, recursive = TRUE)

OUT_EIDS <- file.path(DIR_INTER, "df_eids_email.csv")  
OUT_AUID <- file.path(DIR_INTER, "emails_by_auid.csv") 

INPUT_FILES <- list.files(DIR_INPUT, pattern = "\\.csv$", full.names = TRUE)
if(length(INPUT_FILES) == 0) stop("ERROR: No se encontraron archivos CSV.")

cat("Leyendo", length(INPUT_FILES), "ficheros CSV de Scopus manuales...\n")

# ==============================================================================
# --- 2. FUNCIONES ---
# ==============================================================================
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

simple_norm <- function(x) {
  x <- tolower(x %||% "")
  x <- stringi::stri_trans_general(x, "Latin-ASCII")
  x <- str_replace_all(x, "[\\.,;:_()\\-]+", " ")
  str_squish(x)
}


generate_name_variants <- function(raw_name) {
  if (is.na(raw_name) || raw_name == "") return(character(0))
  
  # Arreglo de errores de codificación (UTF-8 / Latin)
  x <- str_replace_all(raw_name, "RuA-z", "Ruiz")
  x <- str_replace_all(x, "RuÃ­z", "Ruiz")
  x <- str_replace_all(x, "RuA\\+z", "Ruiz")
  
  x_clean <- tolower(stringi::stri_trans_general(x, "Latin-ASCII"))
  base_norm <- str_squish(str_replace_all(x_clean, "[\\.,;:_()\\-]+", " "))
  variants <- c(base_norm)
  
  surname <- ""
  given <- ""
  
  if (str_detect(x_clean, ",")) {
    parts <- str_split(x_clean, ",")[[1]]
    if (length(parts) >= 2) {
      surname <- str_squish(str_replace_all(parts[1], "[\\.,;:_()\\-]+", " "))
      given <- str_squish(str_replace_all(parts[2], "[\\.,;:_()\\-]+", " "))
    }
  } else {
    # Si Scopus no puso coma, asumimos que lo último es el apellido
    toks <- str_split(base_norm, "\\s+")[[1]]
    if (length(toks) >= 2) {
      surname <- toks[length(toks)]
      given <- paste(toks[-length(toks)], collapse = " ")
    }
  }
  
  # 3. CREAR LAS COMBINACIONES EXACTAS (Ej: "j r ruiz")
  if (surname != "" && given != "") {
    variants <- c(variants, paste(given, surname), paste(surname, given))
    
    g_toks <- str_split(given, "\\s+")[[1]]
    g_toks <- g_toks[g_toks != ""]
    
    if (length(g_toks) > 0) {
      # Extraer iniciales (ej. Jonatan -> j)
      g_inis <- paste(substr(g_toks, 1, 1), collapse = " ")
      variants <- c(variants, paste(g_inis, surname), paste(surname, g_inis))
      
      # Iniciales pegadas (ej. jr ruiz)
      g_inis_concat <- paste(substr(g_toks, 1, 1), collapse = "")
      variants <- c(variants, paste(g_inis_concat, surname), paste(surname, g_inis_concat))
      
      # Solo la primera inicial (ej. j ruiz)
      first_ini <- substr(g_toks[1], 1, 1)
      variants <- c(variants, paste(first_ini, surname), paste(surname, first_ini))
    }
  }
  return(unique(variants))
}

# --- VALIDACIÓN DE CORREO ---
is_valid_email_pair <- function(email, name_norm) {
  if (is.na(email) || is.na(name_norm)) return(FALSE)
  
  # Limpieza del prefijo del email
  prefix <- str_replace_all(str_remove(email, "@.*$"), "[^a-z]", "")
  if (prefix == "") return(TRUE) 
  
  # Tokenización
  tokens <- str_split(name_norm, "\\s+")[[1]]
  
  tokens <- str_replace_all(tokens, "[^a-z]", "")
  
  tokens <- tokens[nchar(tokens) > 0]
  if (length(tokens) == 0) return(TRUE)
  
  tokens_validos <- tokens[nchar(tokens) >= 3]
  
  # PRUEBA A: Token largo contenido en el prefijo o viceversa
  if (length(tokens_validos) > 0) {
    if (any(str_detect(prefix, tokens_validos)) || 
        (nchar(prefix) >= 5 && any(str_detect(tokens_validos, prefix)))) return(TRUE)
  }
  
  # PRUEBA B: Iniciales exactas o contenidas (ej. 'jmt' en 'jmt123')
  initials <- paste0(substr(tokens, 1, 1), collapse = "")
  if (str_detect(prefix, initials) || str_detect(initials, prefix)) return(TRUE)
  
  # PRUEBA C: Primera inicial + apellidos (ej. 'jmartinez')
  if (length(tokens) >= 2) {
    init_surname1 <- paste0(substr(tokens[1], 1, 1), tokens[2])
    init_surname2 <- paste0(substr(tokens[1], 1, 1), tokens[length(tokens)])
    if (str_detect(prefix, init_surname1) || str_detect(prefix, init_surname2)) return(TRUE)
  }
  
  # PRUEBA D: Fragmentos parciales
  # Comprueba si el prefijo contiene los primeros 3 caracteres de al menos dos partes del nombre
  if (length(tokens_validos) >= 2) {
    chunks <- substr(tokens_validos, 1, 3)
    if (sum(purrr::map_lgl(chunks, ~ str_detect(prefix, .x))) >= 2) return(TRUE)
  }
  
  return(FALSE)
}

extract_corr_data <- function(corr_str) {
  if (is.na(corr_str) || !nzchar(corr_str)) {
    return(tibble(name_raw = character(0), email = character(0), name_norm = character(0)))
  }
  
  parts <- str_trim(str_split(corr_str, ";")[[1]])
  parts <- parts[parts != ""]
  email_idx <- which(str_detect(tolower(parts), "^email:"))
  
  if (length(email_idx) == 0) {
    return(tibble(name_raw = parts[1], email = NA_character_, name_norm = simple_norm(parts[1])))
  }
  
  emails <- str_replace(parts[email_idx], "(?i)^email:\\s*", "")
  name_idx <- c(1, email_idx[-length(email_idx)] + 1)
  valid_name_idx <- name_idx[name_idx <= length(parts)]
  names_extracted <- parts[valid_name_idx]
  names_extracted <- names_extracted[!str_detect(tolower(names_extracted), "^email:")]
  
  min_len <- min(length(names_extracted), length(emails))
  if (min_len > 0) {
    df_pairs <- tibble(
      name_raw  = str_squish(names_extracted[1:min_len]),
      email     = tolower(emails[1:min_len]),
      name_norm = purrr::map_chr(names_extracted[1:min_len], simple_norm)
    )
    
    df_pairs <- df_pairs %>% 
      mutate(is_valid = map2_lgl(email, name_norm, is_valid_email_pair)) %>%
      filter(is_valid) %>%
      select(-is_valid)
    
    return(df_pairs)
  } else {
    return(tibble(name_raw = character(0), email = character(0), name_norm = character(0)))
  }
}

split_authors_smart <- function(x) {
  xs <- x %||% ""
  if (str_detect(xs, ";")) str_split(xs, ";\\s*")[[1]] else str_split(xs, ",\\s*")[[1]]
}
normalize_length <- function(x, n) { length(x) <- n; x }

# ==============================================================================
# --- 3. LECTURA Y PROCESAMIENTO ---
# ==============================================================================
need_cols <- c("Authors", "Author(s) ID", "Title", "EID", "Correspondence Address")
read_one <- function(path) {
  df <- suppressMessages(read_csv(path, show_col_types = FALSE, locale = locale(encoding = "UTF-8")))
  missing <- setdiff(need_cols, names(df))
  if (length(missing)) stop(sprintf("El archivo %s no tiene: %s", path, paste(missing, collapse=", ")))
  df
}

cat("Procesando datos...\n")
raw_all <- map_dfr(INPUT_FILES, read_one) %>% distinct(EID, .keep_all = TRUE)
if(nrow(raw_all) == 0) stop("No se pudieron leer datos válidos.")

df_parsed <- raw_all %>%
  mutate(corr_data = map(`Correspondence Address`, extract_corr_data))

# ==============================================================================
# --- 4. SALIDA 1: MAPA A NIVEL PAPER ---
# ==============================================================================
cat("Generando Mapa Nivel Paper (EID -> Emails)...\n")
df_eids_email <- df_parsed %>%
  select(EID, corr_data) %>% unnest(corr_data, keep_empty = TRUE) %>% group_by(EID) %>%
  summarise(corresp_auth = paste(na.omit(name_raw), collapse = "; "), correo = paste(na.omit(email), collapse = "; "), .groups = "drop") %>%
  mutate(corresp_auth = na_if(corresp_auth, ""), correo = na_if(correo, ""))
write_csv(df_eids_email, OUT_EIDS)

# ==============================================================================
# --- 5. SALIDA 2: MAPA A NIVEL AUTOR ---
# ==============================================================================
cat("Generando Mapa Nivel Autor (AUID -> Email validado)...\n")
emails_by_auid <- df_parsed %>%
  mutate(
    Authors_list = map(Authors, split_authors_smart),
    AuthorIDs_list = map(`Author(s) ID`, ~str_split(.x %||% "", ";\\s*")[[1]]),
    max_len = pmax(lengths(Authors_list), lengths(AuthorIDs_list)),
    Authors_list = map2(Authors_list, max_len, normalize_length),
    AuthorIDs_list = map2(AuthorIDs_list, max_len, normalize_length)
  ) %>%
  select(EID, Authors_list, AuthorIDs_list, corr_data) %>%
  unnest(cols = c(Authors_list, AuthorIDs_list), names_repair = "minimal") %>%
  rename(author_name = Authors_list, author_id = AuthorIDs_list) %>%
  mutate(
    author_name = str_squish(author_name),
    author_id = str_squish(author_id),
    
    # Lista de combinaciones válidas EXACTAS
    name_variants = map(author_name, generate_name_variants),
    
    email = pmap_chr(list(corr_data, name_variants), function(pairs, variants) {
      if (is.null(pairs) || !nrow(pairs) || length(variants) == 0) return(NA_character_)
      
      for (i in seq_len(nrow(pairs))) {
        c_name <- pairs$name_norm[i]
        c_email <- pairs$email[i]
        
        # Igualdad estricta y contra NA's
        if (!is.na(c_name) && c_name %in% variants) {
          return(c_email)
        }
      }
      return(NA_character_)
    })
  ) %>%
  filter(!is.na(author_id) & author_id != "") %>%
  group_by(author_id) %>%
  summarise(email = paste(unique(na.omit(email)), collapse = "; "), .groups = "drop") %>%
  filter(!is.na(email) & email != "")

write_csv(emails_by_auid, OUT_AUID)

cat("\n PROCESO COMPLETADO\n")
cat("   -> Papers con emails VÁLIDOS guardados:", sum(!is.na(df_eids_email$correo)), "\n")
cat("   -> Autores únicos con email validado y asignado:", nrow(emails_by_auid), "\n")
cat("=====================================================\n")
