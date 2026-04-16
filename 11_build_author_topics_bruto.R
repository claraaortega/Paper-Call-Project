##############################################################################
# Script: 11._build_author_topics_bruto.R
# Propósito: 
#   Tabla en bruto para que el embedding lea para cada topic el número de veces 
#   que aparece.
#
# Entradas:
#   - data/interim/df_pub_UGR_with_topics.csv 
#   - data/interim/df_pub_comp_ENRICHED.csv 
#   - data/output/final_matches_consolidated.csv
#
# Salida:
#   - data/output/final_author_topics_analysis_bruto.csv 
##############################################################################

suppressPackageStartupMessages({
  library(dplyr); library(tidyr); library(stringr); library(readr); library(purrr)
})

# ==============================================================================
# --- 1. CONFIGURACIÓN ---
# ==============================================================================
DIR_INTER  <- file.path("data", "interim")
DIR_OUTPUT <- file.path("data", "output")

FILE_TOPICS   <- file.path(DIR_INTER, "df_pub_UGR_with_topics.csv")      
FILE_PUB_AUTH <- file.path(DIR_INTER, "df_pub_comp_ENRICHED.csv")                  
FILE_MATCHES  <- file.path(DIR_OUTPUT, "final_matches_consolidated.csv") 

# Filtro de años recientes
END_YEAR <- as.numeric(format(Sys.Date(), "%Y")) - 1
START_YEAR <- END_YEAR - 4
ventana <- 5

args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 1) {
  ventana <- as.numeric(args[1])
  START_YEAR <- END_YEAR - (ventana - 1)
}

carpeta_ventana <- paste0(ventana, "_anos")
DIR_OUTPUT_VENTANA <- file.path(DIR_OUTPUT, carpeta_ventana)
dir.create(DIR_OUTPUT_VENTANA, showWarnings = FALSE, recursive = TRUE)
OUT_FINAL <- file.path(DIR_OUTPUT_VENTANA, "final_author_topics_analysis_bruto.csv")

# ==============================================================================
# --- 2. CARGA DE DATOS ---
# ==============================================================================
cat("Cargando datos...\n")

norm_email <- function(x){
  x <- tolower(trimws(x))
  x[x %in% c("", "na", "n/a")] <- NA_character_
  x
}

pub_topics <- read_csv2(FILE_TOPICS, show_col_types = FALSE) %>%
  mutate(eid = as.character(eid)) 

raw_pub_auth <- read_csv2(FILE_PUB_AUTH, show_col_types = FALSE)

if(!"year" %in% names(raw_pub_auth) & "Year" %in% names(raw_pub_auth)) {
  raw_pub_auth <- raw_pub_auth %>% rename(year = Year)
}

authors_master <- read_csv(FILE_MATCHES, show_col_types = FALSE) %>%
  mutate(
    email = norm_email(email),
    author_name = name,
    all_au_ids = as.character(au_id) # Guardamos la cadena "111;222" para el informe final
  ) %>%
  # Separamos en filas para que el cruce con los papers funcione
  separate_rows(au_id, sep = ";\\s*") %>%
  mutate(au_id = str_trim(as.character(au_id))) %>%
  filter(!is.na(au_id), au_id != "", !is.na(email)) %>%
  select(au_id, email, author_name, all_au_ids) %>%
  distinct()

# B. RELACIÓN PAPER -> ID INDIVIDUAL
doc_author <- raw_pub_auth %>%
  filter(!is.na(year), year >= START_YEAR, year <= END_YEAR) %>%
  transmute(
    eid   = as.character(eid),
    au_id = as.character(au_id)
  ) %>%
  separate_rows(au_id, sep = ";\\s*") %>%
  mutate(au_id = str_trim(au_id)) %>%
  filter(!is.na(eid), eid != "", !is.na(au_id), au_id != "") %>%
  distinct(eid, au_id)

# ==============================================================================
# --- 3. JERARQUÍA ---
# ==============================================================================
cat("Procesando topics de OpenAlex...\n")

hs_topics_raw <- pub_topics %>%
  filter(!is.na(hs_topics_all), hs_topics_all != "") %>%
  select(eid, hs_topics_all)

regex_jerarquia <- "^([^(]+) \\(([^;]+); ([^;]+); ([^;]+); ([^)]+)\\)$"

jerarquia_long <- hs_topics_raw %>%
  separate_rows(hs_topics_all, sep = " \\| ") %>% 
  filter(!is.na(hs_topics_all), hs_topics_all != "") %>%
  tidyr::extract(
    "hs_topics_all", 
    into = c("topic", "score", "subfield", "field", "domain"),
    regex = regex_jerarquia,
    convert = TRUE 
  ) %>%
  mutate(weight = 1.0) %>% # Cada paper cuenta como 1
  select(eid, topic, subfield, field, domain, weight)

# ==============================================================================
# --- 4. ASIGNACIÓN A AUTORES (POR PERSONA FÍSICA) ---
# ==============================================================================
cat("Uniendo papers a personas físicas...\n")

person_papers <- doc_author %>%
  # Cruzamos los IDs de los papers con nuestro listado de la UGR
  inner_join(authors_master, by = "au_id") %>%
  # IMPORTANTE: Si un autor firmó el mismo paper con dos IDs distintos, 
  # quitamos el duplicado agrupando por Email + Paper.
  distinct(email, author_name, all_au_ids, eid)

author_jerarquia_long <- person_papers %>%
  inner_join(jerarquia_long, by = "eid")

# ==============================================================================
# --- 5. FUNCIÓN DE AGREGACIÓN (COUNT POR PERSONA) ---
# ==============================================================================
aggregate_count <- function(df_long, group_col_name, file_suffix, top_n_limit = 10) {
  
  cat(paste("   -> Procesando:", group_col_name, "(Top", top_n_limit, "por volumen)\n"))
  
  group_sym <- sym(group_col_name)
  
  author_items_compact <- df_long %>%
    filter(!is.na(!!group_sym) & !!group_sym != "") %>%
    # Agrupamos por la identidad de la persona, no por su ID de Scopus
    group_by(email, author_name, all_au_ids, !!group_sym) %>%
    summarise(paper_count = sum(weight, na.rm = TRUE), .groups = "drop") %>%
    
    # Ordenamos (El que más tiene primero)
    arrange(email, desc(paper_count)) %>%
    
    # Filtramos (Top N)
    group_by(email, author_name, all_au_ids) %>%
    slice_head(n = top_n_limit) %>% 
    mutate(rank = row_number()) %>%
    ungroup() %>%
    
    rename(item_name = !!group_sym) %>%
    mutate(label = sprintf("#%d: %s (%d)", rank, item_name, as.integer(paper_count))) %>%
    
    # Juntamos todo en una cadena de texto
    group_by(email, author_name, all_au_ids) %>%
      summarise(
        !!paste0("top_", file_suffix) := paste(label, collapse = " | "),
        !!paste0("repeated_", file_suffix) := paste(rep(item_name, as.integer(paper_count)), collapse = "; "),
        
        .groups = "drop"
      )
    
  
  return(author_items_compact)
}

# ==============================================================================
# --- 6. EJECUCIÓN ---
# ==============================================================================
cat("Calculando perfiles temáticos...\n")

compact_topics    <- aggregate_count(author_jerarquia_long, "topic",    "topics",    top_n_limit = 15)
compact_subfields <- aggregate_count(author_jerarquia_long, "subfield", "subfields", top_n_limit = 10)
compact_fields    <- aggregate_count(author_jerarquia_long, "field",    "fields",    top_n_limit = 10)
compact_domains   <- aggregate_count(author_jerarquia_long, "domain",   "domains",   top_n_limit = 10)

# ==============================================================================
# --- 7. JOIN FINAL ---
# ==============================================================================
cat("Generando CSV final...\n")

author_topics_final <- compact_topics %>%
  left_join(compact_subfields, by = c("email", "author_name", "all_au_ids")) %>%
  left_join(compact_fields, by = c("email", "author_name", "all_au_ids")) %>%
  left_join(compact_domains, by = c("email", "author_name", "all_au_ids")) %>%
  select(
    email, 
    author_name, 
    scopus_ids = all_au_ids, 
    repeated_topics
  )

write_csv(author_topics_final, OUT_FINAL)

cat(" LISTO. Archivo generado:", OUT_FINAL, "\n")
