##############################################################################
# Script: 12_build_author_text_profile.R
# Propósito: 
#   Crear el "Corpus Textual" unificado de cada investigador.
#   Agrupa todas las publicaciones de todos sus au_ids en una sola fila.
#
# Entradas:
#   - data/interim/df_pub_comp_ENRICHED.csv 
#   - data/output/final_matches_consolidated.csv
#
# Salidas:
#   - data/output/author_text_profiles.csv
##############################################################################

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr); library(stringr)
})

# ==============================================================================
# --- 1. CONFIGURACIÓN ---
# ==============================================================================
DIR_INTERIM <- "data/interim"
DIR_OUTPUT  <- "data/output"

FILE_PUBS <- file.path(DIR_INTERIM, "df_pub_comp_ENRICHED.csv") 
FILE_AUTHORS <- file.path(DIR_OUTPUT, "final_matches_consolidated.csv")
FILE_OUT <- file.path(DIR_OUTPUT, "author_text_profiles.csv")

END_YEAR <- as.numeric(format(Sys.Date(), "%Y")) - 1
START_YEAR <- END_YEAR - 4
NUM_PUBLICATIONS <- 3

# ==============================================================================
# --- 2. CARGA DE DATOS E IDENTIDADES ---
# ==============================================================================
cat("Cargando datos de publicaciones y autores...\n")

# A. CARGAMOS PAPERS
df_pubs <- read_csv2(FILE_PUBS, show_col_types = FALSE) %>%
  mutate(
    eid = as.character(eid),
    au_id_list = as.character(au_id), 
    title = as.character(title),
    abstract = as.character(abstract),
    keywords = as.character(keywords), 
    year = as.numeric(year)
  ) %>%
  filter(!is.na(year), year >= START_YEAR, year <= END_YEAR)

# B. CARGAMOS LISTA
df_master <- read_csv(FILE_AUTHORS, show_col_types = FALSE) %>%
  mutate(
    # Guardamos la cadena original "111;222" para el CSV final
    all_au_ids = as.character(au_id) 
  ) %>% 
  # Separamos en varias filas para que el join con los papers no falle
  separate_rows(au_id, sep = ";\\s*") %>%
  mutate(au_id = str_trim(as.character(au_id))) %>%
  select(au_id, name, email, all_au_ids) %>%
  distinct()

cat("   -> Total Papers con Abstract:", nrow(df_pubs), "\n")
cat("   -> Total relaciones ID-Persona UGR:", nrow(df_master), "\n")

# ==============================================================================
# --- 3. FILTRADO ---
# ==============================================================================
cat("Generando relaciones Autor-Paper...\n")

# Separamos los IDs de los autores que firmaron el paper
df_long <- df_pubs %>%
  select(eid, title, abstract, keywords, au_id_list) %>% 
  separate_rows(au_id_list, sep = ";\\s*") %>%
  mutate(au_id = str_trim(au_id_list)) %>%
  select(-au_id_list)

# 2. Nos quedamos SOLO con los autores UGR usando el cruce directo
df_ugr_pubs <- df_long %>%
  inner_join(df_master, by = "au_id") %>%
  # Si un autor firmó con dos de sus IDs en el mismo paper,
  # quitamos el duplicado agrupando por su Email y el ID del paper.
  distinct(email, name, all_au_ids, eid, .keep_all = TRUE)

cat("   -> Total relaciones Persona-Paper (Filtrado UGR):", nrow(df_ugr_pubs), "\n")

# ==============================================================================
# --- 4. AGREGACIÓN (PERFILADO DE TEXTO POR PERSONA) ---
# ==============================================================================
cat("Construyendo perfiles de texto unificados por persona...\n")

paste_clean <- function(x, collapse_str = " | ") {
  x <- x[!is.na(x) & x != "" & x != "NA"]
  if(length(x) == 0) return("")
  paste(unique(x), collapse = collapse_str)
}

df_profiles <- df_ugr_pubs %>%
  # Agrupamos por la identidad de la persona, no por su ID individual
  group_by(email, name, all_au_ids) %>% 
  summarise(
    n_papers = n_distinct(eid),
    
    all_eids = paste_clean(eid, collapse = ";"),
    all_titles = paste_clean(title, collapse = " | "),
    all_abstracts = paste_clean(abstract, collapse = " /// "),
    all_keywords = paste_clean(keywords, collapse = " | "), 
    
    .groups = "drop"
  ) %>%
  rename(au_id = all_au_ids) 

df_profiles <- df_profiles %>% filter(n_papers >= NUM_PUBLICATIONS)
# ==============================================================================
# --- 5. GUARDADO ---
# ==============================================================================
write_csv(df_profiles, FILE_OUT)

cat("\n PERFILES DE TEXTO GENERADOS.\n")
cat("   Archivo guardado en:", FILE_OUT, "\n")
cat("   Total Personas Físicas:", nrow(df_profiles), "\n")
