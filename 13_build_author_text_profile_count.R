##############################################################################
# Script: 13_build_author_text_profile_count.R
# Propósito: 
#   Crear el "Corpus Textual" unificado de cada investigador.
#   Agrupa todas las publicaciones de todos sus au_ids en una sola fila.
#
# Entradas:
#   - data/interim/df_pub_comp_ENRICHED.csv 
#   - data/output/final_matches_consolidated.csv
#
# Salidas:
#   - data/output/author_text_profiles_bruto.csv
##############################################################################
suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr); library(stringr)
})

# ==============================================================================
# --- 1. CONFIGURACIÓN ---
# ==============================================================================
rango <- Sys.getenv("RANGO_ANOS")
if (rango == "") rango <- "5"

DIR_INTERIM     <- file.path("data", "interim")
DIR_OUTPUT_BASE <- file.path("data", "output")

DIR_SALIDA <- file.path("data", "output", paste0(rango, "_anos"))
if(!dir.exists(DIR_SALIDA)) dir.create(DIR_SALIDA, recursive = TRUE)

FILE_PUBS    <- file.path(DIR_INTERIM, "df_pub_comp_ENRICHED.csv") 
FILE_AUTHORS <- file.path(DIR_OUTPUT_BASE, "final_matches_consolidated.csv")
FILE_OUT <- file.path(DIR_SALIDA, "author_text_profiles_count.csv")

END_YEAR <- as.numeric(format(Sys.Date(), "%Y")) - 1
START_YEAR <- END_YEAR - (as.numeric(rango) - 1)
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

# Nos quedamos SOLO con los autores UGR usando el cruce directo
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

# 4.A - Textos generales (Títulos y Abstracts)
df_general <- df_ugr_pubs %>%
  group_by(email, name, all_au_ids) %>% 
  summarise(
    n_papers = n_distinct(eid),
    all_eids = paste_clean(eid, collapse = ";"),
    all_titles = paste_clean(title, collapse = " | "),
    all_abstracts = paste_clean(abstract, collapse = " /// "),
    .groups = "drop"
  )

# 4.B - Procesamiento específico de Keywords (Contar, ordenar y repetir)
df_keywords <- df_ugr_pubs %>%
  select(email, name, all_au_ids, keywords) %>%
  # Separar las keywords (vienen separadas por ";" o "|")
  separate_rows(keywords, sep = "\\s*[;|]\\s*") %>%
  # Normalizar el texto: quitar espacios y poner tipo título ("Complex Network")
  mutate(
    keywords = str_trim(keywords),
    keywords = tolower(keywords) # Como título: str_to_title(tolower(keywords))
  ) %>%
  filter(!is.na(keywords) & keywords != "" & keywords != "Na") %>%
  
  # 1. CONTAR
  group_by(email, name, all_au_ids, keywords) %>%
  summarise(kw_count = n(), .groups = "drop") %>%
  
  # 2. ORDENAR por relevancia (el que más se repite primero)
  arrange(email, desc(kw_count)) %>%
  
  # 3. ETIQUETAR
  group_by(email, name, all_au_ids) %>%
  mutate(
    rank = row_number(),
    label = sprintf("#%d: %s (%d)", rank, keywords, kw_count)
  ) %>%
  summarise(
    top_keywords = paste(label, collapse = " | "),
    .groups = "drop"
  )

# 4.C - Unir ambos perfiles
df_profiles <- df_general %>%
  left_join(df_keywords, by = c("email", "name", "all_au_ids")) %>%
  rename(au_id = all_au_ids) 

df_profiles <- df_profiles %>% filter(n_papers >= NUM_PUBLICATIONS)
# ==============================================================================
# --- 5. GUARDADO ---
# ==============================================================================
write_csv(df_profiles, FILE_OUT)

cat("\n PERFILES DE TEXTO GENERADOS.\n")
cat("   Archivo guardado en:", FILE_OUT, "\n")
cat("   Total Personas Físicas:", nrow(df_profiles), "\n")
