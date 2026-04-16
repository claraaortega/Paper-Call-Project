##############################################################################
# Script: 15_build_pubs.R
# Propósito: 
#   Obtener una tabla que recoja la información de df_pub_comp_ENRICHED y la 
#   información de los topics de OpenAlex. La información de los topics se limpia.
#
# Entradas:
#   - data/interim/df_pub_comp_ENRICHED.csv 
#   - data/interim/openalex_topics.csv
#
# Salidas:
#   - data/output/final_pubs.csv
##############################################################################

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr); library(stringr)
})

# ==============================================================================
# --- 1. CONFIGURACIÓN ---
# ==============================================================================
rango <- Sys.getenv("RANGO_ANOS")
if (rango == "") rango <- "5"

DIR_INTERIM <- "data/interim"

DIR_SALIDA <- file.path("data", "output", paste0(rango, "_anos"))
if(!dir.exists(DIR_SALIDA)) dir.create(DIR_SALIDA, recursive = TRUE)

FILE_PUBS <- file.path(DIR_INTERIM, "df_pub_comp_ENRICHED.csv") 
FILE_TOPICS <- file.path(DIR_INTERIM, "openalex_topics.csv")
FILE_OUT <- file.path(DIR_SALIDA, "final_pubs.csv")

END_YEAR <- as.numeric(format(Sys.Date(), "%Y")) - 1
START_YEAR <- END_YEAR - (as.numeric(rango) - 1)

# ==============================================================================
# --- 2. CARGA DE DATOS E IDENTIDADES ---
# ==============================================================================
cat("Cargando datos de publicaciones y autores...\n")

# A. CARGAMOS PAPERS
df_pubs <- read_csv2(FILE_PUBS, show_col_types = FALSE) %>%
  mutate(
    eid = as.character(eid),
    doi = str_trim(str_to_lower(as.character(doi))), 
    au_id_list = as.character(au_id), 
    title = as.character(title),
    abstract = as.character(abstract),
    keywords = tolower(as.character(keywords)), 
    year = as.numeric(year)
  ) %>%
  filter(!is.na(year), year >= START_YEAR, year <= END_YEAR)

# B. CARGAMOS TOPICS
df_topics <- read_csv2(FILE_TOPICS, show_col_types = FALSE) %>%
  mutate(doi = str_trim(str_to_lower(doi)))

# ==============================================================================
# --- 3. UNIMOS POR DOI ---
# ==============================================================================
df_pubs_enrich <- df_pubs %>%
  left_join(df_topics, by = "doi")

# ==============================================================================
# --- 4. LIMPIAMOS EL CAMPO HS_TOPICS_ALL ---
# ==============================================================================
df_pubs_enrich <- df_pubs_enrich %>%
  mutate(
    # Eliminamos los paréntesis, su contenido y posibles espacios previos
    hs_topics_all = str_replace_all(hs_topics_all, "\\s*\\(.*?\\)", ""),
    
    # Reemplazamos las barras verticales (con sus espacios) por un punto y coma limpio
    hs_topics_all = str_replace_all(hs_topics_all, "\\s*\\|\\s*", "; ")
  )

# ==============================================================================
# --- 5. ESCRIBIMOS EL NUEVO CSV ---
# ==============================================================================
df_pubs_enrich <- df_pubs_enrich %>%
  select(
    au_id,
    eid,
    keywords,
    abstract,
    title,
    hs_topics_all
  )
write_csv(df_pubs_enrich, FILE_OUT)

cat("\n GENERADO ARCHIVO NUEVO.\n")
cat("   Archivo guardado en:", FILE_OUT, "\n")
