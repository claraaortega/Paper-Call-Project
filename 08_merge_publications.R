##############################################################################
# Script: 08_merge_publications.R
# Propósito:
#   Fusionar la base de datos original de publicaciones con las nuevas 
#   descargas por autor.
#   Rellena abstracts/keywords vacíos de papers que YA existían.
#   Añade como filas nuevas los papers que NO existían en la base.
#
# Entradas:
#   - data/interim/df_pub_comp.csv
#   - data/output/author_filtered_publications.csv
#
# Salidas:
#   - data/interim/df_pub_comp_ENRICHED.csv
##############################################################################

suppressPackageStartupMessages({
  library(dplyr); library(readr); library(stringr)
})

# ==============================================================================
# --- 1. CONFIGURACIÓN ---
# ==============================================================================
FILE_BASE <- "data/interim/df_pub_comp.csv"
FILE_NEW  <- "data/output/author_filtered_publications.csv"
FILE_OUT  <- "data/interim/df_pub_comp_ENRICHED.csv"

cat("Cargando bases de datos...\n")

df_base <- read_csv2(FILE_BASE, show_col_types = FALSE) %>%
  mutate(eid = as.character(eid))

df_new <- read_csv(FILE_NEW, show_col_types = FALSE) %>%
  mutate(eid = as.character(eid))

# Aseguramos que df_base tiene las columnas abstract y keywords.
if (!"abstract" %in% names(df_base)) df_base <- df_base %>% mutate(abstract = NA_character_)
if (!"keywords" %in% names(df_base)) df_base <- df_base %>% mutate(keywords = NA_character_)

cat("  -> Papers en base original:", nrow(df_base), "\n")
cat("  -> Papers en nueva descarga:", nrow(df_new), "\n")

# ==============================================================================
# --- 2. ACTUALIZAR PAPERS YA EXISTENTES ---
# ==============================================================================
cat("Actualizando abstracts y keywords de papers existentes...\n")

# Hacemos un cruce (join) con la tabla nueva, pero solo nos traemos los textos
df_base_updated <- df_base %>%
  left_join(
    df_new %>% select(eid, new_abs = abstract, new_key = keywords, new_doi = doi), 
    by = "eid"
  ) %>%
  mutate(
    # A veces los campos vacíos vienen como "", los pasamos a NA para que 'coalesce' funcione bien
    abs_temp = na_if(str_trim(abstract), ""),
    key_temp = na_if(str_trim(keywords), ""),
    doi_temp = na_if(str_trim(doi), ""),
    new_abs_temp = na_if(str_trim(new_abs), ""),
    new_key_temp = na_if(str_trim(new_key), ""),
    new_doi_temp = na_if(str_trim(new_doi), ""),
    
    # coalesce() coge el primer valor que no sea NA. 
    # Es decir: si la base original tiene abstract, se lo queda. Si es NA, coge el nuevo.
    abstract = coalesce(abs_temp, new_abs_temp),
    keywords = coalesce(key_temp, new_key_temp),
    doi      = coalesce(doi_temp, new_doi_temp)
  ) %>%
  # Limpiamos las columnas temporales que usamos para el cruce
  select(-starts_with("new_"), -ends_with("_temp"))

# ==============================================================================
# --- 3. AÑADIR PAPERS TOTALMENTE NUEVOS ---
# ==============================================================================
cat("Añadiendo papers totalmente nuevos...\n")

# Filtramos df_new para quedarnos solo con los EIDs que no están en df_base
df_novel <- df_new %>%
  filter(!eid %in% df_base$eid) %>%
  # Renombramos 'all_au_ids' a 'au_id' para que la columna se llame igual que en df_base
  rename(au_id = all_au_ids)

cat("  -> Papers nuevos descubiertos:", nrow(df_novel), "\n")

# Pegamos las filas nuevas al final de la base actualizada
df_final <- bind_rows(df_base_updated, df_novel) %>%
  distinct(eid, .keep_all = TRUE) 

# ==============================================================================
# --- 4. GUARDADO ---
# ==============================================================================
write_csv2(df_final, FILE_OUT)

cat("\n=====================================================\n")
cat(" FUSIÓN COMPLETADA\n")
cat("   Total de publicaciones en el archivo final:", nrow(df_final), "\n")
cat("   Total con Abstract:", sum(!is.na(df_final$abstract) & df_final$abstract != ""), "\n")
cat("   Total con Keywords:", sum(!is.na(df_final$keywords) & df_final$keywords != ""), "\n")
cat("   Archivo guardado en:", FILE_OUT, "\n")
cat("=====================================================\n")
