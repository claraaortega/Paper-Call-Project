# ==============================================================================
# Script: 03.1_enrich_authors_with_email.R
# Propósito:
#   Enriquecer la tabla principal de autores (df_au_UGR_complete.csv) 
#   inyectando los emails extraídos de Scopus Manual.
#
# Entradas:
#   - data/interim/df_au_UGR_complete.csv (Tabla de autores a enriquecer)
#   - data/interim/emails_by_auid.csv     
#
# Salida:
#   - Sobrescribe data/interim/df_au_UGR_complete.csv
# ==============================================================================

suppressPackageStartupMessages({
  library(dplyr); library(readr)
})

# --- 1. CONFIGURACIÓN ---
DIR_INTER <- file.path("data", "interim")

PATH_TABLA3     <- file.path(DIR_INTER, "df_au_UGR_complete.csv")
PATH_MINING     <- file.path(DIR_INTER, "emails_by_auid.csv")

stopifnot(file.exists(PATH_TABLA3))

# --- 2. CARGA DE DATOS ---
df_autores <- read_csv2(PATH_TABLA3, show_col_types = FALSE) %>% 
  mutate(au_id = as.character(au_id))

cat("--- ESTADO INICIAL ---\n")
cat("Autores totales:", nrow(df_autores), "\n")
cat("Con email (previo):", sum(!is.na(df_autores$email)), "\n\n")

# --- 3. EMAILS ---
if (file.exists(PATH_MINING)) {
  cat("Cruzando con emails extraídos manualmente...\n")
  
  df_mining <- read_csv(PATH_MINING, show_col_types = FALSE) %>%
    mutate(author_id = as.character(author_id)) %>%
    select(au_id = author_id, email_mining = email)
  
  df_autores <- df_autores %>%
    left_join(df_mining, by = "au_id") %>%
    mutate(
      # Mantenemos el email original si lo hay, si no, usamos el otro
      email = coalesce(email, email_mining)
    ) %>%
    select(-email_mining)
  
  cat("Con email tras el cruce:", sum(!is.na(df_autores$email)), "\n\n")
} else {
  cat("AVISO: No se encontró el archivo de emails ('emails_by_auid.csv').\n\n")
}

# --- 4. GUARDADO ---
write_csv2(df_autores, PATH_TABLA3)

cat(" PROCESO FINALIZADO.\n")
cat("Archivo actualizado y sobrescrito: ", PATH_TABLA3, "\n")