suppressPackageStartupMessages({
  library(readr); library(dplyr); library(stringr); library(tidyr)
})

# ==============================================================================
# 1. Cargar datos 
# ==============================================================================
df_base   <- read_csv("data/output/investigadores_ugr_merged_with_scopus.csv", show_col_types = FALSE)
df_scopus <- read_csv2("data/interim/df_au_UGR_complete.csv", show_col_types = FALSE)
df_pdi    <- read_csv2("data/input/investigators_data/datos_pdi_ugr.csv", show_col_types = FALSE)

# ==============================================================================
# 2. Identificación 
# ==============================================================================
huerfanos_scopus <- df_scopus %>%
  filter(!au_id %in% df_base$au_id) %>%
  # Guardamos la cadena original "correo1; correo2" por si la necesitamos
  mutate(email_original = email) %>%
  separate_rows(email, sep = ";\\s*") %>%
  mutate(email_clean = tolower(trimws(email))) %>%
  filter(!is.na(email_clean) & email_clean != "")

# Preparar PDI
df_pdi_clean <- df_pdi %>%
  mutate(email_pdi = tolower(trimws(correo))) %>%
  filter(!is.na(email_pdi) & email_pdi != "")

# ==============================================================================
# 3. Cruzar  con PDI por EMAIL
# ==============================================================================
rescate_email <- huerfanos_scopus %>%
  inner_join(df_pdi_clean, by = c("email_clean" = "email_pdi")) %>%
  mutate(
    name = str_squish(paste(
      coalesce(NOMBRE, ""), 
      coalesce(APELL1, ""), 
      coalesce(APELL2, "")
    )),
    match_method = "EMAIL",
    # Restauramos la cadena de correos original (con todos los que tuviera)
    email = email_original 
  ) %>%
  # Si un autor cruzó 2 veces porque tenía 2 correos, quitamos el duplicado
  distinct(au_id, .keep_all = TRUE) %>%
  select(name, given_name, surname, email, orcid, au_id, eid, department)

# Unir y Guardar
resultado_v2 <- bind_rows(df_base, rescate_email)
write_csv(resultado_v2, "data/output/final_matches_pdi.csv")

cat("Rescatados por email:", nrow(rescate_email), "\n")

# ==============================================================================
# 4. Exportar los que quedan en el PDI sin unir (Sirve para múltiples correos)
# ==============================================================================
# Extraemos todos los correos sueltos que ya hemos rescatado
todos_los_emails_rescatados <- resultado_v2$email %>%
  str_split(";\\s*") %>%
  unlist() %>%
  tolower() %>%
  str_squish()

pdi_pendientes <- df_pdi_clean %>%
  mutate(
    nombre_completo = str_squish(paste(
      coalesce(NOMBRE, ""), 
      coalesce(APELL1, ""), 
      coalesce(APELL2, "")
    ))
  ) %>%
  # Miramos si el correo del PDI es uno de los correos sueltos
  filter(!email_pdi %in% todos_los_emails_rescatados) %>% 
  filter(!nombre_completo %in% resultado_v2$name)

write_csv(pdi_pendientes, "data/interim/debug_unmatched_pdi.csv")
cat("PDI pendiente para el 04.3:", nrow(pdi_pendientes), "personas.\n")