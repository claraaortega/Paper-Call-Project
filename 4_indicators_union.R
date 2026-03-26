suppressPackageStartupMessages({
  library(dplyr); library(readr); library(stringr); library(purrr); library(tidyr)
})

# --- RUTAS ---
DIR_INPUT   <- "data/input/investigators_data"
DIR_INTERIM <- "data/interim"
DIR_OUTPUT  <- "data/output"

# Archivos de entrada
FILE_IDS    <- file.path(DIR_INPUT, "personas_con_identificadores.csv") 
FILE_PDI    <- file.path(DIR_INPUT, "datos_pdi_ugr.csv")
FILE_SCOPUS <- file.path(DIR_INTERIM, "df_au_UGR_complete.csv")
FILE_OUT    <- file.path(DIR_OUTPUT, "investigadores_ugr_merged_with_scopus.csv")

# Función para fusionar correos
fusionar_emails_vec <- Vectorize(function(e1, e2) {
  vec <- c(str_split(e1, ";")[[1]], str_split(e2, ";")[[1]])
  vec <- str_squish(tolower(vec))
  vec <- unique(vec[vec != "" & !is.na(vec)])
  if (length(vec) == 0) return(NA_character_)
  paste(vec, collapse = "; ")
})

cat("=== INICIANDO PROCESO FINAL DE CONSOLIDACIÓN ===\n")

# ==============================================================================
# 1. PREPARACIÓN SCOPUS
# ==============================================================================
df_scopus <- read_csv2(FILE_SCOPUS, show_col_types = FALSE, col_types = cols(.default = "c")) %>%
  mutate(
    au_id_clean = str_remove(as.character(au_id), "\\.0$"),
    orcid_clean_scopus = str_extract(orcid, "[0-9X]{4}-[0-9X]{4}-[0-9X]{4}-[0-9X]{4}")
  )

# ==============================================================================
# 2. PREPARACIÓN IDENTIFICADORES
# ==============================================================================
df_ids <- read_csv(FILE_IDS, show_col_types = FALSE, col_types = cols(.default = "c")) %>%
  filter(!rol %in% c("EX_iNVESTIGADOR", "INVESTIGADOR_JUBILADO")) %>%
  mutate(
    scp_clean   = str_extract(SCP, "[0-9]+"),
    orcid_clean_ids = str_extract(ORCID, "[0-9X]{4}-[0-9X]{4}-[0-9X]{4}-[0-9X]{4}"),
    parts = str_split(nombre, ","),
    # Guardamos estas partes por si fallara el cruce con PDI (Plan B)
    surnames_id = str_squish(toupper(map_chr(parts, 1))),
    given_id    = str_squish(toupper(map_chr(parts, ~ .x[2] %||% ""))),
    nombre_bien_puesto = str_squish(paste(given_id, surnames_id))
  ) %>%
  filter(!is.na(scp_clean) | !is.na(orcid_clean_ids))

# ==============================================================================
# 3. CRUCE 1: SCOPUS + IDENTIFICADORES -> OBTENER 'name' (nombre_final)
# ==============================================================================
df_step1 <- df_scopus %>%
  left_join(df_ids %>% filter(!is.na(scp_clean)) %>% select(scp_clean, n_scp = nombre_bien_puesto, o_scp = orcid_clean_ids, g_scp=given_id, s_scp=surnames_id), 
            by = c("au_id_clean" = "scp_clean")) %>%
  left_join(df_ids %>% filter(!is.na(orcid_clean_ids)) %>% select(orcid_clean_ids, n_orc = nombre_bien_puesto, g_orc=given_id, s_orc=surnames_id), 
            by = c("orcid_clean_scopus" = "orcid_clean_ids")) %>%
  mutate(
    # Nombre Completo Normalizado
    nombre_final = coalesce(n_scp, n_orc),
    # Partes del nombre (Backup por si no está en PDI)
    given_backup = coalesce(g_scp, g_orc),
    surname_backup = coalesce(s_scp, s_orc),
    # ORCID
    orcid_final = coalesce(o_scp, orcid_clean_scopus)
  ) %>%
  select(-au_id_clean, -orcid_clean_scopus, -n_scp, -o_scp, -n_orc, -g_scp, -s_scp, -g_orc, -s_orc) %>%
  mutate(orcid = orcid_final) %>% select(-orcid_final)

# ==============================================================================
# 4. PREPARACIÓN PDI
# ==============================================================================
df_pdi <- read_csv2(FILE_PDI, show_col_types = FALSE, col_types = cols(.default = "c")) %>%
  mutate(
    n = str_squish(toupper(replace_na(NOMBRE, ""))),
    a1 = str_squish(toupper(replace_na(APELL1, ""))),
    a2 = str_squish(toupper(replace_na(APELL2, ""))),
    nombre_completo_pdi = str_squish(paste(n, a1, a2)),
    correo_pdi = tolower(str_squish(correo))
  ) %>%
  select(nombre_completo_pdi, correo_pdi, NOMBRE_PDI=n, APELL1_PDI=a1, APELL2_PDI=a2) %>%
  distinct(nombre_completo_pdi, .keep_all = TRUE)

# ==============================================================================
# 5. CRUCE FINAL Y SELECCIÓN DE COLUMNAS
# ==============================================================================
df_final <- df_step1 %>%
  # Unir con PDI por nombre
  left_join(df_pdi, by = c("nombre_final" = "nombre_completo_pdi")) %>%
  
  # FILTRO: Solo los que tienen nombre identificado (Estaban en personas_con_identificadores)
  filter(!is.na(nombre_final) & nombre_final != "") %>%
  
  mutate(
    # Fusión de Emails
    email = fusionar_emails_vec(email, correo_pdi),
    
    # Construcción de Apellidos (PDI)
    # Si hay APELL1, usamos APELL1 + APELL2. Si no, usamos el Excel de IDs
    surname_pdi_constructed = str_squish(paste(replace_na(APELL1_PDI,""), replace_na(APELL2_PDI,""))),
    surname_final = if_else(surname_pdi_constructed != "", surname_pdi_constructed, surname_backup),
    
    # Construcción de Nombre de Pila (PDI)
    given_final = coalesce(NOMBRE_PDI, given_backup)
  ) %>%
  
  # Selección y renombrado final
  transmute(
    name                = nombre_final,
    given_name          = given_final,   # Viene de NOMBRE_PDI 
    surname             = surname_final, # Viene de APELL1 APELL2
    email               = email,
    orcid               = orcid,
    au_id               = au_id,
    eid                 = eid,
    initials            = initials,
    affiliation_current = affiliation_current,
    affiliation_name    = affiliation_name,
    department          = department
  )

# ==============================================================================
# 6. EXPORTACIÓN
# ==============================================================================
write_csv(df_final, FILE_OUT, na = "")

cat("\n ARCHIVO FINAL GENERADO:\n")
cat("   Ruta:", FILE_OUT, "\n")
cat("   Total investigadores:", nrow(df_final), "\n")
cat("   Ejemplo:\n")
print(head(df_final %>% select(name, given_name, surname, email), 3))