##############################################################################
# Script: 04.3_crosscheck_people.R
# Propósito:
#   Une los autores de Scopus que aún no se han rescatado a los 
#   de la UGR por nombre, trayéndose su nombre completo. 
#
# Salida:
# - data/output/final_matches_pdi.csv 
# - data/interim/debug_unmatched_pdi.csv
##############################################################################

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(stringr); library(purrr); library(stringi); library(tidyr)
})

# --- RUTAS ---
FILE_V2      <- "data/output/final_matches_pdi.csv"
FILE_SCOPUS  <- "data/interim/df_au_UGR_complete.csv"
FILE_PENDING <- "data/interim/debug_unmatched_pdi.csv"
FILE_FINAL   <- "data/output/final_matches_consolidated.csv"

# 1. CARGA DE DATOS
cat("Cargando datos...\n")
df_v2 <- read_csv(FILE_V2, show_col_types = FALSE) %>% mutate(across(everything(), as.character))
df_scopus <- read_csv2(FILE_SCOPUS, show_col_types = FALSE, col_types = cols(.default = "c"))
pdi_pend <- read_csv(FILE_PENDING, show_col_types = FALSE, col_types = cols(.default = "c"))

# --- FUNCIÓN DE LIMPIEZA ---
get_tokens_clean <- function(x) {
  if (is.na(x)) return(character(0))
  
  # Normalizar (minúsculas y quitar tildes)
  x <- tolower(stri_trans_general(as.character(x), "Latin-ASCII"))
  
  # Limpiar símbolos y trocear
  tokens <- str_split(str_squish(str_replace_all(x, "[^a-z ]", " ")), " ")[[1]]
  tokens <- tokens[tokens != "" & !is.na(tokens)]
  
  # Stopwords a ignorar
  particulas <- c("de", "la", "del", "los", "las", "y", "da", "das", "do", "dos", "di", "von", "van")
  
  # Filtrar: solo nos quedamos con las palabras que NO son stopwords
  tokens_filtrados <- tokens[!(tokens %in% particulas)]
  
  return(tokens_filtrados)
}

# 2. PREPARACIÓN DE CANDIDATOS
scopus_cands <- df_scopus %>%
  filter(!au_id %in% df_v2$au_id) %>%
  mutate(
    tok_n = map(given_name, get_tokens_clean),
    tok_s = map(surname, get_tokens_clean),
    # n_surnames cuenta solo apellidos "reales" (sin 'de', 'la', etc.)
    n_surnames = map_int(tok_s, length)
  ) %>%
  # Al menos 2 apellidos sustanciales
  filter(n_surnames >= 2) %>%
  mutate(first_surname = map_chr(tok_s, ~ .x[1]))

pdi_cands <- pdi_pend %>%
  mutate(
    tok_n = map(NOMBRE, get_tokens_clean),
    tok_s = map(paste(APELL1, APELL2), get_tokens_clean),
    first_surname = map_chr(tok_s, ~ .x[1])
  )

# 3. REGLA: 2 APELLIDOS REALES EXACTOS + 1 NOMBRE COMPLETO REAL
is_paranoid_match_v2 <- function(sc_n, sc_s, pdi_n, pdi_s) {
  # A. APELLIDOS: Los 2 primeros apellidos sustanciales deben ser idénticos
  # (Ej: "de la Hoz Martinez" -> "hoz martinez". Si el otro es "de la Puerta Martinez" -> "puerta martinez". NO coinciden)
  if (length(sc_s) < 2 || length(pdi_s) < 2) return(FALSE)
  if (!all(sc_s[1:2] == pdi_s[1:2])) return(FALSE)
  
  # B. NOMBRES: Inclusión total
  matches_n <- map_lgl(sc_n, function(s_t) {
    if (nchar(s_t) == 1) return(any(str_starts(pdi_n, s_t)))
    return(s_t %in% pdi_n)
  })
  if (!all(matches_n)) return(FALSE)
  
  # C. Al menos 1 nombre sustancial debe ser completo
  has_full_name_match <- any(nchar(sc_n) > 2 & matches_n)
  
  return(has_full_name_match)
}

# 4. EJECUCIÓN
cat("Buscando coincidencias...\n")

candidatos <- scopus_cands %>%
  inner_join(pdi_cands, by = "first_surname", suffix = c("_sc", "_pdi"))

rescates <- candidatos %>%
  mutate(
    is_valid = pmap_lgl(list(tok_n_sc, tok_s_sc, tok_n_pdi, tok_s_pdi), is_paranoid_match_v2)
  ) %>%
  filter(is_valid) %>%
  group_by(au_id) %>%
  slice_head(n = 1) %>%
  ungroup()

# 5. CONSOLIDACIÓN
if(nrow(rescates) > 0) {
  cat(" Se han rescatado", nrow(rescates), "investigadores con apellidos sustanciales.\n")
  
  df_rescate_clean <- rescates %>%
    mutate(
      # Usamos coalesce para evitar que los nulos se vuelvan el texto "NA"
      name = str_squish(paste(coalesce(NOMBRE, ""), coalesce(APELL1, ""), coalesce(APELL2, ""))),
      match_method = "REGLA_ESTRICTA"
    ) %>%
    select(name, given_name, surname, email = correo, orcid, au_id, eid, 
           initials, affiliation_current, affiliation_name, 
           department = Departamento) %>%
    mutate(across(everything(), as.character))
  
  resultado_final <- bind_rows(df_v2, df_rescate_clean)
} else {
  cat("No se han encontrado nuevos rescatados.\n")
  resultado_final <- df_v2
}

# Ampliamos el filtro para bloquear emails "fantasma"
resultado_final <- resultado_final %>% 
  mutate(email = str_trim(email)) %>% # Quitamos espacios
  filter(
    !is.na(email), 
    email != "", 
    email != "@ugr.es",           # Bloquea los que son solo el dominio
    str_detect(email, "^[^@]+@")  # Obliga a que haya al menos un caracter antes de la @
  )

# 
# 6. UNIFICACIÓN DE PERFILES DUPLICADOS
# 
cat("Comprimiendo perfiles duplicados de Scopus para la misma persona...\n")

# Función auxiliar: Rompe, limpia duplicados y vuelve a unir
join_unique <- function(x) {
  val <- unique(str_squish(unlist(str_split(na.omit(x), ";"))))
  val <- val[val != ""]
  if(length(val) == 0) return(NA_character_)
  paste(val, collapse = ";")
}

resultado_definitivo <- resultado_final %>%
  select(-initials) %>%
  # Damos un ID temporal a cada fila de Scopus original
  mutate(row_id = row_number()) %>%
  
  separate_rows(email, sep = ";\\s*") %>%
  mutate(email = tolower(str_squish(email))) %>%
  filter(!is.na(email) & email != "") %>%
  
  # Si coinciden en Nombre y al menos 1 Email, les damos un ID de grupo común
  group_by(name, email) %>%
  mutate(grupo_id = min(row_id)) %>%
  ungroup() %>%
  
  # Aseguramos que los correos extra de la misma persona hereden el ID correcto
  group_by(row_id) %>%
  mutate(grupo_final = min(grupo_id)) %>%
  ungroup() %>%
  
  # Agrupamos por la persona física real y comprimimos
  group_by(grupo_final, name) %>%
  summarise(
    given_name          = first(given_name),
    surname             = first(surname),
    
    email               = join_unique(email),
    au_id               = join_unique(au_id),
    orcid               = join_unique(orcid),
    eid                 = join_unique(eid),
    
    department          = first(department),
    affiliation_current = first(affiliation_current),
    affiliation_name    = first(affiliation_name),
    
    .groups = "drop"
  ) %>%
  select(-grupo_final)

cat("Investigadores físicos únicos finales:", nrow(resultado_definitivo), "\n")

# Guardamos la tabla final 
write_csv(resultado_definitivo, FILE_FINAL)
cat(" Archivo final guardado en:", FILE_FINAL, "\n")