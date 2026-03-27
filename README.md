# Paper-Call-Project

## Autoras del proyecto: 
- Laura Lázaro Soraluce
- Clara Ortega Sevilla

## Credenciales locales (Obligatorio)
Este proyecto no funciona sin claves de API. Las credenciales se guardan en archivos ocultos locales, no se suben al repositorio. Crea estos archivos en tu carpeta `HOME` (~/):

### 1. Clave de API de Scopus
Crea un archivo oculto con tu clave personal:
```bash
echo "API_KEY=TU_API_KEY" > ~/.mi_api_key
chmod 600 ~/.mi_api_key
```

En R, la clave se puede leer con: 
```r
api_key <- sub("API_KEY=", "", readLines("~/.mi_api_key"))
```

### 2. Correo de identificación para OpenAlex
OpenAlex requiere incluir un correo en el encabezado User-Agent para acceder a la cola rápida de la API. Crea el archivo oculto:
```bash
echo 'OPENALEX_MAIL=tu_correo@dominio.com'>~/.openalex_mail
chmod 600 ~/.openalex_mail
```

En R, se puede leer con:
```r
OPENALEX_MAIL <- sub("OPENALEX_MAIL=", "", readLines("~/.openalex_mail"))
```

### 3. Clave de API de OpenAI
Crea un archivo oculto con tu clave personal:
```bash
echo "OPENAI_KEY=TU_API_KEY" > ~/.openai_key
chmod 600 ~/.openai_key
```

En R, la clave se puede leer con: 
```r
OPENAI_KEY <- sub("OPENAI_KEY=", "", readLines("~/.openai_key"))
```

### 4. Protección de credenciales
Asegúrate de que las rutas sensibles están en `.gitignore` para evitar subir claves privadas o archivos pesados:
```.gitignore
# Claves y credenciales (nombres de archivo)
~/.mi_api_key
~/.openalex_mail
~/.openai_key
.env
*.token
*.key

# Archivos de datos generados y caché de R
*.rds
*.RData
.Rhistory
```

## Docker (Solo para Script de scrapeo de webs)
El scraping de webs requiere un navegador Selenium. 
```bash
docker run -d -p 4444:4444 -v /dev/shm:/dev/shm selenium/standalone-chrome
```
Si no vas a hacer scrapping de una web porque ya tengas los datos descargados, puedes saltar este paso. 

## Estructura del flujo de datos
El procesamiento se divide en una secuencia lógica de scripts numerados. El orden de ejecución es estricto. 

Script - Descripción - Entradas clave - Salidas clave

Preparación de la base de datos (scripts en R)

**00_eids_email.R** - Minería de Emails (Básica): Extrae correos de contacto de los archivos exportados manualmente de Scopus. Asocia email al paper. - Entradas: `Scopus_manualX.csv` - Salidas: `df_eids_email.csv`

**01_scopus_search.R** - Descarga Bibliométrica (API): Descarga la producción completa UGR (2015-2025) vía API. Estructura base del proyecto. - - Salidas: `df_pub_comp.csv` y `df_au_UGR.csv`

**02_author_retrieval.R** - Enriquecimiento de Perfiles: Descarga metadatos detallados (ORCID, Depto. actual) para cada autor UGR detectado. - Entradas: `df_au_UGR.csv` - Salidas: `df_au_UGR_complete.csv`

**03_enrich_authors_with_email.R** - Inyección de Emails: Rellena huecos de contacto usando dos estrategias: cruce directo (Fase 1) y rescate (Fase 2). - Entradas: `df_au_UGR_complete.csv` y las salidas del 00 - Salidas: `df_au_UGR_complete.csv` (actualizado)

**04_indicators_union.R** - Fusión Institucional + Bibliométrica: Une los datos de RRHH (Web UGR) con Scopus mediante cruce en cascada (Email > ORCID > ID > Nombre). - Entradas: `investigadores_ugr_scraped.csv` y `df_au_UGR_complete.csv` - Salidas: `investigadores_ugr_merged_with_scopus.csv`

**05_match_authors_by_email.R** - Cruce con PDI (Email): Identifica qué profesores del listado oficial PDI están en nuestra base de datos bibliométrica. - Entradas: `datos_pdi_ugr.csv` y `investigadores_ugr_merged_with_scopus.csv` - Salidas: `final_matches_pdi.csv` y `debug_04_unmatched_pdi.csv`

**06_crosscheck_people.R** - Rescate de PDI (Nombre): Intenta identificar a los profesores que no cruzaron por email usando algoritmos de similitud de nombre. - Entradas: `debug_04_unmatched_pdi.csv` - Salidas: `final_matches_consolidated.csv`

**07_openalex_topics.R** - Topics de Investigación: Consulta a OpenAlex para obtener los temas (Topics) de cada publicación. Normaliza DOIs. - Entradas: `df_pub_comp.csv` - Salidas: `df_pub_UGR_with_topics.csv`

**08_fetch_author_publications.R** - Descarga todo el historial de publicaciones de los autores en bruto. - Entradas: `final_matches_consolidated.csv` - Salidas: `author_filtered_publications.csv`

**09_merge_publications.R** - Fusiona la base de datos original de publicaciones con las nuevas descargas por autor. - Entradas: `df_pub_comp.csv` y  `author_filtered_publications.csv`- Salidas: `df_pub_comp_ENRICHED.csv`

**10_build_author_topics.R** - Perfilado de Investigadores: Calcula el "Perfil Temático" de cada profesor agregando los topics de sus papers desde 2021 a 2025. - Entradas: `df_pub_UGR_with_topics.csv`, `df_pub_comp_ENRICHED.csv` y `final_matches_consolidated.csv`- Salidas: `final_author_topics_analysis.csv`

**11_build_author_topics_bruto.R** - Tabla en bruto del script anterior para que el embedding lea para cada topic el número de veces que aparece. - Entradas: `df_pub_UGR_with_topics.csv`, `df_pub_comp_ENRICHED.csv` y `final_matches_consolidated.csv`- Salidas: `final_author_topics_analysis_bruto.csv`

**12_build_author_text_profile.R** - Crea el "Corpus Textual" unificado de cada investigador. Agrupa todas las publicaciones de todos sus au_ids en una sola fila. - Entradas: `df_pub_comp_ENRICHED.csv` y `final_matches_consolidated.csv`- Salidas: `author_text_profiles.csv`

**13_build_author_text_profile_count.R** - Tabla versionada de `author_text_profiles.csv` con las keywords de cada autor ordenadas de mayor a menor frecuencia. - Entradas: `df_pub_comp_ENRICHED.csv` y `final_matches_consolidated.csv`- Salidas: `author_text_profiles_count.csv`

**14_build_author_text_profile_bruto.R** - Tabla en bruto de `author_text_profiles.csv` con las keywords del autor repetidas, de manera que el embedding las sintetice según el número de veces que aparecen.  - Entradas: `df_pub_comp_ENRICHED.csv` y `final_matches_consolidated.csv`- Salidas: `author_text_profiles_bruto.csv`

**15_build_pubs.R** - Obtiene una tabla que recoge la información de df_pub_comp_ENRICHED y la información de los topics de OpenAlex. La información de los topics se limpia. - Entradas: `df_pub_comp_ENRICHED.csv` y `openalex_topics.csv`- Salidas: `final_pubs.csv`

Preparación del modelo SPECTER2 (Scripts en Python)

**16_specter_score_z.py** - Carga el modelo SPECTER2 para obtener embeddings de los call de HORIZON EUROPE y de las publicaciones de los autores extraídas de Scopus. Genera una tabla final con una clasificación de autores ordenadas en función de su asociación con el call. - Entradas: `final_pubs.csv`, `author_text_profiles_count.csv`, `df_pub_comp_ENRICHED.csv`, `final_author_topics_analysis.csv`, `project_keywords.txt` y `proyectos_simple.json`- Salidas: `all_projects_five_scorez.csv`.

**17_specter_liderazgo_score_z.py** - Carga el modelo SPECTER2 para obtener embeddings de los call de HORIZON EUROPE y de las publicaciones de los autores extraídas de Scopus. Genera una tabla final con una clasificación de autores ordenadas en función de su asociación con el call. Diferencia principal: genera un liderazgo para cada autor, de manera que solo se tienen cuenta aquellas publicaciones en la que el investigador es "Corresponding Author" o aparece en las primera o última posición en autoría (indica participación activa en la publicación). - Entradas: `final_pubs.csv`, `author_text_profiles_count.csv`, `all_information_pubs.csv`, `df_pub_comp_ENRICHED.csv`, `final_author_topics_analysis.csv`, `project_keywords.txt` y `proyectos_simple.json`- Salidas: `all_proj_leader_five_scorez.csv` y `comprobacion_autores_filtrados.csv`. 

**Consideraciones importantes**: 

El `archivo proyectos_simple.json` presenta para cada proyecto un "title" (identificador de la convocatoria con el título de la misma) y "full_content" (descripción de la convocatoria). Este archivo varía en función de qué proyectos queramos escoger, pero se puede obtener mediante scrapping de la web deseada. 

El archivo `project_keywords.txt` se puede generar de muchas maneras. Ha de haber una lista de keywords para cada proyecto considerado en "proyectos_simple.json". Lo importante es que siga la estructura: PROYECTO X (ID del proyecto): keyword1, keyword2, etc. EJEMPLO: PROYECTO 1 (HORIZON-HLTH-2026-01-CARE-03): low-value care, health systems performance assessment, healthcare overuse, healthcare underuse, unwarranted clinical variation, implementation research, patient-centred care, health technology assessment, healthcare resource allocation, value-based healthcare, healthcare quality indicators, artificial intelligence in healthcare

## ⚠️ Notas Importantes de Mantenimiento

**Caché de Embeddings**: El archivo topic_vectors_library.rds contiene los vectores de los 4.500 temas científicos. NO BORRAR, o habrá que volver a pagar para regenerarlo. 

**Inputs Manuales**: Los archivos en data/input/scopus_manual son descargas manuales de la web de Scopus. Si hacen falta datos de años posteriores a 2024, hay descargarlos manualmente y añadirlos ahí siguiendo el formato: `Scopus_manualX.csv`.
