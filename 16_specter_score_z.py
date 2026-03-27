# -*- coding: utf-8 -*-
# Autora: Clara Ortega Sevilla

# Requisito: pip install -U adapter-transformers

import pandas as pd
import json
import torch
import numpy as np
import warnings
import re
import os
from collections import Counter

from transformers import AutoTokenizer
from adapters import AutoAdapterModel


warnings.filterwarnings('ignore')

# ---------------------------------------------------------
# 1. CARGA DE MODELO Y TOKENIZER (SPECTER2)
# ---------------------------------------------------------
print("Cargando modelo SPECTER2...")
tokenizer = AutoTokenizer.from_pretrained("allenai/specter2_base")
model = AutoAdapterModel.from_pretrained("allenai/specter2_base")
model.load_adapter("allenai/specter2", source="hf", load_as="specter2", set_active=True)

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
model.to(device)

# Funciones auxiliares para embedding
def get_embedding(text):
    inputs = tokenizer(text, padding=True, truncation=True, return_tensors="pt", 
                       return_token_type_ids=False, max_length=512)
    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.no_grad():
        output = model(**inputs)

    embedding = output.last_hidden_state[:, 0, :].cpu().numpy().squeeze().astype(np.float32)
    return embedding

# Generación de embedding vacío
print("Calculando vector de sesgo (empty embedding)...")
empty_text = "Title: . Keywords: . Abstract: . Topics: "
empty_emb_raw = get_embedding(empty_text)
EMPTY_UNIT_VECTOR = empty_emb_raw / np.linalg.norm(empty_emb_raw)

# Ortogonalización 
def clean_embedding(v):
    v = np.array(v, dtype=np.float32)
    dot_product = np.dot(v, EMPTY_UNIT_VECTOR)
    v_orthogonal = v - dot_product * EMPTY_UNIT_VECTOR
    
    norm = np.linalg.norm(v_orthogonal)
    if norm == 0:
        return v_orthogonal
    return v_orthogonal / norm

# ---------------------------------------------------------
# 2. LEER PAPERS Y EXTRAER EMBEDDINGS
# ---------------------------------------------------------
print("Leyendo dataset de papers...")
csv_authors = pd.read_csv("final_pubs.csv")
csv_authors['title'] = csv_authors['title'].fillna('')
csv_authors['abstract'] = csv_authors['abstract'].fillna('')
csv_authors['keywords'] = csv_authors['keywords'].fillna('')
csv_authors['hs_topics_all'] = csv_authors['hs_topics_all'].fillna('')

csv_authors['full_text'] = (
    "Title: " + csv_authors['title'] + 
    ". Keywords: " + csv_authors['keywords'] +
    ". Abstract: " + csv_authors['abstract'] +
    ". Topics: " + csv_authors['hs_topics_all']
)

print("Calculando y limpiando embeddings de los papers...")
author_vectors = []
for i in range(len(csv_authors)):
    raw_embedding = get_embedding(csv_authors['full_text'].iloc[i])
    clean_emb = clean_embedding(raw_embedding)
    author_vectors.append(clean_emb)

# ---------------------------------------------------------
# 2.5. FUSIÓN DE AUTORES
# ---------------------------------------------------------
print("Creando diccionario de fusión de IDs de autores...")
id_mapping = {}
try:
    df_perfiles_map = pd.read_csv("author_text_profiles_count.csv")
    df_perfiles_map['au_id'] = df_perfiles_map['au_id'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    for combined_id in df_perfiles_map['au_id'].dropna():
        if combined_id != 'nan':
            individual_ids = [x.strip() for x in combined_id.split(';') if x.strip()]
            for ind_id in individual_ids:
                id_mapping[ind_id] = combined_id
except FileNotFoundError:
    print("No se encontró 'author_text_profiles_count.csv'.")

# ---------------------------------------------------------
# 3. ASOCIACIÓN DE VECTORES A NIVEL DE AUTOR
# ---------------------------------------------------------
print("Agrupando vectores por autor...")
author_paper_data = []

for i, row in csv_authors.iterrows():
    paper_emb = author_vectors[i]
    autores_raw = str(row['au_id']).split(';')
    
    for autor in autores_raw:
        autor_limpio = autor.strip()
        if autor_limpio and autor_limpio != 'nan':
            canonical_id = id_mapping.get(autor_limpio, autor_limpio)
            
            author_paper_data.append({
                'au_id': canonical_id, 
                'eid': row['eid'], 
                'embedding': paper_emb
            })

df_ap = pd.DataFrame(author_paper_data)

author_dict = {}
for au_id, group in df_ap.groupby('au_id'):
    embs = np.vstack(group['embedding'].values) 
    eids = group['eid'].tolist() 
    
    author_dict[au_id] = {
        'num_papers': len(embs),
        'paper_embs': embs, 
        'paper_eids': eids
    }

# ---------------------------------------------------------
# 4. LEER CALLS Y ASOCIAR A PUBLICACIONES
# ---------------------------------------------------------
print("Calculando embedding de proyectos...")
with open('proyectos_simple.json', 'r', encoding='utf-8') as f:
    proyectos_ejemplo = json.load(f)

df_proyectos = pd.DataFrame(proyectos_ejemplo)
df_pub_ENRICHED = pd.read_csv('df_pub_comp_ENRICHED.csv', sep=';')

# --- PRE-PROCESAMIENTO DE DICCIONARIOS ---
eid_to_doi_map = dict(zip(df_pub_ENRICHED['eid'], df_pub_ENRICHED['doi']))
eid_to_kw_list_map = {}
for eid, kw_string in zip(df_pub_ENRICHED['eid'], df_pub_ENRICHED['keywords'].fillna('')):
    if kw_string:
        eid_to_kw_list_map[eid] = [k.strip().lower() for k in str(kw_string).split('|') if k.strip()]
    else:
        eid_to_kw_list_map[eid] = []

resultados_crudos = []

for j in range(len(df_proyectos)):
    p_title_raw = df_proyectos['title'][j]
    p_content = df_proyectos['full_content'][j]
    
    if ":" in p_title_raw:
        partes = p_title_raw.split(":", 1)
        call_id = partes[0].strip()
        call_title = partes[1].strip()
    else:
        call_id = f"PROYECTO_{j+1}"
        call_title = p_title_raw
    
    print(f"Procesando {call_id} ({j+1}/{len(df_proyectos)})...")
    
    proj_text = f"Title: {p_title_raw}\nContent: {p_content}"
    vec_proj = clean_embedding(get_embedding(proj_text))
    
    for au_id, data in author_dict.items():
        # Descartamos autores con < 5 papers (umbral ajustable)
        if data['num_papers'] < 5:
            continue
            
        # Multiplicación matricial (Dot product = Similitud Coseno con vectores normalizados)
        paper_scores = np.dot(data['paper_embs'], vec_proj)
        score_mean = np.mean(paper_scores)
        
        # Promedio TOP-33%
        k = max(1, int(np.ceil(0.33 * len(paper_scores)))) 
        
        if k == len(paper_scores):
            top_indices = np.arange(len(paper_scores))
            score_top_33 = score_mean
        else:
            # argpartition encuentra los Top K sin ordenar el resto (más eficiente que argsort)
            top_indices = np.argpartition(paper_scores, -k)[-k:]
            score_top_33 = np.mean(paper_scores[top_indices])
        
        # Umbral ajustable
        metric = score_mean if k < 3 else score_top_33
        
        top_eids = [data['paper_eids'][i] for i in top_indices]
        
        resultados_crudos.append({
            'Call_ID': call_id,
            'Call_Title': call_title,
            'au_id': au_id, 
            'num_papers': data['num_papers'],
            'score_final': np.round(metric, 4),
            'top_eids_raw': top_eids 
        })

# Asociación de DOIs y Keywords a cada autor
print("Asociando DOIs y Keywords finales...")
df_resultados = pd.DataFrame(resultados_crudos)

def get_dois(eids_list):
    return "; ".join([str(eid_to_doi_map.get(e, 'Sin DOI')) for e in eids_list])

def get_kws(eids_list):
    all_kws = []
    for eid in eids_list:
        all_kws.extend(eid_to_kw_list_map.get(eid, []))
    if not all_kws:
        return ""
    counts = Counter(all_kws)
    return " | ".join([f"{k.capitalize()} ({v})" for k, v in sorted(counts.items())])

df_resultados['papers_doi'] = df_resultados['top_eids_raw'].apply(get_dois)
df_resultados['Keywords_papers'] = df_resultados['top_eids_raw'].apply(get_kws)
df_resultados = df_resultados.drop(columns=['top_eids_raw'])

# ---------------------------------------------------------
# 5. CRUZAR CON TODOS LOS DATOS Y EXPORTAR
# ---------------------------------------------------------
print("Consolidando bases de datos (Nombres, Keywords, Topics)...")

df_resultados['au_id'] = df_resultados['au_id'].astype(str).str.replace(r'\.0$', '', regex=True)

# A) LEER Y CRUZAR PERFILES
try:
    df_perfiles = pd.read_csv("author_text_profiles_count.csv")
    df_perfiles['au_id'] = df_perfiles['au_id'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    df_perfiles_clean = df_perfiles[['au_id', 'name', 'top_keywords']].copy()
    df_perfiles_clean = df_perfiles_clean.rename(columns={'top_keywords': 'author_keywords'})
    df_perfiles_clean = df_perfiles_clean.drop_duplicates(subset=['au_id'])
    
    df_final = pd.merge(df_resultados, df_perfiles_clean, on='au_id', how='inner')
except FileNotFoundError:
    print("No se encontró 'author_text_profiles_count.csv'.")
    df_final = df_resultados

# B) LEER Y CRUZAR TOPICS DE OPENALEX
try:
    df_topics = pd.read_csv("final_author_topics_analysis.csv")
    if 'scopus_ids' in df_topics.columns:
        df_topics = df_topics.rename(columns={'scopus_ids': 'au_id'})
    df_topics['au_id'] = df_topics['au_id'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    df_topics_clean = df_topics[['au_id', 'top_topics']].copy()
    df_topics_clean = df_topics_clean.rename(columns={'top_topics': 'author_topics'})
    df_topics_clean = df_topics_clean.drop_duplicates(subset=['au_id'])
    
    df_final = pd.merge(df_final, df_topics_clean, on='au_id', how='left')
except FileNotFoundError:
    print("No se encontró 'final_author_topics_analysis.csv'.")

# C) LEER Y CRUZAR KEYWORDS DE LAS CALL
call_kw_data = []
if os.path.exists("project_keywords.txt"):
    with open("project_keywords.txt", "r", encoding="utf-8") as f:
        for line in f:
            if ":" in line:
                match = re.search(r'\((.*?)\):\s(.*)', line)
                if match:
                    call_kw_data.append({
                        'Call_ID': match.group(1).strip(), 
                        'call_keywords': match.group(2).strip()
                    })
    
    df_call_kw = pd.DataFrame(call_kw_data)
    if not df_call_kw.empty:
        df_final = pd.merge(df_final, df_call_kw, on='Call_ID', how='left')
else:
    print("No se encontró 'project_keywords.txt'.")


# ---------------------------------------------------------
# 6. ORDENAR Y LIMPIAR LA TABLA FINAL
# ---------------------------------------------------------

columnas_ordenadas = [
    'Call_ID', 'Call_Title', 'call_keywords', 
    'au_id', 'name', 'author_keywords', 'author_topics',
    'num_papers', 'score_final', 'papers_doi', 'Keywords_papers'
]

columnas_ordenadas = [c for c in columnas_ordenadas if c in df_final.columns]
df_final = df_final[columnas_ordenadas]

print("Calculando Z-scores por investigador...")

# Agrupamos por autor y estandarizamos sus puntuaciones en todos los proyectos
df_final['z_score_autor'] = df_final.groupby('au_id')['score_final'].transform(
    lambda x: (x - x.mean()) / x.std() if x.std() > 0 else 0
)

# Ordenamos por proyecto y luego por el z-score de mayor a menor
df_final = df_final.sort_values(by=['Call_ID', 'Call_Title', 'z_score_autor'], ascending=[True, True, False])

# Ranking y percentil basado en z-score
df_final['Ranking_Call'] = df_final.groupby(['Call_ID', 'Call_Title'])['z_score_autor'].rank(ascending=False, method='min').astype(int)

def calcular_percentil(group):
    return group['z_score_autor'].rank(pct=True) * 100

df_final['Percentil_call'] = df_final.groupby(['Call_ID', 'Call_Title'], group_keys=False).apply(calcular_percentil)
df_final['Percentil_call'] = df_final['Percentil_call'].round(2)

# Filtro: para cada call se muestra solo los autores con un percentil mayor de 90
# Este umbral es ajustable (de cara al documento de salida)
print("Filtrando percentiles por proyecto...")
df_final = df_final[df_final['Percentil_call'] >= 90.0]

cols = list(df_final.columns)
cols.insert(cols.index('score_final') + 1, 'z_score_autor')

# Eliminamos duplicados en la lista de columnas por la inserción
cols = list(dict.fromkeys(cols)) 
df_final = df_final[cols]

# 7. EXPORTAR
nombre_salida = "all_projects_five_scorez.csv"
df_final.to_csv(nombre_salida, index=False, encoding='utf-8')

print(f"\nProceso completado con éxito")
print(f"El Excel se ha guardado como '{nombre_salida}'.")