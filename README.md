# EP-BigData
## Queries 1–3 (Double MapReduce con decimales)


### Tabla de Consultas de Alto Impacto - Análisis de Anemia (Hadoop Multi-nodo)

| Query | Pregunta de Alto Impacto | Job 1 (MapReduce Inicial) | Job 2 (MapReduce de Agregación) | Salida Decimal / Formato |
| --- | --- | --- | --- | --- |
| **Q1** | **¿Qué provincias tienen mayor tasa de anemia severa?** | **Map:** (Provincia, Severidad) <br> <br> **Reduce:** Suma de casos por cada combinación. | **Map:** Provincia <br> <br> **Reduce:** Calcula % de cada nivel sobre el total de la provincia. | Porcentajes (ej. SEV = 18.34%) |
| **Q2** | **¿A qué edad se diagnostica la anemia según género y severidad?** | **Map:** (Género, Severidad) <br> <br> **Reduce:** Acumula suma de edades y conteo total de registros. | **Map:** (Género, Severidad) <br> <br> **Reduce:** Divide (Suma de Edades / Conteo Total). | Promedio (ej. 29.75 meses) |
| **Q3** | **¿Qué redes de salud cargan con la anemia más grave?** | **Map:** (Red de Salud, Severidad) <br> <br> **Reduce:** Suma cantidad de ocurrencias por cada Red. | **Map:** Red de Salud <br> <br> **Reduce:** Aplica índice ponderado (LEV=1, MOD=2, SEV=3) / Total Red. | Índice Ponderado (1.00 - 3.00) |

---


## Query 4 (Árbol de Decisión – ML)

  - Job1: Discretiza edad → INF/TOD/YOU/PRE, cuenta distribución de clases por grupo_etario|género, calcula Gini por hoja.
  - Job2: Determina clase mayoritaria por hoja (regla), calcula exactitud de entrenamiento como decimal.

## Query 5 (Perceptrón – Red Neuronal con Gradiente Descendiente)

  - Features normalizadas: edad/60, género, tipo_diagnóstico; etiqueta binaria SEV=1.
  - Job1: Forward pass + backward pass, agrega gradientes, actualiza pesos (w_new = w_old − lr·∇), emite modelo.
  - Driver: lee los pesos nuevos del HDFS y los inyecta en la Configuration del Job2.
  - Job2: Clasifica todo el dataset con los pesos aprendidos, produce exactitud, precisión, recall, F1 y pérdida cross-entropy.

## Cómo ejecutar
```bash
cd EP-BigData
mvn clean package       # genera target/Macrodatos-1.0-SNAPSHOT jar
make start              # sube el TSV a HDFS
make run-query1         # (o run-query2 ... run-query5 / run-all)
```