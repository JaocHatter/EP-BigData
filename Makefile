# ─────────────────────────────────────────────────────────────────────────────
# Makefile – Examen Parcial Big Data
# Dataset: Detección de anemia en niños < 5 años, Región San Martín (2016-2025)
# ─────────────────────────────────────────────────────────────────────────────

JAR_NAME  = Macrodatos-1.0-SNAPSHOT.jar
JAR_PATH  = target/$(JAR_NAME)

HDFS_BASE    = /anemia
HDFS_INPUT   = $(HDFS_BASE)/input
HDFS_OUT     = $(HDFS_BASE)/output
HDFS_INT     = $(HDFS_BASE)/intermediate
DATA_FILE    = src/resources/anemia_hadoop.tsv
BLOCK_SIZE   = 4194304

BLUE  = \033[0;34m
GREEN = \033[0;32m
NC    = \033[0m

.PHONY: all build start clean hdfs-clean \
        run-query1 run-query2 run-query3 run-query4 run-query5 run-all help

# ── Ayuda ──────────────────────────────────────────────────────────────────
help:
	@echo "$(BLUE)═══════════════════════════════════════════════════$(NC)"
	@echo "$(BLUE)  Comandos disponibles$(NC)"
	@echo "$(BLUE)═══════════════════════════════════════════════════$(NC)"
	@echo "  make build       - Compilar y generar el JAR"
	@echo "  make start       - Crear directorios HDFS y subir dataset"
	@echo "  make hdfs-clean  - Eliminar salidas intermedias/finales en HDFS"
	@echo "  make clean       - Limpiar Maven + HDFS"
	@echo "  ─────────────────────────────────────────────────"
	@echo "  make run-query1  - % de severidad por provincia (Double MR)"
	@echo "  make run-query2  - Edad promedio por género × severidad (Double MR)"
	@echo "  make run-query3  - Índice de carga por red de salud (Double MR)"
	@echo "  make run-query4  - Árbol de Decisión: clasificar severidad (ML)"
	@echo "  make run-query5  - Perceptrón con gradiente descendiente (NN)"
	@echo "  make run-all     - Ejecutar las 5 queries en secuencia"
	@echo "$(BLUE)═══════════════════════════════════════════════════$(NC)"

# ── Compilar ───────────────────────────────────────────────────────────────
build:
	@echo "$(BLUE)Compilando proyecto...$(NC)"
	mvn clean package -q
	@echo "$(GREEN)JAR generado: $(JAR_PATH)$(NC)"

# ── Subir datos a HDFS ─────────────────────────────────────────────────────
start:
	@echo "$(BLUE)Creando directorios en HDFS y subiendo dataset...$(NC)"
	hadoop fs -mkdir -p $(HDFS_INPUT)
	hadoop fs -put -f $(DATA_FILE) $(HDFS_INPUT)/
	@echo "$(GREEN)Dataset disponible en $(HDFS_INPUT)/$(NC)"

# Subir datos en bloques (para observar paralelismo)

start_blocks:
	@echo "$(BLUE)Creando directorios en HDFS y subiendo dataset...$(NC)"
	hadoop fs -mkdir -p $(HDFS_INPUT)
	hadoop fs -rm -f $(HDFS_INPUT)/anemia_hadoop.tsv
	hadoop fs -D dfs.blocksize=$(BLOCK_SIZE) -put $(DATA_FILE) $(HDFS_INPUT)/
	@echo "$(GREEN)Dataset disponible en $(HDFS_INPUT)/ con bloques de 4MB$(NC)"
	@echo "$(GREEN)Bloques físicos:$(NC)"
	hdfs fsck $(HDFS_INPUT)/anemia_hadoop.tsv -files -blocks


# ── Limpiar salidas HDFS ───────────────────────────────────────────────────
hdfs-clean:
	@echo "$(BLUE)Limpiando salidas en HDFS...$(NC)"
	-hadoop fs -rm -r $(HDFS_INT)/query1 $(HDFS_OUT)/query1
	-hadoop fs -rm -r $(HDFS_INT)/query2 $(HDFS_OUT)/query2
	-hadoop fs -rm -r $(HDFS_INT)/query3 $(HDFS_OUT)/query3
	-hadoop fs -rm -r $(HDFS_INT)/query4 $(HDFS_OUT)/query4
	-hadoop fs -rm -r $(HDFS_INT)/query5 $(HDFS_OUT)/query5

# ── Limpiar todo ───────────────────────────────────────────────────────────
clean: hdfs-clean
	mvn clean -q

# ─────────────────────────────────────────────────────────────────────────────
# Q1: % Distribución de Severidad por Provincia (Double MapReduce)
# ─────────────────────────────────────────────────────────────────────────────
run-query1:
	@echo "$(BLUE)── Query1: Tasa de severidad por provincia ──$(NC)"
	-hadoop fs -rm -r $(HDFS_INT)/query1 $(HDFS_OUT)/query1
	hadoop jar $(JAR_PATH) Query1.Query1Driver \
	    $(HDFS_INPUT)/anemia_hadoop.tsv \
	    $(HDFS_INT)/query1 \
	    $(HDFS_OUT)/query1
	@echo "$(GREEN)Resultado en HDFS: $(HDFS_OUT)/query1$(NC)"
	hadoop fs -cat $(HDFS_OUT)/query1/part-r-00000

# ─────────────────────────────────────────────────────────────────────────────
# Q2: Edad Promedio de Diagnóstico por Género × Severidad (Double MapReduce)
# ─────────────────────────────────────────────────────────────────────────────
run-query2:
	@echo "$(BLUE)── Query2: Edad promedio por género × severidad ──$(NC)"
	-hadoop fs -rm -r $(HDFS_INT)/query2 $(HDFS_OUT)/query2
	hadoop jar $(JAR_PATH) Query2.Query2Driver \
	    $(HDFS_INPUT)/anemia_hadoop.tsv \
	    $(HDFS_INT)/query2 \
	    $(HDFS_OUT)/query2
	@echo "$(GREEN)Resultado en HDFS: $(HDFS_OUT)/query2$(NC)"
	hadoop fs -cat $(HDFS_OUT)/query2/part-r-00000

# ─────────────────────────────────────────────────────────────────────────────
# Q3: Índice de Carga de Severidad por Red de Salud (Double MapReduce)
# ─────────────────────────────────────────────────────────────────────────────
run-query3:
	@echo "$(BLUE)── Query3: Índice de carga por red de salud ──$(NC)"
	-hadoop fs -rm -r $(HDFS_INT)/query3 $(HDFS_OUT)/query3
	hadoop jar $(JAR_PATH) Query3.Query3Driver \
	    $(HDFS_INPUT)/anemia_hadoop.tsv \
	    $(HDFS_INT)/query3 \
	    $(HDFS_OUT)/query3
	@echo "$(GREEN)Resultado en HDFS: $(HDFS_OUT)/query3$(NC)"
	hadoop fs -cat $(HDFS_OUT)/query3/part-r-00000

# ─────────────────────────────────────────────────────────────────────────────
# Q4: Árbol de Decisión – Clasificar Severidad (ML – Double MapReduce)
# ─────────────────────────────────────────────────────────────────────────────
run-query4:
	@echo "$(BLUE)── Query4: Árbol de Decisión ──$(NC)"
	-hadoop fs -rm -r $(HDFS_INT)/query4 $(HDFS_OUT)/query4
	hadoop jar $(JAR_PATH) Query4.Query4Driver \
	    $(HDFS_INPUT)/anemia_hadoop.tsv \
	    $(HDFS_INT)/query4 \
	    $(HDFS_OUT)/query4
	@echo "$(GREEN)Resultado en HDFS: $(HDFS_OUT)/query4$(NC)"
	hadoop fs -cat $(HDFS_OUT)/query4/part-r-00000

# ─────────────────────────────────────────────────────────────────────────────
# Q5: Red Neuronal (Perceptrón) con Gradiente Descendiente (NN – Double MapReduce)
# ─────────────────────────────────────────────────────────────────────────────
run-query5:
	@echo "$(BLUE)── Query5: Perceptrón con Gradiente Descendiente ──$(NC)"
	-hadoop fs -rm -r $(HDFS_INT)/query5 $(HDFS_OUT)/query5
	hadoop jar $(JAR_PATH) Query5.Query5Driver \
	    $(HDFS_INPUT)/anemia_hadoop.tsv \
	    $(HDFS_INT)/query5 \
	    $(HDFS_OUT)/query5
	@echo "$(GREEN)Resultado en HDFS: $(HDFS_OUT)/query5$(NC)"
	hadoop fs -cat $(HDFS_OUT)/query5/part-r-00000

# ─────────────────────────────────────────────────────────────────────────────
# Ejecutar todas las queries en secuencia
# ─────────────────────────────────────────────────────────────────────────────
run-all: run-query1 run-query2 run-query3 run-query4 run-query5
	@echo "$(GREEN)════ Todas las queries completadas ════$(NC)"
