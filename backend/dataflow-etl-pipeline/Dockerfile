# etl_pipeline/Dockerfile
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY requirements.txt .
COPY csv_to_bigquery_pipeline.py .

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Variable de entorno que le dice a la imagen base dónde está el script
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/csv_to_bigquery_pipeline.py"