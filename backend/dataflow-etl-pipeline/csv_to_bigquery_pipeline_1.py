import argparse
import logging
import csv
from io import StringIO
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Define el esquema de la tabla de BigQuery.
# Esto asegura que los datos se carguen con los tipos correctos.
TABLE_SCHEMA = {
    'fields': [
        {'name': 'make', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'model', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'version', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'fuel', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'kms', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'power', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'doors', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'shift', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'color', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'province', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'publish_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
    ]
}

# Columnas que nos interesan del CSV, en su orden original.
CSV_COLUMNS = [
    'make', 'model', 'version', 'price', 'price_financed', 'fuel', 'year', 'kms',
    'power', 'doors', 'shift', 'color', 'photos', 'is_professional',
    'dealer', 'province', 'country', 'publish_date', 'insert_date'
]

class ParseAndCleanDoFn(beam.DoFn):
    """
    Una función de transformación para parsear cada línea del CSV,
    limpiarla y convertirla a un diccionario compatible con BigQuery.
    """
    def process(self, element):
        # El CSV usa ';' como delimitador. Usamos el módulo csv para manejarlo.
        # StringIO simula un fichero en memoria para que el lector csv trabaje.
        try:
            # Leemos una sola línea del CSV
            reader = csv.reader(StringIO(element), delimiter=';')
            row_values = next(reader)

            # Creamos un diccionario con las cabeceras y los valores
            row_dict = dict(zip(CSV_COLUMNS, row_values))

            # Si la fila es la cabecera del CSV, la ignoramos.
            if row_dict.get('make') == 'make':
                return

            # --- Lógica de Normalización y Estandarización ---
            output_dict = {}
            target_columns = ['make', 'model', 'version', 'price', 'fuel', 'year', 'kms', 'power', 'doors', 'shift', 'color', 'province', 'country', 'publish_date']

            for col in target_columns:
                value = row_dict.get(col, '').strip()
                if not value:  # Si el valor está vacío, lo convertimos a None (NULL en BQ)
                    output_dict[col] = None
                    continue

                try:
                    # Conversión de tipos de datos
                    if col in ['year', 'kms', 'power', 'doors']:
                        output_dict[col] = int(value)
                    elif col == 'price':
                        output_dict[col] = float(value)
                    elif col == 'publish_date':
                        # Formato de entrada: '14/1/21 19:20'
                        # BigQuery espera: 'YYYY-MM-DD HH:MM:SS'
                        # No podemos importar 'datetime' aquí, por lo que lo formateamos como string.
                        parts = value.split(' ')
                        date_parts = parts[0].split('/')
                        day, month, year = date_parts[0], date_parts[1], date_parts[2]

                        # Añadimos el prefijo '20' al año si es necesario
                        if len(year) == 2:
                            year = f"20{year}"

                        # Formateamos a 'YYYY-MM-DD HH:MM:SS'
                        formatted_datetime = f"{year}-{month.zfill(2)}-{day.zfill(2)} {parts[1]}:00"
                        output_dict[col] = formatted_datetime
                    else:
                        output_dict[col] = value
                except (ValueError, TypeError) as e:
                    # Si hay un error de conversión, logueamos y dejamos el valor como None.
                    logging.warning(f"No se pudo procesar la columna '{col}' con valor '{value}'. Error: {e}")
                    output_dict[col] = None

            yield output_dict

        except StopIteration:
            # Línea vacía, la ignoramos
            pass
        except Exception as e:
            logging.error(f"Error procesando la fila: {element}. Error: {e}")


def run(argv=None):
    """Función principal que define y ejecuta la pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Fichero CSV de entrada en GCS (gs://bucket/path/to/file.csv).')
    parser.add_argument(
        '--output_table',
        dest='output_table',
        required=True,
        help='Tabla de salida en BigQuery (project:dataset.table).')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | '1. Leer el fichero CSV' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            | '2. Limpiar y Transformar' >> beam.ParDo(ParseAndCleanDoFn())
            | '3. Escribir en BigQuery' >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=TABLE_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # Borra la tabla y la re-escribe cada vez.
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()