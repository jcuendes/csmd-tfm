import argparse
import logging
import csv
from io import StringIO
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# (El esquema de la tabla TABLE_SCHEMA se mantiene igual)
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


# CAMBIO 1: Asegurarnos de que el orden de las columnas es el correcto
# según tu CSV. Esto es CRÍTICO.
CSV_COLUMNS = [
    'url', 'company', 'make', 'model', 'version', 'price', 'price_financed', 'fuel', 'year', 'kms',
    'power', 'doors', 'shift', 'color', 'photos', 'is_professional',
    'dealer', 'province', 'country', 'publish_date', 'insert_date'
]

class ParseAndCleanDoFn(beam.DoFn):
    def process(self, element):
        try:
            # El delimitador es ';'. StringIO lo trata como un fichero.
            reader = csv.reader(StringIO(element), delimiter=';')
            row_values = next(reader)

            # Convertimos la fila en un diccionario.
            row_dict = dict(zip(CSV_COLUMNS, row_values))

            output_dict = {}

            # Un simple bucle para procesar las columnas que nos interesan.
            for field in TABLE_SCHEMA['fields']:
                col_name = field['name']
                value = row_dict.get(col_name, '').strip()

                # Si el valor está vacío, lo dejamos como None (se convierte en NULL).
                if not value:
                    output_dict[col_name] = None
                    continue

                # Intentamos la conversión de tipo.
                try:
                    if field['type'] == 'INTEGER':
                        output_dict[col_name] = int(float(value)) # Usamos float() para manejar decimales como "150.0"
                    elif field['type'] == 'FLOAT':
                        output_dict[col_name] = float(value)
                    elif field['type'] == 'DATETIME':
                        # Formato entrada: '14/1/21 19:20' -> Salida BQ: 'YYYY-MM-DD HH:MM:SS'
                        parts = value.split(' ')
                        date_parts = parts[0].split('/')
                        day, month, year = date_parts[0], date_parts[1], "20" + date_parts[2]
                        # Aseguramos el formato correcto para BQ
                        formatted_datetime = f"{year}-{month.zfill(2)}-{day.zfill(2)} {parts[1]}:00"
                        output_dict[col_name] = formatted_datetime
                    else: # STRING
                        output_dict[col_name] = value

                except (ValueError, TypeError, IndexError) as e:
                    # Si falla la conversión, registramos el error y ponemos el campo a NULL.
                    # Esto evita que una fila mal formada descarte todo el set de datos.
                    logging.warning(f"Error de conversión en columna '{col_name}' con valor '{value}'. Error: {e}. Se insertará como NULL.")
                    output_dict[col_name] = None

            # Producimos el diccionario limpio como salida de esta etapa.
            yield output_dict

        except Exception as e:
            # Capturamos cualquier otro error inesperado en el parseo de la fila.
            logging.error(f"No se pudo procesar la fila: '{element}'. Error: {e}")
            # Al no hacer 'yield', esta fila se descarta.

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Fichero CSV de entrada en GCS.')
    parser.add_argument('--output_table', dest='output_table', required=True, help='Tabla de salida en BigQuery.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            # CAMBIO 3: Usar skip_header_lines=1 es la forma correcta de saltar la cabecera.
            | '1. Leer el fichero CSV' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            | '2. Limpiar y Transformar Filas' >> beam.ParDo(ParseAndCleanDoFn())
            | '3. Escribir en BigQuery' >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=TABLE_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()