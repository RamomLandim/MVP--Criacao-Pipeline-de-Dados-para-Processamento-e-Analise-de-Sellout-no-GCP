# Importação das bibliotecas
from google.cloud import bigquery # Biblioteca para conexão com o Big Query
import functions_framework # Framework necessário para ativar a função no Cloud Run, com essa tag a function sabe onde deverá executar
import pandas as pd # Biblioteca Pandas para validação do Data frames
import io # Biblioteca padrão do Python para leitura de arquivos
from google.cloud import storage # Biblioteca para conexão com o Cloud Storage

# Definição do schema esperado
EXPECTED_SCHEMA = {
    "IDENTIFICADOR": "STRING",
    "NUMERO DE FACTURA": "INTEGER",
    "FECHA DE LA FACTURA": "INTEGER",
    "TAX ID": "INTEGER",
    "TIPO DE FACTURA": "STRING",
    "CODIGO DE PRODUCTO": "STRING",
    "NUMERO DE LOTE": "INTEGER",
    "CANTIDAD FACTURADA": "INTEGER",
    "UNIDAD DE MEDIDA": "STRING",
    "PRECIO UNITARIO": "FLOAT",
    "FILE NAME": "STRING",
    "CODIGO DEL DISTRIBUIDOR": "INTEGER",
    "NOMBRE DEL DISTRIBUIDOR": "STRING",
    "FECHA DEL ARCHIVO": "INTEGER",
    "TAX ID DIST": "INTEGER",
}

# Nome do dataset e tabelas
DATASET_ID = "layer_l0"
TABLE_ID = "l0_sellout"
LOG_TABLE_ID = "logs_erros"

# Função para validação dos dados
def validate_schema(df):
    errors = [] # Array para armazenamento das linhas com erros
    
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col not in df.columns: # Validação estrutural do arquivo
            errors.append(f"Coluna ausente: {col}")
            continue

        # Validação dos tipos de dados
        if expected_type == "INTEGER":
            if not pd.api.types.is_numeric_dtype(df[col]):
                errors.append(f"{col} deveria ser INTEGER, mas veio {df[col].dtype}")
            elif df[col].isnull().any():
                errors.append(f"{col} contém valores nulos")
            elif (df[col] < 0).any():
                errors.append(f"{col} contém valores negativos inválidos")

        elif expected_type == "FLOAT":
            if not pd.api.types.is_float_dtype(df[col]):
                errors.append(f"{col} deveria ser FLOAT, mas veio {df[col].dtype}")
            elif df[col].isnull().any():
                errors.append(f"{col} contém valores nulos")
            elif (df[col] < 0).any():
                errors.append(f"{col} contém valores negativos inválidos")

        elif expected_type == "STRING":
            if not pd.api.types.is_object_dtype(df[col]):
                errors.append(f"{col} deveria ser STRING, mas veio {df[col].dtype}")
            elif df[col].isnull().any():
                errors.append(f"{col} contém valores nulos")

        # Validação para datas no formato DDMMYYYY
        if col in ["FECHA DE LA FACTURA", "FECHA DEL ARCHIVO"]:
            df[col] = df[col].astype(str).str.zfill(8)  # Garante que tenha 8 caracteres
    
            for index, value in df[col].items():
                if not value.isdigit() or len(value) != 8: #Valida tamanho da Data
                    errors.append(f"Linha {index + 2}: {col} contém caracteres inválidos")
                    continue

                day, month, year = int(value[:2]), int(value[2:4]), int(value[4:])

                if not (1 <= month <= 12): # Valida Mês
                    errors.append(f"Linha {index + 2}: {col} tem mês inválido ({month})")
                    continue

                if not (1 <= day <= 31): # Valida dia
                    errors.append(f"Linha {index + 2}: {col} tem dia inválido ({day})")
                    continue

                # Verifica dias válidos para cada mês
                if month in [4, 6, 9, 11] and day > 30:
                    errors.append(f"Linha {index + 2}: {col} tem dia inválido ({day}) para o mês {month}")
                elif month == 2:
                # Verifica se é ano bissexto
                    is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
                    if (is_leap and day > 29) or (not is_leap and day > 28):
                        errors.append(f"Linha {index + 2}: {col} tem dia inválido ({day}) para fevereiro {year}")

    return errors

@functions_framework.cloud_event
def ingest_csv(cloud_event):
    # Função acionada quando um novo CSV é enviado ao Cloud Storage.
    client = bigquery.Client() # conexão com o BigQuery
    storage_client = storage.Client() # Conexão com a Cloud Storage
    
    bucket_name = cloud_event.data["bucket"] # Conexão com o bucket do Cloud Storage
    file_name = cloud_event.data["name"] # Coleta do arquivo que foi carregado
    uri = f"gs://datacollect_mvp/{file_name}" # Coleta o caminho do arquivo que foi carregado
    
    print(f"Processando o arquivo: {file_name}") # Confirmação no log do Cloud Run para validação se o arquivo foi encontrado

    # Download do arquivo para validar schema
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()
    
    # Carrega o CSV em um DataFrame Pandas
    df = pd.read_csv(io.StringIO(content))

    # Valida schema na função de validação
    errors = validate_schema(df)
    if errors:
        print(f"Erro de schema no arquivo {file_name}: {errors}")

        # Registra erro no BigQuery no caminho: layer_l0.logs_erros
        # O Registro e dos erros do arquivo, no qual será armazenado: Nome do arquivo, erros e data do erro
        log_data = [{"file_name": file_name, "error_message": error} for error in errors]
        log_table_ref = f"{DATASET_ID}.{LOG_TABLE_ID}"
        client.insert_rows_json(log_table_ref, log_data)
        
        return
    
    # Configuração do job de carregamento
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Pula o cabeçalho
        write_disposition="WRITE_APPEND", # Adiciona a nova informação na tabela sem deletar o que já existia
        schema=[ bigquery.SchemaField("IDENTIFICADOR", "STRING"),
            bigquery.SchemaField("NUMERO DE FACTURA", "INTEGER"),
            bigquery.SchemaField("FECHA DE LA FACTURA", "INTEGER"),
            bigquery.SchemaField("TAX ID", "INTEGER"),
            bigquery.SchemaField("TIPO DE FACTURA", "STRING"),
            bigquery.SchemaField("CODIGO DE PRODUCTO", "STRING"),
            bigquery.SchemaField("NUMERO DE LOTE", "INTEGER"),
            bigquery.SchemaField("CANTIDAD FACTURADA", "INTEGER"),
            bigquery.SchemaField("UNIDAD DE MEDIDA", "STRING"),
            bigquery.SchemaField("PRECIO UNITARIO", "FLOAT"),
            bigquery.SchemaField("FILE NAME", "STRING"),
            bigquery.SchemaField("CODIGO DEL DISTRIBUIDOR", "INTEGER"),
            bigquery.SchemaField("NOMBRE DEL DISTRIBUIDOR", "STRING"),
            bigquery.SchemaField("FECHA DEL ARCHIVO", "INTEGER"),
            bigquery.SchemaField("TAX ID DIST", "INTEGER")]
    )

    # Iniciar o job de carregamento
    load_job = client.load_table_from_uri(uri, f"{DATASET_ID}.{TABLE_ID}", job_config=job_config)
    load_job.result()  # Aguarda o término

    print(f"Arquivo {file_name} carregado com sucesso para {DATASET_ID}.{TABLE_ID}")