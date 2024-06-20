import psycopg2
import boto3
import csv
import os
import tempfile

# Configuração do PostgreSQL
db_host = "localhost"
db_port = "5432"
db_name = "nome do banco"
db_user = "usuario"
db_password = "senha"

# Configurações do AWS S3 - usar variáveis de ambiente é mais seguro
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'chave')  # Substitua pelo valor real
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'chave')  # Substitua pelo valor real
s3_bucket = "projintegrador"
s3_file_name = "dados.csv"

# Verificando se as credenciais estão definidas
if not aws_access_key or not aws_secret_key:
    raise EnvironmentError("AWS credentials not found in environment variables.")

# Conectando ao banco de dados PostgreSQL
try:
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    cursor = conn.cursor()
    
    # Executando a query para obter os dados
    cursor.execute("SELECT * FROM 'nome da tabela''")
    rows = cursor.fetchall()

    # Criando um arquivo CSV temporário
    with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', suffix='.csv') as temp_file:
        csv_file = temp_file.name
        writer = csv.writer(temp_file)
        writer.writerow([desc[0] for desc in cursor.description])
        writer.writerows(rows)

except Exception as e:
    print(f"Erro ao acessar o banco de dados: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

# Conectando ao S3 com depuração adicional
try:
    print(f"Usando as credenciais AWS Access Key: {aws_access_key}")
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    # Fazendo o upload do arquivo CSV para o S3
    s3.upload_file(csv_file, s3_bucket, s3_file_name)
    print(f"Dados enviados para o bucket S3: {s3_bucket}/{s3_file_name}")

except Exception as e:
    print(f"Erro ao enviar o arquivo para o S3: {e}")

finally:
    # Removendo o arquivo CSV temporário
    if os.path.exists(csv_file):
        os.remove(csv_file)
