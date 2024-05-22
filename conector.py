import psycopg2
import boto3
import csv
import os

# Configuração do PostgreSql
db_host = "localhost" 
db_port = "5432"
db_name = "nome do banco"
db_user = "usuario"
db_password = "senha do banco"

# Configurações do AWS S3
aws_access_key = "Sua chave do AWS"
aws_secret_key = "Sua chave secreta do AWS"
s3_bucket = "Nome do bucket"
s3_file_name = "dados.csv"

conn = psycopg2.connect (
    host = db_host,
    port = db_port,
    dbname=db_name,
    user =db_user,
    password=db_password
)

cursor = conn.cursor()

# Executando a query para obter os dados
cursor.execute("SELECT * FROM nome da tabela")
rows = cursor.fletchall()

# Escrevendo os dados em um arquivo CSV temporário

csv_file = "/tmp/dados.csv"
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([desc[0] for desc in cursor.description])
    writer.writerows(rows)

cursor.close()
conn.close()


# Conectando ao S3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_key=aws_access_key
)


# Fazendo o upload do arquivo CSV para o S3
s3.upload_file(csv_file, s3_bucket, s3_file_name)

os.remove(csv_file)

print(f"Dados enviados para o bucket S3: {s3_bucket}/{s3_file_name}")
