import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(
    user = "postgres",
    password = "digite sua senha",
    host = "localhost",
    port = "5432"
)
conn.autocommit = True

cur = conn.cursor()

db_name = "nome do banco"
cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

cur.close()
conn.close()

conn = psycopg2.connect(
    dbname = db_name,
    user = "postgres",
    password = "digite sua senha",
    host = "localhost",
    port = "5432"
)

conn.autocommit = True
cur = conn.cursor()

cur.execute("""
    CREATE TABLE usuarios (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            email VARCHAR(100),
            idade INT
    )
""")

usuarios = [
    ('João Silva', 'joao.silva@example.com', 28),
    ('Maria Souza', 'maria.souza@example.com', 34),
    ('Pedro Oliveira', 'pedro.oliveira@example.com', 45),
    ('Ana Costa', 'ana.costa@example.com', 23),
    ('Paulo Pereira', 'paulo.pereira@example.com', 31),
    ('Carla Mendes', 'carla.mendes@example.com', 29),
    ('Lucas Lima', 'lucas.lima@example.com', 22),
    ('Fernanda Gomes', 'fernanda.gomes@example.com', 27),
    ('Ricardo Alves', 'ricardo.alves@example.com', 38),
    ('Mariana Rocha', 'mariana.rocha@example.com', 26)
]

for usuario in usuarios:
    cur.execute(
        "INSERT INTO usuarios (nome,email,idade) VALUES (%s, %s, %s)",
        usuario
    )

cur.close()
conn.close()

print("Banco de dados e tabela criados com sucesso")