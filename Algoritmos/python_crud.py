import mysql.connector

conexao = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='bd_crud'
)

cursor=conexao.cursor()

# CRUD

# Create
# nome = "Sonic the Hedgehog"
# idade = 30
# telefone = "(21)92341-9002"

# comando = f"INSERT INTO clientes(nome, idade, telefone) VALUES ('{nome}', {idade}, '{telefone}')"
# cursor.execute(comando)

# conexao.commit() # ao realizar alterações no banco
# cursor.close()
# conexao.close()

# Read
# comando = f"SELECT * FROM clientes"
# cursor.execute(comando)
# resposta = cursor.fetchall() # trazer tudo da tabela clientes do banco
# print(resposta)
# cursor.close()
# conexao.close()

# Update
# nome = "Mary Jane" # vamos precisar criar um registro no Create com esse nome antes
# idade = 34
# telefone = "(21)92341-9002"

# comando = f"UPDATE clientes SET nome='{nome}', idade={idade}, telefone='{telefone}' WHERE nome='{nome}'"
# cursor.execute(comando)
# conexao.commit() # Atualiza os dados na tabela
# cursor.close()
# conexao.close()

# Delete
# nome = "Mary Jane"
# comando = f"DELETE FROM clientes WHERE nome='{nome}'"
# cursor.execute(comando)
# conexao.commit() # Deleta o registro da tabela
# cursor.close()
# conexao.close()
