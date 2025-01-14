# criando tabela
# no python instale a biblioteca com o codigo PIP INSTALL MYSQL-CONNECTOR-PYTHON (em minusculo)

CREATE TABLE clientes (
	codigo INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    idade INT,
    telefone CHAR(14) NOT NULL
)
