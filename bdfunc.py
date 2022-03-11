import psycopg2
from connect import connect, connect_fiscal

#  FUNÇÕES DO DATABASE ESTOQUELEGAL

def select_cursor(sent_sql):
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(sent_sql)
        return cursor.fetchall()

    except (Exception, psycopg2.Error) as error:
        print('Não está conseguindo fazer pesquisa na tabela', error)



def insert_banco(sent_sql, valores_a_inserir):
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(sent_sql, valores_a_inserir)
        conn.commit()
        return cursor.rowcount

    except (Exception, psycopg2.Error) as error:
        print('Não esta inserindo na tabela', error)



def delete_banco(sent_sql, a_deletar):
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(sent_sql, a_deletar)
        conn.commit()
        return cursor.rowcount

    except (Exception, psycopg2.Error) as error:
        print('Não esta deletando a tabela', error)




def exec_funcao_postgres(sent_sql, valores):
    try:
        
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(sent_sql, valores)
        conn.commit()
        return cursor.rowcount

    except (Exception, psycopg2.Error) as error:
        print('Não esta executando a função do postgres', error)





#  FUNÇÕES DO DATABASE FISCAL000


def exec_funcao_postgres_fiscal(sent_sql, valores):
    try:
        
        conn = connect_fiscal()
        cursor = conn.cursor()
        cursor.execute(sent_sql, valores)
        conn.commit()
        return cursor.rowcount

    except (Exception, psycopg2.Error) as error:
        print('Não esta executando a procedure do postgres - exec_funcao_postgres_fiscal()', error)


def exec_funcao_postgres_fiscal_sem_valores(sent_sql):
    try:
        
        conn = connect_fiscal()
        cursor = conn.cursor()
        cursor.execute(sent_sql)
        conn.commit()
        return cursor.rowcount

    except (Exception, psycopg2.Error) as error:
        print('Não esta executando a procedure do postgres - exec_funcao_postgres_fiscal_sem_valores()', error)
