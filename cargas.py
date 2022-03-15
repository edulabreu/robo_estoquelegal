import bdfunc
import os
import pandas as pd
from connect import connect_fiscal


#  FUNÇÕES PARA DAR CARGA NOS ARQUIVOS SPEDS




def log_erro():
    try:
        sent_sql_funcao = ("""
                            select * from log_erro;
                            
                         """)
        verifica_erro = bdfunc.exec_funcao_postgres_fiscal_sem_valores(sent_sql_funcao)
        if verifica_erro != 0:
            print(f'ERRO VERIFICADO NA TABELA LOG_ERRO - {verifica_erro}')
            exit(1)
    except(Exception) as e:
        print('Não está executando a função separa_sped_txt_em_registro', e)





def carga_speds(caminho_completo):
    try:
        sent_sql_funcao = ("""SET client_encoding TO latin1;
                            truncate sped_txt;
                            copy sped_txt (linha) from %s;
                            
                         """)
        valores = (caminho_completo)
        bdfunc.exec_funcao_postgres_fiscal(sent_sql_funcao, (valores,))
    except(Exception) as e:
        print('Não está executando a função carga_speds', e)





def separa_sped_txt_em_registro():
    try:
        sent_sql_funcao = ("""
                            update sped_txt set registro= upper(substr(linha,2,4));
                            
                         """)
        bdfunc.exec_funcao_postgres_fiscal_sem_valores(sent_sql_funcao)
    except(Exception) as e:
        print('Não está executando a função separa_sped_txt_em_registro', e)





def salva_bloco_sped_txt(n):
    try:
        sent_sql_funcao = ("""

                            select salva_bloco_sped_txt(%s);
                           
                         """)
        valores = n
        bdfunc.exec_funcao_postgres_fiscal(sent_sql_funcao, (valores,))
    except(Exception) as e:
        print('Não está executando a função salva_bloco_sped_txt', e)





def sp_exporta_reg_fiscal(pasta):
    try:
        sent_sql_funcao = ("""

                            SELECT sp_exporta_reg_fiscal(reg_sped, %s ,tabela_tmp) FROM registro_sped_fiscal WHERE tabela_tmp is not null ORDER BY id;
                           
                         """)
        valores = pasta
        bdfunc.exec_funcao_postgres_fiscal(sent_sql_funcao, (valores,))
    except(Exception) as e:
        print('Não está executando a função sp_exporta_reg_fiscal', e)





def sp_processa_sped_txt(contador):
    try:
        sent_sql_funcao = ("""Select sp_processa_sped_txt( %s ) """)
        valores = (contador)
        bdfunc.exec_funcao_postgres_fiscal(sent_sql_funcao, (valores,))
    except(Exception) as e:
        print('Não está executando a função sp_processa_sped_txt', e)





def sp_atualiza_registro_sped_fiscal():
    try:
        sent_sql_funcao = ("""Select sp_atualiza_registro_sped_fiscal(); """)
        bdfunc.exec_funcao_postgres_fiscal_sem_valores(sent_sql_funcao)
    except(Exception) as e:
        print('Não está executando a função sp_atualiza_registro_sped_fiscal', e)




def limpa_banco_total():
    try:
        sent_sql_funcao = (""" Select limpa_banco_total(); """)
        bdfunc.exec_funcao_postgres_fiscal_sem_valores(sent_sql_funcao)
    except(Exception) as e:
        print('Não está executando a função limpar_banco_total', e)





def sys_copy_from_tabela_tmp_where_qtd_campo_igual_0():
    try:
        sent_sql_funcao = (""" SELECT sp_sql_copy(a.comando_sql,'C:\\Users\\eduar\\Desktop\\programa_aureliano\\temp\\'||a.arquivo_tmp_txt, a.nome_tabela) FROM sys_copy_from_tabela_tmp a 
                          join (select distinct registro from sped_campo) as tb(registro) on a.registro=tb.registro where qtd_campo = 0 ;""")

        bdfunc.exec_funcao_postgres_fiscal_sem_valores(sent_sql_funcao)
    except(Exception) as e:
        print('Não está executando a função sys_copy_from_tabela_tmp_where_qtd_campo_igual_0', e)





def sp_sql_copy(comando_sql, arquivo_tmp_txt, nome_tabela):
    try:
        sent_sql_funcao = (""" SELECT sp_sql_copy(%s,%s,%s); """)
        valores = (comando_sql, arquivo_tmp_txt, nome_tabela)
        bdfunc.exec_funcao_postgres_fiscal(sent_sql_funcao, valores)
    except(Exception) as e:
        print('Não está executando a função sp_processa_sped_txt', e)




def inserir_sped_campo(contador, registro, qtd_campo):
    try:
        sent_sql_funcao = (""" insert into sped_campo (reg_0000_id, registro, qtd_campo) values (%s,%s,%s); """)
        valores = (contador, registro, qtd_campo)
        bdfunc.exec_funcao_postgres_fiscal(sent_sql_funcao, valores)
    except(Exception) as e:
        print('Não está executando a função inserir_sped_campo', e)




def procedimento_sped_campo(n):
    try:
        sql = """ select distinct registro from sped_txt; """
        registros = pd.read_sql_query(sql, connect_fiscal())

        for i in registros.index:

            registro = registros.at[i, 'registro']
            sql =f""" select linha from sped_txt where registro = '{registro}' limit 1; """
            linha = pd.read_sql_query(sql, connect_fiscal())
            qtd_campos = linha.at[0, 'linha'].count('|')
                    
            inserir_sped_campo(n, registro, qtd_campos - 1)
    except(Exception) as e:
        print('Não está executando a função procedimento_sped_campo', e)




#  FUNCOES PARA CARGA NOS ARQUIVOS NOTAS_TXT 




def importa_nota_txt(caminho):
    try:
        sent_sql_funcao = (""" SELECT importa_nota_txt(%s); """)
        valores = (os.path.join(caminho, 'notas_txt'))
        bdfunc.exec_funcao_postgres_fiscal(sent_sql_funcao, valores)
    except(Exception) as e:
        print('Não está executando a função importa_nota_txt', e)




def elimina_duplicidade_nota_txt():
    try:
        sent_sql_funcao = (""" SELECT elimina_duplicidade_nota_txt(); """)
        bdfunc.exec_funcao_postgres_fiscal_sem_valores(sent_sql_funcao)
    except(Exception) as e:
        print('Não está executando a função elimina_duplicidade_nota_txt', e)




def elimina_duplicidade_dos_arquivos_reg_c100():
    try:
        sent_sql_funcao = (""" SELECT proc_elimina_duplicidade_dos_arquivos_reg_c100(); """)
        bdfunc.exec_funcao_postgres_fiscal_sem_valores(sent_sql_funcao)
    except(Exception) as e:
        print('Não está executando a função elimina_duplicidade_dos_arquivos_reg_c100', e)




def atualiza_reg_c100_e_0200_c170_via_nota_txt():
    try:
        sent_sql_funcao = (""" SELECT atualiza_reg_c100_e_0200_c170_via_nota_txt(); """)
        bdfunc.exec_funcao_postgres_fiscal_sem_valores(sent_sql_funcao)
    except(Exception) as e:
        print('Não está executando a função atualiza_reg_c100_e_0200_c170_via_nota_txt', e)




def processa_c100_sem_c170():
    try:
        sent_sql_funcao = (""" SELECT processa_c100_sem_c170(%s); """)
        valores = (True)
        bdfunc.exec_funcao_postgres_fiscal(sent_sql_funcao, (valores,))
    except(Exception) as e:
        print('Não está executando a função processa_c100_sem_c170', e)
