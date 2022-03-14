import bdfunc


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




def limpar_banco_total():
    try:
        sent_sql_funcao = (""" Select limpa_banco_total(); """)
        bdfunc.exec_funcao_postgres_fiscal_sem_valores(sent_sql_funcao)
    except(Exception) as e:
        print('Não está executando a função limpar_banco_total', e)





def lendo_query_passo_3_10_02():
    try:
        sent_sql_funcao = (""" SELECT sp_sql_copy(a.comando_sql,'C:\\Users\\eduar\\Desktop\\programa_aureliano\\temp\\'||a.arquivo_tmp_txt, a.nome_tabela) FROM sys_copy_from_tabela_tmp a 
                          join (select distinct registro from sped_campo) as tb(registro) on a.registro=tb.registro where qtd_campo = 0 ;""")

        bdfunc.exec_funcao_postgres_fiscal_sem_valores(sent_sql_funcao)
    except(Exception) as e:
        print('Não está executando a função lendo_query_passo_3_10_02', e)






def sp_sql_copy(comando_sql, arquivo_tmp_txt, nome_tabela):
    try:
        sent_sql_funcao = (""" SELECT sp_sql_copy(%s,%s,%s); """)
        valores = (comando_sql, arquivo_tmp_txt, nome_tabela)
        bdfunc.exec_funcao_postgres_fiscal(sent_sql_funcao, valores)
    except(Exception) as e:
        print('Não está executando a função sp_processa_sped_txt', e)
