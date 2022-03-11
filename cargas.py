import bdfunc


def carga_speds(caminho_completo, n):
    try:
        sent_sql_funcao = ("""SET client_encoding TO latin1;
                            truncate sped_txt;
                            copy sped_txt (linha) from %s;

                            update sped_txt set registro= upper(substr(linha,2,4));

                            select salva_bloco_sped_txt(%s);

                            SELECT sp_exporta_reg_fiscal(reg_sped, 'C:\\Users\\eduar\\Desktop\\programa_aureliano\\temp\\' ,tabela_tmp) FROM registro_sped_fiscal WHERE tabela_tmp is not null ORDER BY id;

                            
                         """)
        valores = (caminho_completo, n)
        bdfunc.exec_funcao_postgres_fiscal(sent_sql_funcao, valores)
    except(Exception) as e:
        print('Não está executando a função carga_speds', e)





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