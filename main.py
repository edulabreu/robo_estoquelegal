#!/usr/bin/python

from bdfunc import select_cursor
import os
import verifica_periodo_sped
import notas_txt
import funcoes
import cargas
import relatorio



if __name__ == '__main__':

    # banco_nuvem

    registros=select_cursor("""select os.id, osf.cnpj, os.num_ordem_servico, os.pasta_base as pasta_base_cliente, osf.nome_filial, osf.flg_sped_valido from ordem_servico os
              join ordem_servico_filial osf on os.id=osf.ordem_servico_id
              where osf.status_ordem_servico_id in (2,3)""")


    # registros=select_cursor("""select os.id, osf.cnpj, os.num_ordem_servico, os.pasta_base as pasta_base_cliente, fl.nome_pasta, fl.nome, flg_sped_valido from ordem_servico os
    #           join ordem_servico_filial osf on os.id=osf.ordem_servico_id join filial fl on osf.cnpj=fl.cnpj
    #           where os.status_ordem_servico_id in (2,3) and osf.status_ordem_servico_id in (2,3) ; """)
    for row in registros:

        
        pasta = os.path.join(row[3],row[4])
        ordem_servico = row[2]
        num_os = row[2]
        cnpj = row[1]

        print('ITENS DA TABELA: ',row)
        
        #  verifica_periodo_sped.ler_periodo_sped_txt(pasta, ordem_servico, num_os, cnpj)
        
        #  funcoes.gerar_speds_limpos(pasta, cnpj)

        #  cargas.gerar_carga_todos_os_speds(pasta, cnpj)
                
        notas_txt.gerar_nota_txt(pasta)
        
        #  cargas.cargas_notas_txt(pasta)
    
        # relatorio.gerar_relatorio(row[4],cnpj)

        # funcoes.compactar_arquivos_sped(pasta, ordem_servico, cnpj) 
    
    #  VERIFICAR SPEDS FALTANTES

    # funcoes.deletar_ordem_servico_sped_erro(num_os)
    # funcoes.log_erro()
    # funcoes.add_erro_periodo_faltantes(num_os)
    # funcoes.log_erro()
    # funcoes.add_erro_periodo_duplicado(num_os)
    # funcoes.log_erro()
    # funcoes.add_erro_cnpj_faltante(num_os)
    # funcoes.log_erro()
    