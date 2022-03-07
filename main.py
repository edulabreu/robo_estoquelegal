#!/usr/bin/python

from bdfunc import select_cursor
import os
import verifica_periodo_sped
import funcoes


if __name__ == '__main__':
   # conn = connect()
   # cursor = conn.cursor()
   # cursor.execute('select * from grupo')

    registros=select_cursor("""select os.id, osf.cnpj, os.num_ordem_servico, os.pasta_base as pasta_base_cliente, fl.nome_pasta, fl.nome, flg_sped_valido from ordem_servico os
              join ordem_servico_filial osf on os.id=osf.ordem_servico_id join filial fl on osf.cnpj=fl.cnpj
              where os.status_ordem_servico_id in (2,3) and osf.status_ordem_servico_id in (2,3) """)

    for row in registros:
        pasta = os.path.join(row[3],row[4])
        ordem_servico = row[2]
        num_os = row[0]
        cnpj = row[1]
        verifica_periodo_sped.ler_periodo_sped_txt(pasta, ordem_servico, num_os, cnpj)
        funcoes.compactar_arquivos(pasta, num_os, cnpj) 
    
    
     
    