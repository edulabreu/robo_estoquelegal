import bdfunc
import funcoes
import os
import cargas
from connect import connect_fiscal
import pandas as pd
from pathlib import Path



def ler_periodo_sped_txt(par_pasta, ordem_servico, num_os, cnpj):

    try:
        n = 1  # Contador para utilizar na geração de sped limpo

        #  DELETANDO A TABELA ORDEM_SERVICO_SPED NO INICIO DA LEITURA
        sent_delete = (""" delete from ordem_servico_sped where ordem_servico_id = %s and cnpj = %s""")
        a_deletar = (num_os, cnpj)
        bdfunc.delete_banco(sent_delete, a_deletar) 

        par_pasta_speds = os.path.join(par_pasta , 'sped' , 'originais')

        #  DESCOMPACTANDO ARQUIVOS NA PASTA SPED/ORIGINAIS
        funcoes.descompactar_arquivos(par_pasta_speds)


        #  MODIFICANDO A EXTENSÃO DE TODOS OS ARQUIVOS NA PASTA
        for arquivos in os.listdir(par_pasta_speds):
            try:
                arquivo_antigo = os.path.join(par_pasta_speds , arquivos)
                arquivo_com_txt_novo = os.path.join(par_pasta_speds ,Path(arquivos).stem + '.txt')
                os.rename(arquivo_antigo, arquivo_com_txt_novo)
                
            except(Exception) as e:
                print(f'Não foi possivel modificar extensão do arquivo {arquivos}', e)

        #  VERIFICANDO CADA UM DOS ARQUIVOS NA PASTA
        for arquivos in os.listdir(par_pasta_speds):

            if arquivos.endswith(".txt"):
                file = os.path.join(par_pasta_speds, arquivos)
                funcoes.pegar_primeira_linha(file)

                #  GERANDO DADOS NA TABELA ORDEM_SERVICO_SPED
                sent_insert = ("""insert into ordem_servico_sped (ordem_servico_id, cnpj, dt_inicio, dt_fim, pasta, nome_arquivo) 
                               values (%s,%s,%s,%s,%s,%s)""")
                valores_a_inserir = (ordem_servico, funcoes.pegar_primeira_linha.cnpj, funcoes.pegar_primeira_linha.dt_inicio, funcoes.pegar_primeira_linha.dt_fim, par_pasta, arquivos)

                #  INSERINO VALORES DA PRIMEIRA LINHA NA TABELA ORDEM_SERVICO_SPED
                bdfunc.insert_banco(sent_insert, valores_a_inserir)

                #  GERANDO OS ARQUIVOS SPEDS LIMPOS NA PASTA RAIZ/SPED/CNPJ
                funcoes.gerar_speds_limpos(par_pasta, par_pasta_speds, arquivos, n)
                print(funcoes.gerar_speds_limpos.caminho_sped_limpo)

                #  CRIANDO PASTA PADRÃO PARA TEMPORARIOS DE CARGA SPED
                pasta_temp = os.path.join(os.getcwd(), 'temp\\')

                if not os.path.exists(pasta_temp):
                    os.makedirs(pasta_temp)


                #  LIMPANDO BANCO - EXECUTANDO LIMPA BANCO TOTAL
                cargas.limpar_banco_total()
                

                #  GERANDO CARGA NOS ARQUIVOS SPED DENTRO DA RESPECTIVA PASTA (RAIZ / SPED / CNPJ)
                cargas.carga_speds(funcoes.gerar_speds_limpos.caminho_sped_limpo)
                cargas.separa_sped_txt_em_registro()
                cargas.sp_processa_sped_txt(n)
                cargas.salva_bloco_sped_txt(n)
                cargas.sp_exporta_reg_fiscal(pasta_temp)
                cargas.sp_atualiza_registro_sped_fiscal()
                
                #  IMPLEMENTAR PASSO 3.09


                #  PASSO 3.10
                cargas.lendo_query_passo_3_10_02()
                
                n += 1  # Contador utilizado na geração de sped limpo

        #  EXECUTANDO A FUNÇÃO POSTGRES PARA VERIFICAR SPEDS FALTANTES
        funcoes.add_erro_periodo_faltantes(num_os, cnpj)
        

    except (Exception) as e:
        print('Erro na função ler_periodo_sped_txt.  erro ->', e)         
        

