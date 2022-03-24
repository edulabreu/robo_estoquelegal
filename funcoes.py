import re
import os
import bdfunc
import time
import zipfile
import shutil
from datetime import datetime
from pathlib import Path




def add_inserir_qnt_sped_lido(num_os, cnpj):
    try:
        sent_sql = (""" update ordem_servico_filial set qtd_sped_lido =( select count(*) from ordem_servico_sped where ordem_servico_sped.ordem_servico_id=%s
                    and ordem_servico_sped.cnpj=%s) where ordem_servico_filial.ordem_servico_id = %s and ordem_servico_filial.cnpj=%s """)
        valores =(num_os, cnpj, num_os, cnpj)
        bdfunc.exec_funcao_postgres(sent_sql, valores)
    except(Exception) as e:
        print('Não está executando a função add_inserir_qnt_sped_lido', e)





def add_inserir_qnt_sped_erro_lido(num_os, cnpj):
    try:
        sent_sql = (""" update ordem_servico_filial set qtd_sped_erro =( select count(*) from ordem_servico_sped_erro where ordem_servico_sped_erro.ordem_servico_id=%s
                    and ordem_servico_sped_erro.cnpj=%s) where ordem_servico_filial.ordem_servico_id = %s and ordem_servico_filial.cnpj=%s """)
        valores =(num_os, cnpj, num_os, cnpj)
        bdfunc.exec_funcao_postgres(sent_sql, valores)
    except(Exception) as e:
        print('Não está executando a função add_inserir_qnt_sped_erro_lido', e)





def verificar_flg_sped_valido(num_os, cnpj):
    try:
        sent_sql = (""" UPDATE ordem_servico_filial set flg_sped_valido  =  CASE WHEN qtd_sped_lido = qtd_sped and qtd_sped_erro = 0 THEN 'S' ELSE 'N' END  
    WHERE ordem_servico_filial.ordem_servico_id=%s and ordem_servico_filial.cnpj=%s """)
        valores =(num_os, cnpj)
        bdfunc.exec_funcao_postgres(sent_sql, valores)
    except(Exception) as e:
        print('Não está executando a função add_erro_periodo_faltantes', e)

 



def add_erro_periodo_faltantes(num_os):
    try:
        sent_sql_funcao = ("""select os_add_erro_periodo_faltante(ordem_servico_id, cnpj) from ordem_servico_filial where status_ordem_servico_id in (2,3) and ordem_servico_id= %s""")
        valores = (num_os)
        bdfunc.exec_funcao_postgres(sent_sql_funcao, (valores,))
    except(Exception) as e:
        print('Não está executando a função add_erro_periodo_faltantes', e)




def add_erro_periodo_duplicado(num_os):
    try:
        sent_sql_funcao = ("""delete from ordem_servico_sped_erro where ordem_servico_id = %s and status_erro_sped_id = 2;
                            insert into ordem_servico_sped_erro (ordem_servico_id, cnpj, dt_inicio, dt_fim, status_erro_sped_id)
                            select osf.ordem_servico_id, osf.cnpj, oss.dt_inicio, oss.dt_fim, 2 from ordem_servico os join ordem_servico_filial osf on os.id=osf.ordem_servico_id join ordem_servico_sped oss on os.id=osf.ordem_servico_id and
                            osf.cnpj=oss.cnpj where osf.status_ordem_servico_id in (2,3) and os.id= 1 group by osf.ordem_servico_id, osf.cnpj, oss.dt_inicio, oss.dt_fim having count(*)>1 order by osf.cnpj, oss.dt_inicio;""")
        valores = (num_os)
        bdfunc.exec_funcao_postgres(sent_sql_funcao, (valores,) )
    except(Exception) as e:
        print('Não está executando a função add_erro_periodo_duplicado', e)




def add_erro_cnpj_faltante(num_os):
    try:
        sent_sql_funcao= ("""delete from ordem_servico_sped_erro where ordem_servico_id = %s and status_erro_sped_id = 3;
                            insert into ordem_servico_sped_erro (ordem_servico_id, cnpj, status_erro_sped_id)
                            select osf.ordem_servico_id, count(distinct osf.cnpj), 3 from ordem_servico os join ordem_servico_filial osf on os.id=osf.ordem_servico_id join ordem_servico_sped oss on os.id=osf.ordem_servico_id and
                            osf.cnpj=oss.cnpj where osf.status_ordem_servico_id in (2,3) and os.id= 1 group by osf.ordem_servico_id, osf.cnpj having  count(distinct osf.cnpj)>1 order by osf.cnpj;""")
        valores = (num_os)
        bdfunc.exec_funcao_postgres(sent_sql_funcao, (valores,) )
    except(Exception) as e:
        print('Não está executando a função add_erro_cnpj_faltante', e)




def func_limpar(dict, text):
    regex = re.compile(r"(%s)" % "|".join(map(re.escape, dict.keys())), re.IGNORECASE)
    return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text)




# def split_primeira_linha(arquivo):

#     arquivo_sped = open(arquivo, 'r', encoding='latin1')
#     primeira_linha = arquivo_sped.readline()
#     tupla_primeira_linha = primeira_linha.split('|')
    
#     split_primeira_linha.cnpj = tupla_primeira_linha[7]
#     split_primeira_linha.dt_inicio = tupla_primeira_linha[4]
#     split_primeira_linha.dt_fim = tupla_primeira_linha[5]




def pegar_txt(extensao, par_pasta, par_pasta_speds, arquivos, ordem_servico):
    if arquivos.endswith(extensao):
        file = os.path.join(par_pasta_speds, arquivos)
        pegar_primeira_linha(file)

        #  GERANDO DADOS NA TABELA ORDEM_SERVICO_SPED
        sent_insert = ("""insert into ordem_servico_sped (ordem_servico_id, cnpj, dt_inicio, dt_fim, pasta, nome_arquivo) 
                       values (%s,%s,%s,%s,%s,%s)""")
        valores_a_inserir = (ordem_servico, pegar_primeira_linha.cnpj, pegar_primeira_linha.dt_inicio, pegar_primeira_linha.dt_fim, par_pasta, arquivos)

        #  INSERINO VALORES DA PRIMEIRA LINHA NA TABELA ORDEM_SERVICO_SPED
        bdfunc.insert_banco(sent_insert, valores_a_inserir)

        #  GERANDO OS ARQUIVOS SPEDS LIMPOS NA PASTA RAIZ/SPED/CNPJ
                
        # funcoes.gerar_speds_limpos(par_pasta, par_pasta_speds, arquivos, n)
        # print('GERADO ARQUIVO SPED LIMPO NO CAMINHO: ',funcoes.gerar_speds_limpos.caminho_sped_limpo)

        #  CRIANDO PASTA PADRÃO PARA TEMPORARIOS DE CARGA SPED
        # pasta_temp = os.path.join(os.getcwd(), 'temp') # 

        # if not os.path.exists(pasta_temp):
        #     os.makedirs(pasta_temp)





def pegar_primeira_linha(arquivo):

    try:

        arquivo_sped = open(arquivo, 'r', encoding='latin1')
        primeira_linha = arquivo_sped.readline()
        tupla_primeira_linha = primeira_linha.split('|')
    
        cnpj = tupla_primeira_linha[7]
        dt_inicio = tupla_primeira_linha[4]
        dt_fim = tupla_primeira_linha[5]

        data_inicio = datetime.strptime(dt_inicio, '%d%m%Y')
        data_inicio_formatada = data_inicio.strftime('%Y%m%d')
        data_fim = datetime.strptime(dt_fim , '%d%m%Y')
        data_fim_formatada = data_fim.strftime('%Y%m%d')
        

        pegar_primeira_linha.cnpj = cnpj
        pegar_primeira_linha.dt_inicio = data_inicio_formatada
        pegar_primeira_linha.dt_fim = data_fim_formatada
    
    except(Exception) as e:
        print('Erro ao entrar na função pegar_primeira_linha.  erro ->', e)
        erro = open('ARQUIVOS_COM_ERRO.txt', 'a', encoding='utf-8')
        erro.write(f'ERRO AO LER A PRIMEIRA LINHA DO ARQUIVO {arquivo} \n')
        erro.close()





def gerar_speds_limpos(pasta_raiz, cnpj):
    try:

        dict = {
'á': 'A',
'Â': 'A',
'â': 'A',
'Á': 'A',
'ã': 'A',
'Ã': 'A',
'É': 'E',
'é': 'É',
'ç': 'C',
'Ç': 'C',
'õ': 'O',
'Õ': 'O',
'ó': 'O',
'Ó': 'O',
'Ô': 'O',
'ô': 'O',
'\\': '',
';': '',
'<': '',
'>': '',
'//': '',
'/': '',
'¨': '',
'&': '',
'{': '',
'}': '',
'^': '',
'~': '',
'[': '',
']': '',
'¹': '',
'²': '',
'³': '',
'£': '',
'¢': '',
'¬': '',
'ª': '',
'º': '',
'°': '',
'ÿ': '',
'\x00': ''  

}       
        
        pasta_completa = os.path.join(pasta_raiz , 'speds' , 'originais')
        n = 1

        if os.path.exists(os.path.join(pasta_raiz, 'speds', cnpj)):
            
            shutil.rmtree(os.path.join(pasta_raiz, 'speds', cnpj)) 

        if not os.path.exists(os.path.join(pasta_raiz, 'speds', cnpj)):

            os.makedirs(os.path.join(pasta_raiz, 'speds', cnpj))

        for txt in os.listdir(pasta_completa):

            if txt.endswith ('.txt'): 
                print('LIMPANDO ARQUIVO SPED ', txt)  
          
                caminho_para_limpar = os.path.join(pasta_completa, txt)
        
                original = open(caminho_para_limpar, 'r', encoding='latin-1')
                pegar_primeira_linha(caminho_para_limpar)

                caminho_para_criar_arquivo = os.path.join(pasta_raiz, 'speds', pegar_primeira_linha.cnpj)

                novoTexto = open(os.path.join(caminho_para_criar_arquivo, pegar_primeira_linha.cnpj+'_'+str(pegar_primeira_linha.dt_fim)+"_"+str(n).zfill(3)+'.txt'), 'w+', encoding='utf-8')
                original.seek(0)
                texto = original.read()
                resultado = func_limpar(dict, texto)

                textopronto, lixo, tail = resultado.partition('SBRCAAEPDR0')
                if lixo == "" or tail == "":
                    print(f"ARQUIVO SPED DE NOME: {txt} NÃO TEM ASSINATURA ELETRONICA")

                novoTexto.write(textopronto)
                novoTexto.close()
                original.close()

                gerar_speds_limpos.caminho_sped_limpo = os.path.join(caminho_para_criar_arquivo, pegar_primeira_linha.cnpj+'_'+str(pegar_primeira_linha.dt_fim)+"_"+str(n).zfill(3)+'.txt')

                n += 1

            if txt.endswith ('.TXT'): 
                print('LIMPANDO ARQUIVO SPED ', txt)  
          
                caminho_para_limpar = os.path.join(pasta_completa, txt)
        
                original = open(caminho_para_limpar, 'r', encoding='latin-1')
                pegar_primeira_linha(caminho_para_limpar)

                caminho_para_criar_arquivo = os.path.join(pasta_raiz, 'speds', pegar_primeira_linha.cnpj)

                novoTexto = open(os.path.join(caminho_para_criar_arquivo, pegar_primeira_linha.cnpj+'_'+str(pegar_primeira_linha.dt_fim)+"_"+str(n).zfill(3)+'.txt'), 'w+', encoding='utf-8')
                original.seek(0)
                texto = original.read()
                resultado = func_limpar(dict, texto)

                textopronto, lixo, tail = resultado.partition('SBRCAAEPDR0')
                if lixo == "" or tail == "":
                    print(f"ARQUIVO SPED DE NOME: {txt} NÃO TEM ASSINATURA ELETRONICA")

                novoTexto.write(textopronto)
                novoTexto.close()
                original.close()

                gerar_speds_limpos.caminho_sped_limpo = os.path.join(caminho_para_criar_arquivo, pegar_primeira_linha.cnpj+'_'+str(pegar_primeira_linha.dt_fim)+"_"+str(n).zfill(3)+'.txt')

                n += 1

    except(Exception) as e:
        print(f'Não foi possivel executar a função gerar_speds_limpo ', e)




def descompactar_arquivos(pasta):
    try:

        #  DESCOMPACTANDO TODOS OS SPEDS COMPACTADOS EM CADA PASTA

        zips = [sub for sub in Path(pasta).glob("*.zip")]
       
        for zip in zips:
            with zipfile.ZipFile(os.path.join(pasta, zip), 'r') as arquivoZipado:
                arquivoZipado.extractall(pasta)
            os.remove(os.path.join(pasta, zip))


    except(Exception) as e:

        print(f'Não foi possivel descompactar o arquivo {zip}', e)
        



def compactar_arquivos_sped(pasta, num_os, cnpj):
    try:

        dataAtual = datetime.now().strftime('%Y%m%d%H%M')

        pastaCompleta_sped = os.path.join(pasta, 'speds', 'originais')
  
        #  COMPACTANDO OS ARQUIVOS SPEDS ORIGINAIS

        with zipfile.ZipFile(os.path.join(pastaCompleta_sped , f'backup_{num_os}_{cnpj}_{dataAtual}.zip'), 'w') as arquivoCompactado:

            arquivos = [sub for sub in Path(pastaCompleta_sped).glob("*.txt")]
            
            for arquivo in arquivos:
                try:
                    arquivoCompactado.write(arquivo, arquivo.name)
                    os.remove(arquivo)
                except(Exception) as e:
                    print(f'Verifique se o arquivo {arquivo} foi compactado', e) 

    except(Exception) as e:

        print('Não foi possivel compactar.', e)
