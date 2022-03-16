import re
import os
import bdfunc
import time
from datetime import datetime
import zipfile
from pathlib import Path
from datetime import datetime


def add_erro_periodo_faltantes(num_os, cnpj):
    try:
        sent_sql_funcao = ("""SELECT os_add_erro_periodo_faltante(%s, %s) """)
        valores = (num_os, cnpj)
        bdfunc.exec_funcao_postgres(sent_sql_funcao, valores)
    except(Exception) as e:
        print('Não está executando a função add_erro_periodo_faltantes', e)




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





def gerar_speds_limpos(pasta_raiz, pasta_completa, txt, n):
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
}

        caminho_para_limpar = os.path.join(pasta_completa, txt)
        original = open(caminho_para_limpar, 'r', encoding='latin-1')
        pegar_primeira_linha(caminho_para_limpar)        
        
        if not os.path.exists(os.path.join(pasta_raiz, 'sped', pegar_primeira_linha.cnpj)):
            os.makedirs(os.path.join(pasta_raiz, 'sped', pegar_primeira_linha.cnpj))

        caminho_para_criar_arquivo = os.path.join(pasta_raiz, 'sped', pegar_primeira_linha.cnpj)

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

    except(Exception) as e:
        print(f'Não foi possivel limpar o arquivo {txt}', e)




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

        pastaCompleta_sped = os.path.join(pasta, 'sped', 'originais')
  
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
