from importlib.resources import path
import re
import os
import bdfunc
import time
from datetime import datetime
import zipfile
from pathlib import Path
from datetime import datetime


def regex_primeira_linha(linha):

    rel_exp = re.compile(r'[\sa-zA-Z0-9-.]+')
    lista = rel_exp.finditer(str(linha))
    particionado = []

    for itens in lista:
        particionado.append(itens)

    regex_primeira_linha.primeira_linha = particionado




def pegar_primeira_linha(linha):

    try:

        regex_primeira_linha(linha)

        data_inicio = datetime.strptime(regex_primeira_linha.primeira_linha[3].group(), '%d%m%Y')
        data_inicio_formatada = data_inicio.strftime('%Y%m%d')
        data_fim = datetime.strptime(regex_primeira_linha.primeira_linha[4].group(), '%d%m%Y')
        data_fim_formatada = data_fim.strftime('%Y%m%d')

        pegar_primeira_linha.cnpj = regex_primeira_linha.primeira_linha[6].group()
        pegar_primeira_linha.data_inicio = data_inicio_formatada
        pegar_primeira_linha.data_fim = data_fim_formatada
    
    except(Exception) as e:
        print('Erro ao entrar na função pegar_primeira_linha.  erro ->', e)




def add_erro_periodo_faltantes(num_os, cnpj):
    try:
        
        sent_sql_funcao = ("""SELECT os_add_erro_periodo_faltante(%s, %s) """)
        valores = (num_os, cnpj)
        bdfunc.exec_funcao_postgres(sent_sql_funcao, valores)
    except(Exception) as e:
        print('Não está executando a função selecionada', e)




def func_limpar(dict, text):
    regex = re.compile(r"(%s)" % "|".join(map(re.escape, dict.keys())), re.IGNORECASE)
    return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text)




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
'-': '',
'_': '',
'!': '',
'#': '',
'$': '',
'%': '',
'¨': '',
'&': '',
'*': '',
'(': '',
')': '',
'+': '',
'=': '',
'{': '',
'}': '',
'^': '',
'~': '',
'[': '',
']': '',
'?': '',
':': '',
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
        rel_exp = re.compile(r'[\sa-zA-Z0-9-.]+')
        lista = rel_exp.finditer(str(original.readline()))
        particionado = []

        for itens in lista:
            particionado.append(itens)

        data = datetime.strptime(particionado[4].group(), '%d%m%Y')
        dataFormatada = data.strftime('%Y%m%d')

        
        
        if not os.path.exists(os.path.join(pasta_raiz, 'sped', particionado[6].group())):
            os.makedirs(os.path.join(pasta_raiz, 'sped', particionado[6].group()))

        caminho_para_criar_arquivo = os.path.join(pasta_raiz, 'sped', particionado[6].group())

        novoTexto = open(os.path.join(caminho_para_criar_arquivo, particionado[6].group()+'_'+str(dataFormatada)+"_"+str(n).zfill(3)+'.txt'), 'w+', encoding='utf-8')
        original.seek(0)
        texto = original.read()
        resultado = func_limpar(dict, texto)
        textopronto, lixo, tail = resultado.partition('SBRCAAEPDR0')
        novoTexto.write(textopronto)

        novoTexto.close()
        original.close()

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
        



def compactar_arquivos(pasta, num_os, cnpj):
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
