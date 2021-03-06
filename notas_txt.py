from datetime import date
from pathlib import Path
import os
import re





def multiplas_trocas(d, text):
    regex = re.compile(r"(%s)" % "|".join(map(re.escape, d.keys())), re.IGNORECASE)
    return regex.sub(lambda mo: d[mo.string[mo.start():mo.end()]], text)


cEspeciais = {
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



def gerar_nota_txt(caminho):

    caminho_completo = os.path.join(caminho, 'planilha_cliente_xml')

    arquivos = [sub for sub in Path(caminho_completo).glob("**/*.csv")] 
    n = 1
    for arquivo in arquivos:
        try:
            print('LIMPANDO ARQUIVO NOTAS_TXT ', arquivo)

            #  POSSO CHAMAR A FUNÇÃO PEGAR PRIMEIRA LINHA AQUI  -> VERIFICAR DEPOIS
            arquivo_notas_txt = open(arquivo, 'r', encoding='latin1')
            arquivo_notas_txt.readline()   # PULAR A PRIMEIRA LINHA - CABEÇALHO  
            numero_de_linhas = arquivo_notas_txt.readlines()   # PEGAR O TOTAL DE LINHAS
            arquivo_notas_txt.seek(0)  # VOLTAR PAR AO INICIO DO ARQUIVO  
            arquivo_notas_txt.readline()   # PULAR A PRIMEIRA LINHA - CABEÇALHO     
        
            novoTexto = open('temp.txt', 'w', encoding='utf-8')
            erros = open(os.path.join(caminho, 'erros_limpeza_xml_'+str(n).zfill(2)+'.err'), 'w', encoding='utf-8')

            for itens in (range(len(numero_de_linhas) - 1)):
                
                linha = arquivo_notas_txt.readline()
                
                tupla_valores = linha.split(';')
                
            

                chv_nfe = str.strip(tupla_valores[1])
                cnpj_emitente = str.strip(tupla_valores[2])
                tp_emis = str.strip(tupla_valores[3])
                cod_sit = str.strip(tupla_valores[4])
                nr_sat = 000000000
                num_doc = str.strip(tupla_valores[7])
                dt_doc_inicial = str.strip(tupla_valores[9])
                dt_doc = dt_doc_inicial[0:-9]
                dt_e_s = str.strip(tupla_valores[10])
                num_item = str.strip(tupla_valores[11])
                cod_item = str.strip(tupla_valores[12])
                descr_item = str.strip(tupla_valores[13]) 
                qtd = str.strip(tupla_valores[14]).replace(',' , '.')  
                unid = str.strip(tupla_valores[15]).replace(',' , '.')   
                vl_item = str.strip(tupla_valores[16]).replace(',' , '.')    
                vl_desc = str.strip(tupla_valores[17]).replace(',' , '.')  
                # qtd = str.strip(tupla_valores[14]) + '.' + str.strip(tupla_valores[15])
                # unid = str.strip(tupla_valores[16])
                # vl_item = str.strip(tupla_valores[17]) + '.' + str.strip(tupla_valores[18])
                # vl_desc = str.strip(tupla_valores[19]) + '.' + str.strip(tupla_valores[20])
                ind_mov = ''
                cst_icms = str.strip(tupla_valores[19])
                cfop = str.strip(tupla_valores[20])
                cod_cta= str.strip(tupla_valores[24])
                cod_ncm = str.strip(tupla_valores[22])
                cest = str.strip(tupla_valores[21])
                #  OBS:  LISTA INVERSA -> | 24(cod_cta) = -1 | - | 14(qtd) = -11  |   

                if qtd.replace('.','',1).isdigit() == False or len(unid) > 6 or vl_item.replace('.','',1).isdigit() == False or vl_desc.replace('.','',1).isdigit() == False or len(cst_icms) > 4 or len(cfop) > 4:
                    dados = [chv_nfe, cnpj_emitente, tp_emis, cod_sit, num_doc, dt_doc, dt_e_s, num_item, cod_item, descr_item, qtd, unid, vl_item, vl_desc, cst_icms, cfop, cod_cta, cod_ncm, cest]
                    erros.write('|' + dados[0] + '|' + dados[1] + '|' + dados[2] + '|' + dados[3] + '|' + str(nr_sat) + '|' + dados[4] + '|' + dados[5] + '|' + dados[6] + '|' + dados[7] + '|' + dados[8] + '|' + dados[9] + '|' + dados[10] + '|' + dados[11] + '|' + dados[12] + '|' + dados[13] + '|' + str(ind_mov) + '|' + dados[14] + '|' + dados[15] + '|' + '|0.00|0.00|0.00|0.00|0.00|0.00|0|||0.00|0.00|0.00||0.00|0.00|0.00|0.00|0.00||0.00|0.00|0.00|0.00|0.00|' + dados[16] + '||' + dados[17]  + '|' + dados[18]  + '|' + '\n')
                

                else:
                    dados = [chv_nfe, cnpj_emitente, tp_emis, cod_sit, num_doc, dt_doc, dt_e_s, num_item, cod_item, descr_item, qtd, unid, vl_item, vl_desc, cst_icms, cfop, cod_cta, cod_ncm, cest]
                    novoTexto.write('|' + dados[0] + '|' + dados[1] + '|' + dados[2] + '|' + dados[3] + '|' + str(nr_sat) + '|' + dados[4] + '|' + dados[5] + '|' + dados[6] + '|' + dados[7] + '|' + dados[8] + '|' + dados[9] + '|' + dados[10] + '|' + dados[11] + '|' + dados[12] + '|' + dados[13] + '|' + str(ind_mov) + '|' + dados[14] + '|' + dados[15] + '|' + '|0.00|0.00|0.00|0.00|0.00|0.00|0|||0.00|0.00|0.00||0.00|0.00|0.00|0.00|0.00||0.00|0.00|0.00|0.00|0.00|' + dados[16] + '||' + dados[17]  + '|' + dados[18]  + '|' + '\n')
                
            

                # for i in range(len(dados)):
                #     if dados[i] == 'NULL':
                #         dados[i] = ''
    
                
                
        
            novoTexto.seek(0)
            novoTexto.close()
            erros.seek(0)
            erros.close()

            caminhoCompleto = os.path.join(caminho, 'notas_txt')
            if not os.path.exists(caminhoCompleto): 
                os.makedirs(caminhoCompleto)
    
            caminhoArquivo = os.path.join(caminhoCompleto, str(arquivo.stem + '_limpo.txt'))  
            output = open(caminhoArquivo, "w+", encoding='utf-8')
            input = open("temp.txt", 'r', encoding='latin1').read()

            print(caminhoArquivo)


            output.write(multiplas_trocas(cEspeciais, input))
    
            # output.write(re.sub(r'[\\]+',
            #                     ' ', 
            #                     input, 
            #                     flags=re.M))

            output.close()
            os.remove('temp.txt')
            n += 1
        except(Exception) as e:
            print('Exception - Erro na função gerar_nota_txt', e)