# from fpdf import FPDF
# import datetime
# from sqlalchemy import create_engine
# import pandas as pd

# engine = create_engine('postgresql+psycopg2://postgres:legal@localhost/fiscal000')

# def gerar_relatorio(nomeEmpresa, cnpj): 

#     df = pd.read_sql_query("""select * from total_xml_faltantes_resumo""", engine)
    
#     dataAtual = datetime.datetime.now().strftime('%d/%m/%Y %H:%M')

    
    
#     pdf = FPDF('P', 'mm', 'A4') 
#     pdf.alias_nb_pages()
#     pdf.add_page()
#     pdf.set_font('Arial', '' , 8)
#     # Title
#     pdf.cell(300, 10, 'ESTOQUE LEGAL INFORMATICA')
#     pdf.ln(4)
#     pdf.cell(0, 10, 'TOTAL DE XML FALTANTES')
#     pdf.ln(4)
#     pdf.cell(30, 10, 'CLIENTE')
#     pdf.cell(15)
#     pdf.cell(0, 10, f': {nomeEmpresa.upper()} - CNPJ: {cnpj} ', align='L')
#     pdf.ln(4)
#     pdf.cell(30, 10, 'DATA DA EXECUÇÃO')
#     pdf.cell(15)
#     pdf.cell(0, 10, f': {dataAtual}', align='L')
#     pdf.ln(3)
#     pdf.cell(30, 10, '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
#     pdf.ln(4)
#     pdf.cell(0, 10, '|                            |          XML FALTANTES            |' , align='L')
#     pdf.ln(3)
#     pdf.cell(0, 10, '| ANO | MODELO | QUANT ANT | QUANT ATUAL |' , align='L')
#     pdf.ln(3)
#     pdf.cell(30, 10, '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')

#     pdf.ln(3)
    
#     for i in range(len(df)):

#         ano = df.at[i, 'ano']
#         modelo = df.at[i,'modelo']
        
#         qtd_ant = df.at[i, 'qtd_ant_faltantes']
#         qtd_atual = df.at[i, 'qtd_atual_faltantes']
#         soma_qtd_ant = df['qtd_ant_faltantes'].sum()
#         soma_qtd_atual = df['qtd_atual_faltantes'].sum()
        
#         # nomeArquivo = df.at[i, 'nome_arquivo']
#         # caminho = df.at[i,'pasta']

    
#         pdf.cell(0, 10, f'| {ano} |           {modelo} |             {qtd_ant}   |                  {qtd_atual}|' , align='L')
#         pdf.ln(3)
#         pdf.cell(30, 10, '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
#         pdf.ln(3)
#         pdf.cell(0, 10, f'TOTAL              : |            {soma_qtd_ant}    |          {soma_qtd_atual}      |' , align='L')
#         pdf.ln(3)
#         pdf.cell(30, 10, '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
#         pdf.ln(3)


#     pdf.output(f'relatorio_xmlFaltantes_{nomeEmpresa}.pdf', 'F')