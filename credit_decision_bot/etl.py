# *-* encoding:utf-8 *-*

import csv
import datetime
import os
import numpy as np
import pandas as pd
import sqlite3
from time import sleep


class CustomerDataManager:
    def __init__(self, repositorio_arquivos={'clientesdf': None, 'sapdf': None, 'renegdf': None, 'fatdf': None, 'prefiltrodf': None}, dir_base=os.getcwd()):
        self.dir_base = dir_base
        self.repositorio_arquivos = repositorio_arquivos
        self.caminho_basesql = 'cadastroclientes.db'
        self.bases = dict()

    def Updater(self):
        """This method is responsible to ensure that the most updated information will be used on the current run.

        1 - Ensure that network drive is mapped and available
        2 - Wait for inputs to be the most recent. In case the SQLite database is already updated it runs normally.
        3 - Load the inputs into memory then call transformations function.
        4 - Save work done as a SQLite database."""
        while os.path.exists('z:') == False:
            sleep(60)
        while datetime.datetime.fromtimestamp(os.path.getmtime(self.repositorio_arquivos['sapdf'])).date() < datetime.date.today() or datetime.datetime.fromtimestamp(os.path.getmtime(self.caminho_basesql)).date() == datetime.date.today():
            sleep(60)
        for nome, planilha in self.repositorio_arquivos.items():
            self.FilesLoader(nome, planilha)
            # del self.repositorio_arquivos[nome]
        self.DataProcessing()
        self.CreditPolicyAppliance()
        self.SaveToFile()

    def FilesLoader(self, nome_base, caminho):
        """It load Excel Spreadsheets used as inputs into memory for later use.""""""
        if nome_base == 'sapdf':
            self.bases[nome_base] = pd.read_excel(caminho)
        elif nome_base == 'clientesdf':
            self.bases[nome_base] = pd.read_excel(caminho)
        elif nome_base == 'fatdf':
            self.bases[nome_base] = pd.read_excel(
                caminho, sheet_name='FaturamentoLTM')
        elif nome_base == 'prefiltrodf':
            self.bases[nome_base] = pd.read_excel(
                caminho, sheet_name='Bloqueios')
        elif nome_base == 'renegdf':
            self.bases[nome_base] = pd.read_excel(caminho, sheet_name='RENEG')
        else:
            self.bases[nome_base] = pd.read_excel(caminho)

    def DataProcessing(self):
    "This is performing a mix of transformations: renaming cols, creating categorical columns, fillin null and so on.
        # Base de Partidas vencidas, consolidação e cálculo de índices:
        self.bases['sapdf'].loc[(self.bases['sapdf']['VencLíquid'] <= str(datetime.date.fromordinal(datetime.date.toordinal(datetime.date.today())-5))) & (self.bases['sapdf']['Compensaç.'].isna() == True) & (self.bases['sapdf']
                                                                                                                                                                                                                ['BancEmpr'].isna() == False) & (self.bases['sapdf']['Mont.em MI'] > 0) & (self.bases['sapdf']['MP'] == 'N') & (self.bases['sapdf']['Solic.L/C'].isna() == True), 'Vencidos'] = self.bases['sapdf']['Mont.em MI']
        self.bases['sapdf'].loc[(self.bases['sapdf']['VencLíquid'] <= str(datetime.date.fromordinal(datetime.date.toordinal(datetime.date.today())-5))) & (self.bases['sapdf']
                                                                                                                                                           ['Compensaç.'].isna() == True) & (self.bases['sapdf']['BancEmpr'].isna() == False) & (self.bases['sapdf']['Mont.em MI'] > 0), 'Vencidos'] = self.bases['sapdf']['Mont.em MI']
        self.bases['sapdf'].loc[(self.bases['sapdf']['VencLíquid'] <= str(datetime.date.fromordinal(datetime.date.toordinal(datetime.date.today())-5))) & (self.bases['sapdf']['Compensaç.'].isna() == True) & (self.bases['sapdf']['BancEmpr'].isna() == False) & (
            self.bases['sapdf']['Mont.em MI'] > 0) & (self.bases['sapdf']['MP'] == 'N') & (self.bases['sapdf']['Solic.L/C'].isna() == False), 'Vencido em Dias'] = (pd.to_datetime('today') - self.bases['sapdf']['VencLíquid']).astype('timedelta64[D]')
        # self.bases['sapdf'].loc[(self.bases['sapdf']['VencLíquid'] <= '2020-03-17') & (self.bases['sapdf']['Compensaç.'].isna() == True) & (self.bases['sapdf']['Chave referência 3'].isna() == False) & (self.bases['sapdf']['Mont.em MI'] > 0), 'Vencidos Pré-Covid19'] = self.bases['sapdf']['Mont.em MI']
        self.bases['sapdf'].loc[self.bases['sapdf']['Mont.em MI']
                                < 0, 'Créditos'] = -self.bases['sapdf']['Mont.em MI']
        self.bases['sapdf'] = self.bases['sapdf'].groupby('Cliente').agg(
            {'Mont.em MI': sum, 'Créditos': sum, 'Vencidos': [sum, 'count'], 'Vencido em Dias': max})
        self.bases['sapdf'].rename(
            {'Mont.em MI': 'Exposição Total', 'Vencido em Dias': 'Maior Atraso em Dias'}, axis=1, inplace=True)
        # Base Cadastral de clientes - Limpeza:
        self.bases['clientesdf'] = self.bases['clientesdf'][(
            self.bases['clientesdf']['Cod Sap'] <= 999999)]
        self.bases['clientesdf'].drop(['Fantasia', 'Endereço', 'Nro', 'Complemento', 'Bairro', 'Cep',
                                      'País', 'Telefone', 'Celular', 'Email', 'CRO', 'Paciente Inst Ensino'], axis=1, inplace=True)
        self.bases['clientesdf'].fillna({'Tipo de cliente': 'Varejo', 'Ramo de atividade': 'Consultório',
                                        'Frequência de compra': 'Sem Histórico', 'Segmentação': 'Sem Histórico'}, inplace=True)
        # self.bases['clientesdf']['Data Cadastro'].fillna(method='ffill', inplace=True)
        self.bases['renegdf'] = self.bases['renegdf'][[
            'Cliente', 'Nº documento']].groupby('Cliente')['Nº documento'].nunique()
        self.bases['renegdf'].rename('Total Renegs', inplace=True)
        self.bases['cadastro'] = pd.merge(
            self.bases['clientesdf'], self.bases['prefiltrodf'], left_on='Cod Sap', right_on='Código', how='left')
        self.bases['cadastro'] = pd.merge(
            self.bases['cadastro'], self.bases['fatdf'], left_on='Cod Sap', right_on='ID parc.', how='left')
        self.bases['cadastro'] = pd.merge(
            self.bases['cadastro'], self.bases['renegdf'], left_on='Cod Sap', right_on='Cliente', how='left')
        self.bases['cadastro'] = pd.merge(
            self.bases['cadastro'], self.bases['sapdf'], left_on='Cod Sap', right_on='Cliente', how='left')
        # self.bases['cadastro'] = pd.merge(self.bases['cadastro'], self.bases['covid19'][['Código', 'Qtde Boletos Pagos', 'Índice de Pontualidade Ponderada por Faixa']], how='left')
        del self.bases['sapdf'], self.bases['clientesdf'], self.bases['renegdf'], self.bases['fatdf'], self.bases['prefiltrodf']
        self.bases['cadastro'].drop(['ID parc.', 'Nome 1', 'x', 'Código', 'Cod. Regional', 'Consultor interno no mercanet', 'Canal 1', 'Canal 2', 'Canal 3',
                                    'Distrito', 'Cód. Laboratórios', 'Base Laboratórios', 'Gestão  Interna', 'GNV', 'Consultor FVI', 'Gestão Externa'], axis=1, inplace=True)
        self.bases['cadastro'][('Vencidos', 'sum')].fillna(0, inplace=True)
        self.bases['cadastro'][('Vencidos', 'count')].fillna(0, inplace=True)
        self.bases['cadastro'][('Maior Atraso em Dias', 'max')].fillna(
            0, inplace=True)
        '''self.bases['cadastro'][('Total dos Vencidos Pré-Covid19', 'sum')].fillna(0, inplace=True)
        self.bases['cadastro'][('Total dos Vencidos Pré-Covid19', 'count')].fillna(0, inplace=True)'''
        self.bases['cadastro'][('Créditos', 'sum')].fillna(0, inplace=True)
        self.bases['cadastro']['Valor Faturado'].fillna(0, inplace=True)
        self.bases['cadastro']['Total Renegs'].fillna(0, inplace=True)
        self.bases['cadastro'][('Exposição Total', 'sum')
                               ].fillna(0, inplace=True)
        # self.bases['cadastro'][['Qtde Boletos Pagos', 'Índice de Pontualidade Ponderada por Faixa']].fillna(0, inplace=True)
        self.bases['cadastro']['Mensagem'].fillna('Dispensado', inplace=True)
        self.bases['cadastro'] = pd.merge(self.bases['cadastro'], self.bases['correldf'][[
                                          'ID SAP', 'ID SAP.1']], left_on='Cod Sap', right_on='ID SAP', how='left')
        self.bases['cadastro'].drop('ID SAP', inplace=True, axis=1)
        self.bases['cadastro'].rename({'Mensagem': 'Pré-filtro', 'Descrição': 'Obs Pré-filtro',
                                      'ID SAP.1': 'Cadastro Relacionado'}, axis=1, inplace=True)

    def CreditPolicyAppliance(self):
        "It effectivelly evaluates customer's data against the credit policy to 'stamp' eligibility on the database that later will be used as input for the robot itself."""
        self.bases['cadastro']['Fluxo'] = 'Clientes Novos'
        self.bases['cadastro'].loc[(self.bases['cadastro']['Frequência de compra'] == 'Frequente') | (
            self.bases['cadastro']['Frequência de compra'] == 'Não Frequente'), 'Fluxo'] = 'Varejo'
        self.bases['cadastro'].loc[(self.bases['cadastro']['Segmentação'] == 'A') | (
            self.bases['cadastro']['Segmentação'] == 'AAA'), 'Fluxo'] = 'Clientes Especiais'
        self.bases['cadastro'].loc[(self.bases['cadastro']['Tipo de cliente'] == 'Key Account') & (
            self.bases['cadastro'][('Exposição Total', 'sum')] >= 300), 'Fluxo'] = 'Key Account'
        # O Semáforo é o indicador primário de atrasos vigentes. É o principal driver da política e sua definição pode ser distinta para cada fluxo.
        atraso_tolerado_por_fluxo = {
            'Varejo': 100, 'Clientes Novos': 100, 'Clientes Especiais': 500, 'Key Account': 500}
        def ajust_fat(
            faturamento): return 1 if faturamento.empty == True else faturamento
        for fluxo, valor in atraso_tolerado_por_fluxo.items():
            self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] == fluxo) & (
                self.bases['cadastro'][('Vencidos', 'sum')] <= valor), 'Semáforo'] = 'Verde'
            self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] == fluxo) & (self.bases['cadastro'][('Vencidos', 'sum')] > 0) & (self.bases['cadastro'][(
                'Vencidos', 'sum')] > valor) & (self.bases['cadastro'][('Vencidos', 'sum')] / ajust_fat(self.bases['cadastro']['Valor Faturado']) <= 0.05), 'Semáforo'] = 'Amarelo'
            self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] == fluxo) & (self.bases['cadastro'][('Vencidos', 'sum')] > 0) & (self.bases['cadastro'][(
                'Vencidos', 'sum')] > valor) & (self.bases['cadastro'][('Vencidos', 'sum')] / ajust_fat(self.bases['cadastro']['Valor Faturado']) > 0.05), 'Semáforo'] = 'Vermelho'
        """A partir de março de 2020, no contexto da pandemia do Novo Coronavirus (COVID19), adotou-se a política de exceção de crédito que seria mais tolerante com clientes habituais e de bom histórico de pagamento de seus boletos.
        self.bases['cadastro'].loc[(self.bases['cadastro'][('Total dos Vencidos Pré-Covid19', 'sum')] == 0) &(self.bases['cadastro']['Índice de Pontualidade Ponderada por Faixa'] >= 75), 'Semáforo'] = 'Verde'
        self.bases['cadastro'].loc[(self.bases['cadastro'][('Total dos Vencidos Pré-Covid19', 'sum')] == 0) & (self.bases['cadastro']['Índice de Pontualidade Ponderada por Faixa'] >= 50) & (self.bases['cadastro']['Qtde Boletos Pagos'] >= 4), 'Semáforo'] = 'Verde' """
        # Histórico de Pagamento reflete a quantidade de renegociações que cada cliente fez nos últimos 13 meses.
        self.bases['cadastro']['Histórico de pagamentos'] = pd.cut(self.bases['cadastro']['Total Renegs'], bins=[
                                                                   0, 1, 2, 99], labels=['Alto', 'Médio', 'Baixo'], include_lowest=True)
        # Volume de Compras é baseado na informação de Segmentação vinda da base de clientes 3.6 do Mercanet. Como sugere o título, tem relação com o volume consumido por cada cliente.
        self.bases['cadastro'].loc[(self.bases['cadastro']['Segmentação'] == 'A') | (self.bases['cadastro']['Segmentação'] == 'AAA') | (
            self.bases['cadastro']['Segmentação'] == 'Black'), 'Volume de Compras LTM'] = 'Alto'
        # self.bases['cadastro'].loc[(self.bases['cadastro']['Segmentação'] == 'B') | (self.bases['cadastro']['Segmentação'] == 'C') | (self.bases['cadastro']['Segmentação'] == 'Platinum'), 'Volume de Compras LTM'] = 'Médio'
        self.bases['cadastro']['Volume de Compras LTM'].fillna(
            'Baixo', inplace=True)
        # Dependendo das variáveis acima calculadas, especialmente Histórico de Pagamento e Volume de compras, cada cliente poderá ter um limite 'pré-aprovado' de compra (por dados_pedido):
        self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] != 'Clientes Novos') & (self.bases['cadastro']['Histórico de pagamentos']
                                                                                            == 'Alto'), 'Limite Aprovação Automática'] = np.maximum(15000, self.bases['cadastro']['Valor Faturado'] * 2)
        self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] != 'Clientes Novos') & (self.bases['cadastro']['Histórico de pagamentos'] == 'Médio') & (
            self.bases['cadastro']['Volume de Compras LTM'] == 'Alto'), 'Limite Aprovação Automática'] = np.maximum(self.bases['cadastro']['Valor Faturado'] * 2, 15000)
        self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] != 'Clientes Novos') & (self.bases['cadastro']['Histórico de pagamentos'] == 'Médio') & (
            self.bases['cadastro']['Volume de Compras LTM'] == 'Médio'), 'Limite Aprovação Automática'] = np.maximum(5000, self.bases['cadastro']['Valor Faturado'])
        self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] != 'Clientes Novos') & (
            self.bases['cadastro']['Limite Aprovação Automática'].isna() == True), 'Limite Aprovação Automática'] = 1000
        self.bases['cadastro'].loc[self.bases['cadastro']['Fluxo']
                                   == 'Clientes Novos', 'Limite Aprovação Automática'] = 300

    def SaveToFile(self):
        """Save customers'data fully processed on the SQLite database."""
        with sqlite3.connect(os.path.join(self.dir_base, self.caminho_basesql)) as base:
            self.bases['cadastro'].to_sql(
                name='Cadastro_completo', con=base, if_exists='replace')

    def CustomerDataQuery(self, codigo_sap):
        """This function queries a customer by it's ID on the bot's SQLite database"""
        with sqlite3.connect(os.path.join(self.dir_base, self.caminho_basesql)) as base:
            base_cursor = base.cursor()
            base_cursor.row_factory = sqlite3.Row
            try:
                customer_data = dict(base_cursor.execute(
                    'select * from cadastro_completo where "Cod Sap" = {}'.format(codigo_sap)).fetchone())
            except:
                customer_data = dict(base_cursor.execute(
                    'select * from cadastro_completo order by [Cod Sap] desc limit 1').fetchone())
            # customer_data_dict = {'Pré-Filtro':customer_data[0], 'Fluxo':customer_data[1], 'Semáforo':customer_data[2], 'Volume de Compras LTM':customer_data[3], 'Histórico de Pagamentos':customer_data[4], 'Valor Faturado':customer_data[5]}
        return customer_data

    def TestQuery(self, codigo_sap):
        base_cursor = sqlite3.connect(self.caminho_basesql).cursor()
        base_cursor.row_factory = sqlite3.Row
        customer_data = list(base_cursor.execute(
            'select * from cadastro_completo where "Cod Sap" = {}'.format(codigo_sap)).fetchone())
        return customer_data

    def GravarRegistroHistorico(self, dict_pedido):
        """TThis function tracks all decisions made by the bot, like a log to be audited whenever there's any contestation."""
        nome_arq = os.path.join(self.dir_base, 'Controle de pedidos - ' + str(datetime.date.today().day).zfill(
            2) + str(datetime.date.today().month).zfill(2) + str(datetime.date.today().year) + '.csv')
        with open(nome_arq, 'a', newline='') as csv_arq:
            file_manipulator = csv.DictWriter(
                csv_arq, list(dict_pedido.keys()), delimiter=';')
            if os.path.getsize(nome_arq) == 0:
                file_manipulator.writeheader()
            else:
                pass
            file_manipulator.writerow(dict_pedido)
