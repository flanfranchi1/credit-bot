# *-* encoding: utf-8 *-*
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.common import exceptions		
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.firefox.firefox_binary import FirefoxBinary.
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from time import sleep
import csv, _thread, sys, os, time, datetime, pandas as pd, sqlite3 as sql, numpy as np

usuario_mercanet = 'credit.bot'
senha_mercanet = 'password'

caminho_arquivos = {
	'sapdf': r'z:\planejamento de crédito e cobrança\Power BI\bases\CAR DIARIO.xlsx', 	
	'clientesdf': r'z:\planejamento de crédito e cobrança\Power BI\bases\base clientes.xlsm',
	'fatdf': r'z:\planejamento de crédito e cobrança\crédito\Posição Consolidada de Crédito.xlsm',
	'renegdf': r'z:\planejamento de crédito e cobrança\crédito\Posição Consolidada de Crédito.xlsm',
	'prefiltrodf': r'z:\planejamento de crédito e cobrança\crédito\Posição Consolidada de Crédito.xlsm',
	'correldf': r'z:\planejamento de crédito e cobrança\automatização de crédito\bases\Base Correlação.xlsx'
}
class CustomerDataManager:
	def __init__(self, repositorio_arquivos={'clientesdf':None, 'sapdf':None, 'renegdf':None, 'fatdf':None, 'prefiltrodf':None}, dir_base = os.getcwd()):
		self.dir_base = dir_base
		self.repositorio_arquivos = repositorio_arquivos
		self.caminho_basesql = 'cadastroclientes.db'
		self.bases = dict()
	def Updater(self):
		while os.path.exists('z:') == False:
			sleep(60)
		while datetime.datetime.fromtimestamp(os.path.getmtime(self.repositorio_arquivos['sapdf'])).date() < datetime.date.today() or datetime.datetime.fromtimestamp(os.path.getmtime(self.caminho_basesql)).date() == datetime.date.today():
			sleep(60)
		for nome, planilha in self.repositorio_arquivos.items():
			self.FilesLoader(nome, planilha)
			# del self.repositorio_arquivos[nome]
		self.DataProcessing()
		self.	CreditPolicyAppliance()
		self.SaveToFile()
	def FilesLoader(self, nome_base, caminho):
		if nome_base == 'sapdf':
			self.bases	[nome_base] = pd.read_excel(caminho)
		elif nome_base == 'clientesdf':
			self.bases[nome_base] = pd.read_excel(caminho)
		elif nome_base == 'fatdf':
			self.bases[nome_base] = pd.read_excel(caminho, sheet_name='FaturamentoLTM')
		elif nome_base == 'prefiltrodf': 
			self.bases[nome_base] = pd.read_excel(caminho, sheet_name='Bloqueios')
		elif nome_base == 'renegdf':
			self.bases[nome_base] = pd.read_excel(caminho, sheet_name='RENEG')
		else:
			self.bases[nome_base] = pd.read_excel(caminho)
	def DataProcessing(self):
		# Base de Partidas vencidas, consolidação e cálculo de índices:
		self.bases['sapdf'].loc[(self.bases['sapdf']['VencLíquid'] <= str(datetime.date.fromordinal(datetime.date.toordinal(datetime.date.today())-5))) & (self.bases['sapdf']['Compensaç.'].isna() == True) & (self.bases['sapdf']['BancEmpr'].isna() == False) & (self.bases['sapdf']['Mont.em MI'] > 0) & (self.bases['sapdf']['MP'] == 'N') & (self.bases['sapdf']['Solic.L/C'].isna() == True), 'Vencidos'] = self.bases['sapdf']['Mont.em MI']
		self.bases['sapdf'].loc[(self.bases['sapdf']['VencLíquid'] <= str(datetime.date.fromordinal(datetime.date.toordinal(datetime.date.today())-5))) & (self.bases['sapdf']['Compensaç.'].isna() == True) & (self.bases['sapdf']['BancEmpr'].isna() == False) & (self.bases['sapdf']['Mont.em MI'] > 0), 'Vencidos'] = self.bases['sapdf']['Mont.em MI']
		self.bases['sapdf'].loc[(self.bases['sapdf']['VencLíquid'] <= str(datetime.date.fromordinal(datetime.date.toordinal(datetime.date.today())-5))) & (self.bases['sapdf']['Compensaç.'].isna() == True) & (self.bases['sapdf']['BancEmpr'].isna() == False) & (self.bases['sapdf']['Mont.em MI'] > 0) & (self.bases['sapdf']['MP'] == 'N') & (self.bases['sapdf']['Solic.L/C'].isna() == False), 'Vencido em Dias'] = (pd.to_datetime('today') - self.bases['sapdf']['VencLíquid']).astype('timedelta64[D]') 
		# self.bases['sapdf'].loc[(self.bases['sapdf']['VencLíquid'] <= '2020-03-17') & (self.bases['sapdf']['Compensaç.'].isna() == True) & (self.bases['sapdf']['Chave referência 3'].isna() == False) & (self.bases['sapdf']['Mont.em MI'] > 0), 'Vencidos Pré-Covid19'] = self.bases['sapdf']['Mont.em MI']
		self.bases['sapdf'].loc[self.bases['sapdf']['Mont.em MI'] <0, 'Créditos'] = -self.bases['sapdf']['Mont.em MI']
		self.bases['sapdf'] = self.bases['sapdf'].groupby('Cliente').agg({'Mont.em MI':sum, 'Créditos':sum, 'Vencidos':[sum, 'count'], 'Vencido em Dias':max})
		self.bases['sapdf'].rename({'Mont.em MI':'Exposição Total', 'Vencido em Dias':'Maior Atraso em Dias'}, axis=1, inplace=True)
		# Base Cadastral de clientes - Limpeza:
		self.bases['clientesdf'] = self.bases['clientesdf'][(self.bases['clientesdf']['Cod Sap'] <= 999999)]
		self.bases['clientesdf'].drop(['Fantasia', 'Endereço', 'Nro', 'Complemento', 'Bairro', 'Cep', 'País', 'Telefone', 'Celular', 'Email', 'CRO', 'Paciente Inst Ensino'], axis=1, inplace=True)
		self.bases['clientesdf'].fillna({'Tipo de cliente':'Varejo', 'Ramo de atividade':'Consultório', 'Frequência de compra':'Sem Histórico', 'Segmentação':'Sem Histórico'}, inplace=True)
		# self.bases['clientesdf']['Data Cadastro'].fillna(method='ffill', inplace=True)
		self.bases['renegdf'] = self.bases['renegdf'][['Cliente', 'Nº documento']].groupby('Cliente')['Nº documento'].nunique()
		self.bases['renegdf'].rename('Total Renegs', inplace=True)
		self.bases['cadastro'] = pd.merge(self.bases['clientesdf'], self.bases['prefiltrodf'], left_on='Cod Sap', right_on='Código', how='left')
		self.bases['cadastro'] = pd.merge(self.bases['cadastro'], self.bases['fatdf'], left_on='Cod Sap', right_on='ID parc.', how='left')
		self.bases['cadastro'] = pd.merge(self.bases['cadastro'], self.bases['renegdf'], left_on='Cod Sap', right_on='Cliente', how='left')
		self.bases['cadastro'] = pd.merge(self.bases['cadastro'], self.bases['sapdf'], left_on='Cod Sap', right_on='Cliente', how='left')
		# self.bases['cadastro'] = pd.merge(self.bases['cadastro'], self.bases['covid19'][['Código', 'Qtde Boletos Pagos', 'Índice de Pontualidade Ponderada por Faixa']], how='left')
		del self.bases['sapdf'], self.bases['clientesdf'], self.bases['renegdf'], self.bases['fatdf'], self.bases['prefiltrodf']
		self.bases['cadastro'].drop(['ID parc.', 'Nome 1', 'x', 'Código', 'Cod. Regional', 'Consultor interno no mercanet', 'Canal 1', 'Canal 2', 'Canal 3', 'Distrito', 'Cód. Laboratórios', 'Base Laboratórios', 'Gestão  Interna', 'GNV', 'Consultor FVI', 'Gestão Externa'], axis=1, inplace=True)
		self.bases['cadastro'][('Vencidos', 'sum')].fillna(0, inplace=True)
		self.bases['cadastro'][('Vencidos', 'count')].fillna(0, inplace=True)
		self.bases['cadastro'][('Maior Atraso em Dias', 'max')].fillna(0, inplace=True)
		'''self.bases['cadastro'][('Total dos Vencidos Pré-Covid19', 'sum')].fillna(0, inplace=True)
		self.bases['cadastro'][('Total dos Vencidos Pré-Covid19', 'count')].fillna(0, inplace=True)'''
		self.bases['cadastro'][('Créditos', 'sum')].fillna(0, inplace=True)
		self.bases['cadastro']['Valor Faturado'].fillna(0, inplace=True)
		self.bases['cadastro']['Total Renegs'].fillna(0, inplace=True)
		self.bases['cadastro'][('Exposição Total', 'sum')].fillna(0, inplace=True)
		# self.bases['cadastro'][['Qtde Boletos Pagos', 'Índice de Pontualidade Ponderada por Faixa']].fillna(0, inplace=True)
		self.bases['cadastro']['Mensagem'].fillna('Dispensado', inplace=True)
		self.bases['cadastro'] = pd.merge(self.bases['cadastro'], self.bases['correldf'][['ID SAP', 'ID SAP.1']], left_on='Cod Sap', right_on='ID SAP', how='left')
		self.bases['cadastro'].drop('ID SAP', inplace=True, axis=1)
		self.bases['cadastro'].rename({'Mensagem':'Pré-filtro', 'Descrição':'Obs Pré-filtro', 'ID SAP.1':'Cadastro Relacionado'}, axis=1, inplace=True)
	def CreditPolicyAppliance(self):
		'''Interpreta os registros de cada cliente e cria parâmetros que se adequem a política de crédito vigente'''
		self.bases['cadastro']['Fluxo'] = 'Clientes Novos'
		self.bases['cadastro'].loc[(self.bases['cadastro']['Frequência de compra'] == 'Frequente') | (self.bases['cadastro']['Frequência de compra'] == 'Não Frequente'), 'Fluxo'] = 'Varejo'
		self.bases['cadastro'].loc[(self.bases['cadastro']['Segmentação'] == 'A') | (self.bases['cadastro']['Segmentação'] == 'AAA'), 'Fluxo'] = 'Clientes Especiais'
		self.bases['cadastro'].loc[(self.bases['cadastro']['Tipo de cliente'] == 'Key Account') & (self.bases['cadastro'][('Exposição Total', 'sum')] >= 300), 'Fluxo'] = 'Key Account'
		# O Semáforo é o indicador primário de atrasos vigentes. É o principal driver da política e sua definição pode ser distinta para cada fluxo.
		atraso_tolerado_por_fluxo = {'Varejo':100, 'Clientes Novos':100, 'Clientes Especiais':500, 'Key Account':500}
		ajust_fat = lambda faturamento: 1 if faturamento.empty == True else faturamento
		for fluxo, valor in atraso_tolerado_por_fluxo.items():
			self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] == fluxo) & (self.bases['cadastro'][('Vencidos', 'sum')] <= valor), 'Semáforo'] = 'Verde'
			self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] == fluxo) & (self.bases['cadastro'][('Vencidos', 'sum')] > 0) & (self.bases['cadastro'][('Vencidos', 'sum')] > valor) & (self.bases['cadastro'][('Vencidos', 'sum')] / ajust_fat(self.bases['cadastro']['Valor Faturado']) <= 0.05), 'Semáforo'] = 'Amarelo'
			self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] == fluxo) & (self.bases['cadastro'][('Vencidos', 'sum')] > 0) & (self.bases['cadastro'][('Vencidos', 'sum')] > valor) & (self.bases['cadastro'][('Vencidos', 'sum')] / ajust_fat(self.bases['cadastro']['Valor Faturado']) > 0.05), 'Semáforo'] = 'Vermelho'
		"""A partir de março de 2020, no contexto da pandemia do Novo Coronavirus (COVID19), adotou-se a política de exceção de crédito que seria mais tolerante com clientes habituais e de bom histórico de pagamento de seus boletos.
		self.bases['cadastro'].loc[(self.bases['cadastro'][('Total dos Vencidos Pré-Covid19', 'sum')] == 0) &(self.bases['cadastro']['Índice de Pontualidade Ponderada por Faixa'] >= 75), 'Semáforo'] = 'Verde'
		self.bases['cadastro'].loc[(self.bases['cadastro'][('Total dos Vencidos Pré-Covid19', 'sum')] == 0) & (self.bases['cadastro']['Índice de Pontualidade Ponderada por Faixa'] >= 50) & (self.bases['cadastro']['Qtde Boletos Pagos'] >= 4), 'Semáforo'] = 'Verde' """
		# Histórico de Pagamento reflete a quantidade de renegociações que cada cliente fez nos últimos 13 meses.
		self.bases['cadastro']['Histórico de pagamentos'] = pd.cut(self.bases['cadastro']['Total Renegs'], bins=[0, 1, 2, 99], labels=['Alto', 'Médio', 'Baixo'], include_lowest=True)
		# Volume de Compras é baseado na informação de Segmentação vinda da base de clientes 3.6 do Mercanet. Como sugere o título, tem relação com o volume consumido por cada cliente.	
		self.bases['cadastro'].loc[(self.bases['cadastro']['Segmentação'] == 'A') | (self.bases['cadastro']['Segmentação'] == 'AAA') | (self.bases['cadastro']['Segmentação'] == 'Black'), 'Volume de Compras LTM'] = 'Alto'
		# self.bases['cadastro'].loc[(self.bases['cadastro']['Segmentação'] == 'B') | (self.bases['cadastro']['Segmentação'] == 'C') | (self.bases['cadastro']['Segmentação'] == 'Platinum'), 'Volume de Compras LTM'] = 'Médio'
		self.bases['cadastro']['Volume de Compras LTM'].fillna('Baixo', inplace=True)
		# Dependendo das variáveis acima calculadas, especialmente Histórico de Pagamento e Volume de compras, cada cliente poderá ter um limite 'pré-aprovado' de compra (por dados_pedido):
		self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] != 'Clientes Novos') & (self.bases['cadastro']['Histórico de pagamentos'] == 'Alto'), 'Limite Aprovação Automática'] = np.maximum(15000, self.bases['cadastro']['Valor Faturado'] * 2)
		self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] != 'Clientes Novos') & (self.bases['cadastro']['Histórico de pagamentos'] == 'Médio') & (self.bases['cadastro']['Volume de Compras LTM'] == 'Alto'), 'Limite Aprovação Automática'] = np.maximum(self.bases['cadastro']['Valor Faturado'] * 2, 15000)
		self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] != 'Clientes Novos') & (self.bases['cadastro']['Histórico de pagamentos'] == 'Médio') & (self.bases['cadastro']['Volume de Compras LTM'] == 'Médio'), 'Limite Aprovação Automática'] = np.maximum(5000, self.bases['cadastro']['Valor Faturado'])
		self.bases['cadastro'].loc[(self.bases['cadastro']['Fluxo'] != 'Clientes Novos') & (self.bases['cadastro']['Limite Aprovação Automática'].isna() == True), 'Limite Aprovação Automática'] = 1000
		self.bases['cadastro'].loc[self.bases['cadastro']['Fluxo'] == 'Clientes Novos', 'Limite Aprovação Automática'] = 300
	def SaveToFile(self):
		with sql.connect(os.path.join(self.dir_base, self.caminho_basesql)) as base:
			self.bases['cadastro'].to_sql(name=	'Cadastro_completo', con=base, if_exists='replace')
	def CustomerDataQuery(self, codigo_sap):
		with sql.connect(os.path.join(self.dir_base, self.caminho_basesql)) as base:
		 base_cursor = base.cursor()
		 base_cursor.row_factory = sql.Row
		 try:
 			customer_data = dict(base_cursor.execute('select * from cadastro_completo where "Cod Sap" = {}'.format(codigo_sap)).fetchone())
		 except:
			 customer_data = dict(base_cursor.execute('select * from cadastro_completo order by [Cod Sap] desc limit 1').fetchone())
		 # customer_data_dict = {'Pré-Filtro':customer_data[0], 'Fluxo':customer_data[1], 'Semáforo':customer_data[2], 'Volume de Compras LTM':customer_data[3], 'Histórico de Pagamentos':customer_data[4], 'Valor Faturado':customer_data[5]}
		return customer_data
	def TestQuery(self, codigo_sap):
		base_cursor = sql.connect(self.caminho_basesql).cursor()
		base_cursor.row_factory = sql.Row
		customer_data = list(base_cursor.execute('select * from cadastro_completo where "Cod Sap" = {}'.format(codigo_sap)).fetchone())
		return customer_data
	def GravarRegistroHistorico(self, dict_pedido):
		nome_arq = os.path.join(self.dir_base, 'Controle de pedidos - ' + str(datetime.date.today().day).zfill(2) +str(datetime.date.today().month).zfill(2) + str(datetime.date.today().year) + '.csv')
		with open(nome_arq, 'a', newline='') as csv_arq:
			file_manipulator = csv.DictWriter(csv_arq, list(dict_pedido.keys()), delimiter=';')
			if os.path.getsize(nome_arq) == 0:
				file_manipulator.writeheader()
			else:
				pass
			file_manipulator.writerow(dict_pedido)

class NavigationRobot:
	def __init__(self, webdriver, customer_database, user, pwd, url='https://www.anyonlinecrm.com/login/login.aspx', timeout=45):
		self.customer_database = customer_database
		self.nav = webdriver
		self.nav.maximize_window()
		self.nav.implicitly_wait(0.7)
		self.nav.get(url)
		self.credenciais = [user, pwd]
		self.timeout = timeout
		self.queue_refresh_time = 10
		self.last_time_updated = datetime.datetime.now()
		self.menu_principal_id = 'iconeMenuWrapper'
		self.tab_pedidos_id = 'ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_grdPedidosLiberarAvaliar_DXMainTable'
		self.cabecalho_tab_pedidos_id = 'ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_grdPedidosLiberarAvaliar_DXHeadersRow0'
		self.processando_class = 'fundo-processando'
		self.botao_atualizar_mass_id = "ctl00_ctl00_mbuToolbar_DXI4_T"
		self.botao_liberar_detalhe_id = 'ctl00_ctl00_ctl00_mbuToolbar_DXI5_Img'
		self.botao_avaliar_detalhe_id = 'ctl00_ctl00_ctl00_mbuToolbar_DXI7_T'
		self.caixa_avaliar_id = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_popupAvaliar_PW-1'
		self.entrada_obs_caixa_avaliar_id = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_popupAvaliar_txtObservacao_I'
		self.botao_confirmar_caixa_avaliar = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_popupAvaliar_btnConfirmarAvalicao_I'
		self.link_download_anexo_detalhe_id = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_tabPedidoVenda_grdAnexos_cell0_1_lnkDownloadAnexo_Click'
		self.info_qtde_pedidos_pendentes_id = 'ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_txtLiberarOuAvaliarQuantidade_I'
		self.opcao_faturamento_detalhe_id = "ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_tabPedidoVenda_txtOpcaoFaturamento_I"
		self.caixa_avaliar_id = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_popupAvaliar_PW-1'
		self.caixa_data_futura_detalhe_id = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_popupAvaliar_txtPrevisaoLiberacao_I'
		self.data_futura_avaliar_id = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_popupAvaliar_txtPrevisaoLiberacao_I'
		self.prazo_avaliacao_futura = 5
		self.botao_confirmar_caixa_avaliar = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_popupAvaliar_btnConfirmarAvalicao'
		self.info_situacao_pedido_detalhe_id = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_lblSituacaoPedido'
		self.msg_avaliar_atrasos = 'Cliente com partida(s) vencida(s): constam {} boleto(s) vencido(s), no total de R$ {}, sendo o maior atraso de {} dias. Pedido será analisado após a regularização da(s) pendência(s).'
		self.status_pedido_detalhe_id = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_lblSituacaoPedido'
		self.numero_pedido_detalhe = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_lblNrPedido'
		self.frames = dict()
	def Autoexec(self):
		self.Login()
		self.LoadTab(['Pedidos', 'Pedido Detalhe'])
		self.LoadTab(['Pedidos', 'Liberação de Pedidos'])
		self.SwitchTab('Liberação de Pedidos')
		while True:
			self.NovoOrdersQueueAnalisis()
	def Login(self):
	 self.nav.find_element_by_id("txtUsuario").send_keys(self.credenciais[0])
	 self.nav.find_element_by_id("txtSenha").send_keys('Si@2023')
	 self.nav.find_element_by_id("btnEntrar").click()
	def LoadTab(self, path_as_list):
		self.nav.switch_to.parent_frame()
		WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.ID, self.menu_principal_id))).click()
		frames = [frame.get_attribute('name') for frame in self.nav.find_elements_by_tag_name('iframe')]
		WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.CSS_SELECTOR, 'table.keepMenuOpen')))
		for chunk in path_as_list:
			WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.CSS_SELECTOR, 'table.keepMenuOpen')))
			for option in self.nav.find_elements_by_css_selector('table.keepMenuOpen'):
				if chunk.lower() in str(option.text).lower():
					option.click()
					break
				else:
					pass
		while len(self.nav.find_elements_by_tag_name('iframe')) == len(frames):
			sleep(1)
		self.frames[path_as_list[-1]] = [frame.get_attribute('name') for frame in self.nav.find_elements_by_tag_name('iframe') if frame.get_attribute('name') not in frames][0]
		frames = [frame.get_attribute('name') for frame in self.nav.find_elements_by_tag_name('iframe')]
		self.nav.switch_to.frame(self.nav.find_element_by_name(self.frames[path_as_list[-1]]))
	def SwitchTab(self, tab_title):
		self.nav.switch_to.parent_frame()
		sleep(1)
		WebDriverWait(self.nav, self.timeout).until(expected_conditions.presence_of_element_located((By.PARTIAL_LINK_TEXT, tab_title))).click()
		sleep(0.5)
		frames = [frame.get_attribute('name') for frame in self.nav.find_elements_by_tag_name('iframe')]
		self.nav.switch_to.frame(self.nav.find_element_by_name(self.frames[tab_title]))
	def NovoOrdersQueueAnalisis(self):
		while WebDriverWait(self.nav, self.timeout).until(expected_conditions.presence_of_element_located((By.ID, self.info_qtde_pedidos_pendentes_id))).get_attribute('value') == '0':
		 self.last_time_updated = datetime.datetime.now()	
		 sleep(self.queue_refresh_time)
		 WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.ID, self.botao_atualizar_mass_id))).click()
		self.last_time_updated = datetime.datetime.now()
		tab_pedidos = [linha for linha in WebDriverWait(self.nav, self.timeout).until(expected_conditions.visibility_of_element_located((By.ID, self.tab_pedidos_id))).find_elements_by_tag_name('tr') if len(linha.text) >= 1]
		tab_pedidos = [Linha for Linha in tab_pedidos if len(Linha.text) > 1]
		cabecalho = ['-', '--'] + [' '.join(campo.text.split()) for campo in self.nav.find_element_by_id(self.cabecalho_tab_pedidos_id).find_elements_by_tag_name('tr')]
		for linha in [linha for linha in WebDriverWait(self.nav, self.timeout).until(expected_conditions.visibility_of_element_located((By.ID, self.tab_pedidos_id))).find_elements_by_tag_name('tr') if len(linha.text) >= 1]:
			dados_pedido = dict()
			for campo, valor in  zip(cabecalho, linha.find_elements_by_tag_name('td')):
				dados_pedido[campo] = valor.text
			dados_pedido['Hora Análise'] = str(datetime.datetime.now().time())[:8]
			dados_pedido['Data Análise'] = str(datetime.date.today())
			del dados_pedido['-']; del dados_pedido['--']
			try:
			 self.nav.find_element_by_link_text(dados_pedido['Pedido']).click()
			except:
				sleep(0.5)
			self.SwitchTab('Pedido Detalhe')
			dados_cliente = self.customer_database.CustomerDataQuery(dados_pedido['Cod. cliente'])
			dados_pedido['Decisão'] = Order(dados_pedido, dados_cliente).FinalAnswer()
			status_pedido = WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.ID, self.status_pedido_detalhe_id))).text
			if WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.ID, self.numero_pedido_detalhe))).text != dados_pedido['Pedido']:
				pass
			elif 'ABERTO' in status_pedido:
				pass
			elif 'cartão' in dados_pedido['Condição pagamento'].lower() and 'cliente na filial' not in WebDriverWait(self.nav, self.timeout).until(expected_conditions.presence_of_element_located((By.ID, self.opcao_faturamento_detalhe_id))).get_attribute('value').lower():
				WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.ID, self.botao_liberar_detalhe_id))).click()
			elif dados_pedido['Decisão'] == ['Aprovação', 'Automática']:
				WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.ID, self.botao_liberar_detalhe_id))).click()
			else:
				WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.ID, self.botao_avaliar_detalhe_id))).click()
				WebDriverWait(self.nav, self.timeout).until(expected_conditions.presence_of_element_located((By.ID, self.entrada_obs_caixa_avaliar_id))).send_keys('Aguardando análise manual')
				WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.ID, self.data_futura_avaliar_id))).send_keys(Keys.UP * 5)
				WebDriverWait(self.nav, self.timeout).until(expected_conditions.visibility_of_element_located((By.ID, self.botao_confirmar_caixa_avaliar))).click()
			self.customer_database.GravarRegistroHistorico(dados_pedido)
			self.SwitchTab('Liberação de Pedidos')
		while (datetime.datetime.now() - self.last_time_updated).seconds <= self.queue_refresh_time:
			sleep(1)
		WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.ID, self.botao_atualizar_mass_id))).click()

class Order():
	def __init__(self, dados_pedido, dados_cliente):
		self.dados_pedido = dados_pedido
		self.dados_pedido['Valor total Ajustado'] = float(self.dados_pedido['Valor total'].replace('.', '').replace(',', '.'))
		self.dados_cliente = dados_cliente
		if 'X' in self.dados_pedido['Condição pagamento'].title():
			pos = self.dados_pedido['Condição pagamento'].title().find('X')
			self.dados_pedido['Forma Pgto Ajustada'] = self.dados_pedido['Condição pagamento'].split()[0]
			self.dados_pedido['Prazo Pgto'] = int(self.dados_pedido['Condição pagamento'][pos-2:pos])
		else:
			self.dados_pedido['Forma Pgto Ajustada'] = self.dados_pedido['Condição pagamento'].split()[0]
			self.dados_pedido['Prazo Pgto'] = 1
			self.decisao = ['Análise', 'Manual']
		self.msg_analise_manual = 'Pedido aguardando análise de crédito'
		self.msg_cobranca = 'Os atrasos do cliente excedem o limite tolerado pela Política de Crédito. O cliente deve regularizar as pendências para que seu dados_pedido seja analisado.'
		self.id_forma_pgto_detalhe = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_tabPedidoVenda_txtCondPagamento_I'
		self.id_opcao_faturamento_detalhe = 'ctl00_ctl00_ctl00_PageContentPlaceHolder_PageContentPlaceHolder_tabPedidoVenda_txtOpcaoFaturamento_I'
		self.id_btn_liberar_detalhe = 'ctl00_ctl00_ctl00_mbuToolbar_DXI5_Img'
	def NonCreditAnalisis(self):
		if self.dados_pedido['Forma Pgto Ajustada'] in ['Boleto', 'Compensação']:
			return self.PaymentThermAnalisis()
		elif self.dados_pedido['Forma Pgto Ajustada'] == 'Cartão':
			return 'Individual'
		elif self.dados_pedido['Forma Pgto Ajustada'] == 'Cheque' and self.dados_cliente['Fluxo'] == 'Key Account':
			return self.PaymentThermAnalisis()
		else:	
			return 'Manual'
	def PaymentThermAnalisis(self):
		if self.dados_pedido['Prazo Pgto'] == 1:
			return 'Automática'
		elif self.dados_pedido['Prazo Pgto'] > 1:
			saida = 'Automática' if self.dados_pedido['Valor total Ajustado']/self.dados_pedido['Prazo Pgto'] >= 300 else 'Manual'
			return saida
	def CreditAnalisis(self):
		if self.dados_cliente['Pré-filtro'] != 'Dispensado':
			return 'Análise'
		elif self.dados_pedido['Valor total Ajustado'] <= 300:
			return self.AdjustedCreditTrafficLight()
		elif self.dados_cliente['Fluxo'] == 'Clientes Novos':
			saida = self.AdjustedCreditTrafficLight() if self.dados_pedido['Forma Pgto Ajustada'] == 'Cartão' else 'Análise'
			return saida
		elif self.dados_pedido['Valor total Ajustado'] <= 1000:
			return self.AdjustedCreditTrafficLight()
		elif self.dados_pedido['Valor total Ajustado'] <= self.dados_cliente['Limite Aprovação Automática']:
			return self.AdjustedCreditTrafficLight()
		else:
			return 'Análise'
	def AdjustedCreditTrafficLight(self):
		if self.dados_cliente['Semáforo'] == 'Verde':
			return 'Aprovação'
		elif self.dados_cliente['Semáforo'] == 'Vermelho':
			return 'Cobrança'
		elif self.dados_cliente['Fluxo'] == 'Key Account':
			return 'Aprovação'
		else:
			return 'Cobrança'
	def FinalAnswer(self):
		saida = [self.CreditAnalisis(), self.NonCreditAnalisis()]
		if saida[0] == 'Análise':
			saida[1] = 'Manual'
		elif saida[0] == 'Cobrança':
			saida[1] = 'Automática'
		return saida
datamanager = CustomerDataManager(caminho_arquivos, r'z:\planejamento de crédito e cobrança\automatização de crédito\produtividade')
autocred = NavigationRobot(webdriver.Firefox(), datamanager, usuario_mercanet, senha_mercanet)
_thread.start_new_thread(datamanager.Updater, tuple())
autocred.Autoexec()