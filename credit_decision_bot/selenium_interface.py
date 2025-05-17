# *-* encoding: utf-8 *-*

import datetime
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.common import exceptions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.firefox.firefox_binary import FirefoxBinary.
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


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
        self.nav.find_element_by_id(
            "txtUsuario").send_keys(self.credenciais[0])
        self.nav.find_element_by_id("txtSenha").send_keys('Si@2023')
        self.nav.find_element_by_id("btnEntrar").click()

    def LoadTab(self, path_as_list):
        self.nav.switch_to.parent_frame()
        WebDriverWait(self.nav, self.timeout).until(
            expected_conditions.element_to_be_clickable((By.ID, self.menu_principal_id))).click()
        frames = [frame.get_attribute(
            'name') for frame in self.nav.find_elements_by_tag_name('iframe')]
        WebDriverWait(self.nav, self.timeout).until(
            expected_conditions.element_to_be_clickable((By.CSS_SELECTOR, 'table.keepMenuOpen')))
        for chunk in path_as_list:
            WebDriverWait(self.nav, self.timeout).until(
                expected_conditions.element_to_be_clickable((By.CSS_SELECTOR, 'table.keepMenuOpen')))
            for option in self.nav.find_elements_by_css_selector('table.keepMenuOpen'):
                if chunk.lower() in str(option.text).lower():
                    option.click()
                    break
                else:
                    pass
        while len(self.nav.find_elements_by_tag_name('iframe')) == len(frames):
            sleep(1)
        self.frames[path_as_list[-1]] = [frame.get_attribute('name') for frame in self.nav.find_elements_by_tag_name(
            'iframe') if frame.get_attribute('name') not in frames][0]
        frames = [frame.get_attribute(
            'name') for frame in self.nav.find_elements_by_tag_name('iframe')]
        self.nav.switch_to.frame(
            self.nav.find_element_by_name(self.frames[path_as_list[-1]]))

    def SwitchTab(self, tab_title):
        self.nav.switch_to.parent_frame()
        sleep(1)
        WebDriverWait(self.nav, self.timeout).until(
            expected_conditions.presence_of_element_located((By.PARTIAL_LINK_TEXT, tab_title))).click()
        sleep(0.5)
        frames = [frame.get_attribute(
            'name') for frame in self.nav.find_elements_by_tag_name('iframe')]
        self.nav.switch_to.frame(
            self.nav.find_element_by_name(self.frames[tab_title]))

    def NovoOrdersQueueAnalisis(self):
        while WebDriverWait(self.nav, self.timeout).until(expected_conditions.presence_of_element_located((By.ID, self.info_qtde_pedidos_pendentes_id))).get_attribute('value') == '0':
            self.last_time_updated = datetime.datetime.now()
            sleep(self.queue_refresh_time)
            WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable(
                (By.ID, self.botao_atualizar_mass_id))).click()
        self.last_time_updated = datetime.datetime.now()
        tab_pedidos = [linha for linha in WebDriverWait(self.nav, self.timeout).until(expected_conditions.visibility_of_element_located(
            (By.ID, self.tab_pedidos_id))).find_elements_by_tag_name('tr') if len(linha.text) >= 1]
        tab_pedidos = [Linha for Linha in tab_pedidos if len(Linha.text) > 1]
        cabecalho = ['-', '--'] + [' '.join(campo.text.split()) for campo in self.nav.find_element_by_id(
            self.cabecalho_tab_pedidos_id).find_elements_by_tag_name('tr')]
        for linha in [linha for linha in WebDriverWait(self.nav, self.timeout).until(expected_conditions.visibility_of_element_located((By.ID, self.tab_pedidos_id))).find_elements_by_tag_name('tr') if len(linha.text) >= 1]:
            dados_pedido = dict()
            for campo, valor in zip(cabecalho, linha.find_elements_by_tag_name('td')):
                dados_pedido[campo] = valor.text
            dados_pedido['Hora Análise'] = str(
                datetime.datetime.now().time())[:8]
            dados_pedido['Data Análise'] = str(datetime.date.today())
            del dados_pedido['-']
            del dados_pedido['--']
            try:
                self.nav.find_element_by_link_text(
                    dados_pedido['Pedido']).click()
            except:
                sleep(0.5)
            self.SwitchTab('Pedido Detalhe')
            dados_cliente = self.customer_database.CustomerDataQuery(
                dados_pedido['Cod. cliente'])
            dados_pedido['Decisão'] = Order(
                dados_pedido, dados_cliente).FinalAnswer()
            status_pedido = WebDriverWait(self.nav, self.timeout).until(
                expected_conditions.element_to_be_clickable((By.ID, self.status_pedido_detalhe_id))).text
            if WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable((By.ID, self.numero_pedido_detalhe))).text != dados_pedido['Pedido']:
                pass
            elif 'ABERTO' in status_pedido:
                pass
            elif 'cartão' in dados_pedido['Condição pagamento'].lower() and 'cliente na filial' not in WebDriverWait(self.nav, self.timeout).until(expected_conditions.presence_of_element_located((By.ID, self.opcao_faturamento_detalhe_id))).get_attribute('value').lower():
                WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable(
                    (By.ID, self.botao_liberar_detalhe_id))).click()
            elif dados_pedido['Decisão'] == ['Aprovação', 'Automática']:
                WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable(
                    (By.ID, self.botao_liberar_detalhe_id))).click()
            else:
                WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable(
                    (By.ID, self.botao_avaliar_detalhe_id))).click()
                WebDriverWait(self.nav, self.timeout).until(expected_conditions.presence_of_element_located(
                    (By.ID, self.entrada_obs_caixa_avaliar_id))).send_keys('Aguardando análise manual')
                WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable(
                    (By.ID, self.data_futura_avaliar_id))).send_keys(Keys.UP * 5)
                WebDriverWait(self.nav, self.timeout).until(expected_conditions.visibility_of_element_located(
                    (By.ID, self.botao_confirmar_caixa_avaliar))).click()
            self.customer_database.GravarRegistroHistorico(dados_pedido)
            self.SwitchTab('Liberação de Pedidos')
        while (datetime.datetime.now() - self.last_time_updated).seconds <= self.queue_refresh_time:
            sleep(1)
        WebDriverWait(self.nav, self.timeout).until(expected_conditions.element_to_be_clickable(
            (By.ID, self.botao_atualizar_mass_id))).click()


class Order():
    def __init__(self, dados_pedido, dados_cliente):
        self.dados_pedido = dados_pedido
        self.dados_pedido['Valor total Ajustado'] = float(
            self.dados_pedido['Valor total'].replace('.', '').replace(',', '.'))
        self.dados_cliente = dados_cliente
        if 'X' in self.dados_pedido['Condição pagamento'].title():
            pos = self.dados_pedido['Condição pagamento'].title().find('X')
            self.dados_pedido['Forma Pgto Ajustada'] = self.dados_pedido['Condição pagamento'].split()[
                0]
            self.dados_pedido['Prazo Pgto'] = int(
                self.dados_pedido['Condição pagamento'][pos-2:pos])
        else:
            self.dados_pedido['Forma Pgto Ajustada'] = self.dados_pedido['Condição pagamento'].split()[
                0]
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
            saida = 'Automática' if self.dados_pedido['Valor total Ajustado'] / \
                self.dados_pedido['Prazo Pgto'] >= 300 else 'Manual'
            return saida

    def CreditAnalisis(self):
        if self.dados_cliente['Pré-filtro'] != 'Dispensado':
            return 'Análise'
        elif self.dados_pedido['Valor total Ajustado'] <= 300:
            return self.AdjustedCreditTrafficLight()
        elif self.dados_cliente['Fluxo'] == 'Clientes Novos':
            saida = self.AdjustedCreditTrafficLight(
            ) if self.dados_pedido['Forma Pgto Ajustada'] == 'Cartão' else 'Análise'
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
