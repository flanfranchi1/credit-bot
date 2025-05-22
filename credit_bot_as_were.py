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
import csv
import _thread
import sys
import os
import time
import datetime
import pandas as pd
import sqlite3 as sql
import numpy as np
from credit_decision_bot.etl import CustomerDataManager
from credit_decision_bot.selenium_interface import NavigationRobot

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

datamanager = CustomerDataManager(
    caminho_arquivos, r'z:\planejamento de crédito e cobrança\automatização de crédito\produtividade')
autocred = NavigationRobot(
    webdriver.Firefox(), datamanager, usuario_mercanet, senha_mercanet)
_thread.start_new_thread(datamanager.Updater, tuple())
datamanager.Updater()
autocred.Autoexec()
