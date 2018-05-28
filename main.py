import requests
from settings import API_KEY
from YandexRuEnTranslator import YandexRuEnTranslator

trans = YandexRuEnTranslator(API_KEY)
try:
    to_trans = 'Привет, мир'
    print('{0}: {1}'.format(to_trans, trans.translate(to_trans)))
except Exception as e:
    print(e)

