import logging
from typing import List
from MySQLHelper import MySQLHelper
from mysql.connector import MySQLConnection
from YandexRuEnTranslator import YandexRuEnTranslator


class NixTranslator(object):

    BATCH_SIZE = 100

    def __init__(self, api_key: str):
        self._translator = YandexRuEnTranslator(api_key)

    def translate(self):
        conn = MySQLHelper.connection_inst()
        conn.connect()
        cnx = None
        try:
            cnx = conn.cursor()
            qeuery = 'select `id`, `title` from unit where id > 0'


        except Exception as e:
            if conn.in_transaction:
                conn.rollback()
            logging.error(e)
        finally:
            cnx = None
            conn.close()
            conn = None


    def _update_batch(self, translated):
        conn = MySQLHelper.connection_inst()  # type: MySQLConnection
        conn.connect()
        cnx = None
        try:
            query = 'update `unit` set `title_en`=%(title_en)s where `id`=%(id)s'
            cnx = conn.cursor()
            cnx.executemany(query, translated)
            conn.commit()
        except Exception as e:
            if conn.in_transaction:
                conn.rollback()
            logging.error(e)
        finally:
            cnx = None
            conn.close()
            conn = None
