import logging
from typing import List, Dict
from MySQLHelper import MySQLHelper
from mysql.connector import MySQLConnection
from YandexRuEnTranslator import YandexRuEnTranslator


class NixTranslator(object):

    BATCH_SIZE = 100

    def __init__(self, api_key: str):
        self._translator = YandexRuEnTranslator(api_key)

    def translate_unit(self):
        conn = MySQLHelper.connection_inst()
        conn.connect()
        cnx = None
        try:
            cnx = conn.cursor()
            query = 'select `id`, `title` from unit where id > 0'
            cnx.execute(query)
            translated = []  # type: List[Dict[str, str]]
            for _id, title in cnx.fetchall():
                translated.append(
                    {'id': _id,
                     'title_en': self._translator.translate(title)}
                )
                if len(translated) >= 100:
                    self._update_batch(translated)
                    translated = []

            if len(translated) > 0:
                self._update_batch(translated)

        except Exception as e:
            if conn.in_transaction:
                conn.rollback()
            logging.error(e)
        finally:
            cnx = None
            conn.close()
            conn = None

    def _update_batch(self, translated: List[Dict[str, str]]):
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
