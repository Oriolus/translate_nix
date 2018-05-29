import logging
from typing import List, Dict
from MySQLHelper import MySQLHelper
from mysql.connector import MySQLConnection
from YandexRuEnTranslator import YandexRuEnTranslator


class NixTranslator(object):

    BATCH_SIZE = 100

    def __init__(self, api_key: str):
        self._translator = YandexRuEnTranslator(api_key)

    def translate_category(self):
        update_query = 'update `category` set `title_en`=%(title_en)s where `id`=%(id)s'
        conn = MySQLHelper.connection_inst()
        conn.connect()
        cnx = None
        try:
            cnx = conn.cursor()
            query = 'select `id`, `title` from `category` where id > 0'
            cnx.execute(query)
            translated = []  # type: List[Dict[str, str]]
            for _id, title in cnx.fetchall():
                translated.append({
                    'id': _id,
                    'title_en': self._translator.translate(title)
                })
                if len(translated) >= self.BATCH_SIZE:
                    MySQLHelper.update_many(update_query, translated)
                    translated = []

            if len(translated) > 0:
                MySQLHelper.update_many(update_query, translated)

        except Exception as e:
            if conn.in_transaction:
                conn.rollback()
            logging.error(e)
        finally:
            cnx = None
            conn.close()
            conn = None

    def translate_property(self):

        query = 'select distinct `key` as `key` from `property`' \
                ' where `key` is not null and `key` <> \'\' and `value` is not null'
        keys = MySQLHelper.fetch_all(query)

        update_query = 'update `property` set `key_en`=%(key_en)s where `key`=%(key)s'
        translated = []  # type: List[Dict[str, str]]
        for k in keys:
            translated.append({
                'key': k[0],
                'key_en': self._translator.translate(k[0])
            })
            if len(translated) > self.BATCH_SIZE:
                MySQLHelper.update_many(update_query, translated)
                translated = []

        if len(translated) > 0:
            MySQLHelper.update_many(update_query, translated)
