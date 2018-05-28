import requests
import json
from typing import Optional


class YandexRuEnTranslator(object):

    _api_key = None
    _lang = 'ru-en'
    _base_url = 'https://translate.yandex.net/api/v1.5/tr.json/translate'

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._ru_en_template = self._base_url \
            + '?key={}'.format(self._api_key)\
            + '&text={}'\
            + '&lang={}'.format(self._lang)

    def _get_error_text(self, resp_context: bytes):
        result = ''
        try:
            item = json.loads(resp_context.decode('utf-8'))
            result = item['message']
        except:
            pass
        finally:
            return result

    def _send_request(self, text: str) -> requests.Response:
        r = requests.post(
            url=self._ru_en_template.format(text)
        )
        if r.status_code != 200:
            raise Exception('Request returns not 200: {0}, {1}'.format(r.status_code, self._get_error_text(r.content)))
        return r

    def _handle_response(self, resp: requests.Response) -> str:
        item = json.loads(resp.content.decode('utf-8'))
        if item['code'] != 200:
            raise Exception('API error: {}'.format(item['message']))
        return item['text'][0]

    def translate(self, text: str = None) -> Optional[str]:
        if text is None:
            return None
        resp = self._send_request(text)
        result = self._handle_response(resp)
        return result
