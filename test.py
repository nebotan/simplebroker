#!/usr/bin/env python3

# Простой скрипт, чтобы проверить интеграцию основных компонентов сервиса брокера очередей.
# Скрипрт подключается к уже запущенному сервису и делает следующие REST вызовы:
# * Помещает в очередь два сообщения
# * Читает из очереди два сообщения (ожидается код 200 и JSON с сообщениями)
# * Пытается с таймаутом прочитать сообщение, которого нет, (ожидается код 404)
# В скрипте нет автоматических проверок правильности работы. Проверку выполняет человек, читая логи скрипта.
# Скрипт логирует URL'и, HTTP codes, и JSON'ы.

import requests, logging, argparse

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--url", dest="url", default="http://localhost:8080", help="URL of queue service")
parser.add_argument("--name", dest="queueName", default="first", help="name  of a queue in queue service")

args = parser.parse_args()

url_pref = args.url + "/queue/" + args.queueName

def parse_reponse(res):
    logging.info("status_code: %d", res.status_code)
    try:
        logging.info(res.json())
    except:
        pass

def do_operation(method, url, json = None):
    logging.info("=" * 20)
    logging.info("%s %s || %s", method,  url, str(json))
    if method == "PUT":
        parse_reponse(requests.put(url, json=json))
    elif method == "GET":
        parse_reponse(requests.get(url))
    else:
        raise Exception("Unsupported method: " + method)

    pass

url = url_pref

do_operation("PUT", url, {"message": "message 1"})
do_operation("PUT", url, {"message": "message 2"})
do_operation("GET", url)
do_operation("GET", url + "?timeout=3")
logging.info("=" * 20)
logging.info("Example of getting message from empty queue")
do_operation("GET", url + "?timeout=3")