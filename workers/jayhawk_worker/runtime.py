#SPDX-License-Identifier: MIT
from flask import Flask, jsonify, request, Response
import click, os, json, requests, logging
from workers.jayhawks_worker.jayhawks_worker import JayhawksWorker
from workers.util import create_server, WorkerGunicornApplication

def main():
    """
    Creates the Flask app and data collection worker, then starts the Gunicorn server
    """
    app = Flask(__name__)
    app.worker = JawhksWorker()

    create_server(app)
    WorkerGunicornApplication(app).run()

    if app.worker._child is not None:
        app.worker._child.terminate()
    try:
        requests.post('http://{}:{}/api/unstable/workers/remove'.format(broker_host, broker_port), json={"id": config['id']})
    except:
        pass

    os.kill(os.getpid(), 9)
