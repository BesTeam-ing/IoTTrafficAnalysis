# Start with a basic flask app webpage.
from flask_socketio import SocketIO, emit
from flask import Flask, render_template, url_for, copy_current_request_context
from random import random
from time import sleep
from threading import Thread, Event
import datetime

from cassandra.cluster import Cluster

__author__ = 'Besteaming'

host = 'localhost'
port = '9042'
keyspace = 'traffickeyspace'

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
app.config['DEBUG'] = True
async_mode = None

#turn the flask app into a socketio app
socketio = SocketIO(app, async_mode=async_mode, logger=True, engineio_logger=True)

#random number Generator Thread
thread = Thread()
thread_stop_event = Event()

sec = 30

def db_read():
    cluster = Cluster([host], port=port)
    session = cluster.connect(keyspace)
    all_p = []
    all_c = []

    all_points = session.execute("SELECT latitude, longitude FROM traffickeyspace.points WHERE type='COORD' ALLOW FILTERING")
    all_centers = session.execute("SELECT latitude, longitude FROM traffickeyspace.points WHERE type='CENTER' AND timestamp > '" + str((datetime.datetime.now() - datetime.timedelta(seconds=sec)).strftime("%Y-%m-%d %H:%M:%S")) + "' ALLOW FILTERING")
    
    for point in all_points:
        all_p.append({
            'x': point.latitude,
            'y': point.longitude
            })

    for c in all_centers:
        all_c.append({
            'x': c.latitude,
            'y': c.longitude
            })
    return all_p, all_c
    

def thread_gen():
    print("Aquiring new data from Cassandra ...")
    while not thread_stop_event.isSet():
        all_p, all_c = db_read()
        print("Centers:", all_c)
        socketio.emit('coordinates', {'points': {'all':all_p, 'centers': all_c}}, namespace='/test')
        socketio.sleep(10)


@app.route('/')
def index():
    #only by sending this page first will the client be connected to the socketio instance
    return render_template('index.html')

@socketio.on('connect', namespace='/test')
def test_connect():
    # need visibility of the global thread object
    global thread
    print('Client connected')

    #Start the random number generator thread only if the thread has not been started before.
    if not thread.isAlive():
        print("Starting Thread")
        thread = socketio.start_background_task(thread_gen)

@socketio.on('disconnect', namespace='/test')
def test_disconnect():
    print('Client disconnected')


if __name__ == '__main__':
    socketio.run(app,host='0.0.0.0', port=5000)

