import json
from flask import Flask, render_template
from flask_socketio import SocketIO
from kafka import KafkaConsumer


app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)
thread = None
consumer = KafkaConsumer('result1')


def background_thread():
    jg = 0
    gm = 0
    sc = 0
    for msg in consumer:
        data_json = msg.value.decode('utf8')
        data_list = json.loads(data_json)
        for data in data_list:
            if '1' in data.keys():
                jg = data['1']
            elif '2' in data.keys():
                gm = data['2']
            elif '3' in data.keys():
                sc = data['3']
            else:
                continue
        result = str(jg) + ',' + str(gm)+',' + str(sc)
        print(result)
        socketio.emit('test_message',{'data':result})


@socketio.on('test_connect')
def connect(message):
    print(message)
    global thread
    if thread is None:
        thread = socketio.start_background_task(target=background_thread)
    socketio.emit('connected', {'data': 'Connected'})


@app.route("/")
def handle_mes():
    return render_template("index1.html")


if __name__ == '__main__':
    socketio.run(app,host='0.0.0.0',debug=True)