import json
from flask import Flask, render_template
from flask_socketio import SocketIO
from kafka import KafkaConsumer


app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)
thread = None
consumer = KafkaConsumer('result2')


def background_thread():
    r18 = 0
    r24 = 0
    r29 = 0
    r34 = 0
    r39 = 0
    r49 = 0
    r50 = 0
    for msg in consumer:
        data_json = msg.value.decode('utf8')
        data_list = json.loads(data_json)
        for data in data_list:
            if '1' in data.keys():
                r18 = data['1']
            elif '2' in data.keys():
                r24 = data['2']
            elif '3' in data.keys():
                r29 = data['3']
            elif '3' in data.keys():
                r34 = data['4']
            elif '3' in data.keys():
                r39 = data['5']
            elif '3' in data.keys():
                r49 = data['6']
            elif '3' in data.keys():
                r50 = data['7']
            else:
                continue
        result = str(r18) + ',' + str(r24)+',' + str(r29)+ ',' + str(r34)+',' + str(r49)+ ',' + str(r49)+',' + str(r50)
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
    return render_template("index2.html")


if __name__ == '__main__':
    socketio.run(app,host='0.0.0.0',debug=True)