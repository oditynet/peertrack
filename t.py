from flask import Flask, jsonify, request
import uuid
import json
import os
import threading
import time
import requests
import random
from collections import defaultdict

app = Flask(__name__)
DATA_FILE = 'tracker_data.json'
#REPLICA_TARGET = 3
ping_time = 10
repl_time = 60


def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            data = json.load(f)
            return data['peers'], data['files'], data['chunks'], data['replication_queue']
    return {}, {}, {}, defaultdict(list)


def save_data():
    data = {
        'peers': peers,
        'files': files,
        'chunks': chunks,
        'replication_queue': dict(replication_queue)
    }

    with open(DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)

peers, files, chunks, replication_queue = load_data()

def peer_health_check():
    while True:
        time.sleep(ping_time) # 10sec
        dead_peers = []
        for peer_id in list(peers.keys()):
            try:
                peer = peers[peer_id]
               # print(f"PING {peer} | {peer_id} | {peers}")
                response = requests.get(
                    f"http://{peer['address']}:{peer['port']}/ping",
                    timeout=3
                )
                if response.status_code != 200:
                    raise Exception("No response")
            except Exception as e:
                print(f"Peer {peer_id} is dead: {str(e)}")
                dead_peers.append(peer_id)

        for peer_id in dead_peers:
            handle_dead_peer(peer_id)

        if dead_peers:
            save_data()


def handle_dead_peer(peer_id):
    del peers[peer_id]
    for chunk_hash in list(chunks.keys()):
        if peer_id in chunks[chunk_hash]:
            chunks[chunk_hash].remove(peer_id)

def check_replication_queue():
  while True:
    time.sleep(repl_time)                                     #!!!! 60 sec copy repl
    for chunk_id in chunks.keys(): #to thread !!
        #print("Check file: ",chunk_id)
        count_peers=len(list(peers.keys()))
        copy_chunk = len(chunks[chunk_id])
        if count_peers > copy_chunk:
          res,have = check_missing_peers(chunk_id)
          #print("[???]",have)
          if res:
             for i in res:
               add_to_chunk(chunk_id, i)# + copy file
               #print("[add]", i, " | ",chunk_id)
               targ_peers = peers[i] #peers.get(i, [])
               src_peers= peers[list(have)[0]]  # select 0 from copy file
               #src_peers = chunks.get(have, ) # select 0 from copy file
               if targ_peers:
                  #print(src_peers,targ_peers,chunk_id)
                  ret = requests.post(f"http://{targ_peers['address']}:{targ_peers['port']}/replicate_chunk", json={'source_peer':f"{src_peers['address']}:{src_peers['port']}",'chunk_hash': chunk_id,'target_peer': f"{targ_peers['address']}:{targ_peers['port']}" },timeout=10 )
                  if ret == 200:
                      chunks[chunk_id].append(i)
                      add_to_chunk(chunk_id, i)
             save_data()


health_check_thread = threading.Thread(target=peer_health_check, daemon=True) #PING
health_check_thread.start()

repl_check_thread = threading.Thread(target=check_replication_queue, daemon=True) #copy src to all peers
repl_check_thread.start()

@app.route('/get_file/<file_hash>')
def get_file(file_hash):
    file_info = files.get(file_hash)
    if not file_info:
        return jsonify({'error': 'File not found'}), 404

    chunk_peers = {}
    for chunk in file_info['chunks']:
        chunk_peers[chunk] = chunks.get(chunk, [])

    return jsonify({
        'name': file_info['name'],
        'size': file_info['size'],
        'chunks': file_info['chunks'],
        'chunk_peers': chunk_peers
    })

@app.route('/get_peer/<peer_id>')
def get_peer(peer_id):
    peer = peers.get(peer_id)
    if not peer:
        return jsonify({'error': 'Peer not found'}), 404
    return jsonify(peer)

@app.route('/get_chunk_peers/<chunk_hash>')
def get_chunk_peers(chunk_hash):
    return jsonify({'peers': chunks.get(chunk_hash, [])})


def check_missing_peers(file_name):
    # Получаем все доступные пиры
    all_peers = set(peers.keys())

    # Получаем пиры из chunks для указанного файла
    file_chunks = chunks.get(file_name, [])
    chunks_peers = set()

    for chunk_group in file_chunks:
        if isinstance(chunk_group, str):
           chunk_group = [chunk_group]
        chunks_peers.update(chunk_group)
        #print(chunk_group)
    #print("[?miss ]", chunks_peers, set(peers.keys()))

    # Находим отсутствующих пиров
    missing_peers = all_peers - chunks_peers

    if missing_peers:
        print(f"Для файла '{file_name}' отсутствуют пиры: {', '.join(missing_peers)}")
        return missing_peers,chunks_peers
    else:
        print(f"Все пиры присутствуют для файла '{file_name}'")
        #return [],[] ????

def add_new_files(file_id,name, size):
    if file_id not in files:
        files[file_id] = {
            "name": name,
            "size": size
        }
        save_data()
    else:
        print(f"File with ID {file_id} already exists!")


@app.route('/register_peer', methods=['POST'])
def register_peer():
    data = request.json
    peer_id = str(uuid.uuid4())

    peers[peer_id] = {
        'address': data['ip'],
        'port': data['port'],
        'data_dir': data.get('data_dir', 'chunks'),
        'status': 'online'
    }

    #check_replication_queue()
    save_data()
    return jsonify({'peer_id': peer_id, 'status': 'registered'})

def add_to_chunk(chunk_id, number):
    """Добавляет число в указанный chunk"""
    if chunk_id in chunks:
        chunks[chunk_id].append(number)
    else:
        chunks[chunk_id] = [number]

@app.route('/announce_chunk', methods=['POST'])
def announce_chunk():
    data = request.json
    chunk_hash = data['chunk_hash']
    peer_id = data['peer_id']

    if peer_id not in peers:
        return jsonify({'status': 'Peer not registered'}), 400

    #add_to_chunk(chunk_hash, peer_id)
    if chunk_hash not in chunks:
        chunks[chunk_hash] = [peer_id]
    else:
        chunks[chunk_hash].append(peer_id)

    save_data()
    return jsonify({'status': 'ok'})


@app.route('/announce_file', methods=['POST'])
def announce_file():
    data = request.json
    file_hash = data['file_hash']

    files[file_hash] = {
        'name': data['file_name'],
        'size': data['file_size'],
        'chunks': data['chunks']
    }

    save_data()
    return jsonify({'status': 'ok'})

@app.route('/list_files')
def list_files():
    return jsonify({
        'files': [
            {
                'hash': fh,
                'name': fi['name'],
                'size': fi['size'],
                'chunks': fi['chunks']
            }
            for fh, fi in files.items()
        ]
    })

@app.route('/list_peers')
def list_peers():
    return jsonify({
        'peers': [
            {
                'id': pid,
                'address': f"{info['address']}:{info['port']}",
                'status': info['status']
            }
            for pid, info in peers.items()
        ]
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
