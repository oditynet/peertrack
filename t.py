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
REPLICA_TARGET = 3
ping_time = 10


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

    print("SAVE DATA!!!!")
    print(dict(replication_queue))

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
            if len(chunks[chunk_hash]) < REPLICA_TARGET:
                replication_queue[chunk_hash].extend(chunks[chunk_hash])
            if not chunks[chunk_hash]:
                del chunks[chunk_hash]
    check_replication_queue()


def check_replication_queue():
    for chunk_hash in list(replication_queue.keys()):
        needed = REPLICA_TARGET - len(chunks.get(chunk_hash, []))
        if needed <= 0:
            del replication_queue[chunk_hash]
            continue

        candidates = [
                         p_id for p_id in peers
                         if p_id not in chunks[chunk_hash] and p_id not in replication_queue[chunk_hash]
                     ][:needed]

        if candidates:
            replication_queue[chunk_hash].extend(candidates)
            for peer_id in candidates:
                trigger_replication(chunk_hash, peer_id)


def trigger_replication(chunk_hash, target_peer_id):
    source_peers = chunks.get(chunk_hash, [])
    if not source_peers:
        return

    source_peer = peers[source_peers[0]]
    target_peer = peers[target_peer_id]

    try:
        requests.post(
            f"http://{source_peer['address']}:{source_peer['port']}/replicate_chunk",
            json={
                'chunk_hash': chunk_hash,
                'target_peer': f"{target_peer['address']}:{target_peer['port']}"
            },
            timeout=10
        )
    except Exception as e:
        print(f"Replication trigger failed: {str(e)}")


health_check_thread = threading.Thread(target=peer_health_check, daemon=True)
health_check_thread.start()


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

    check_replication_queue()
    save_data()
    return jsonify({'peer_id': peer_id, 'status': 'registered'})

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
"""@app.route('/announce_file', methods=['POST'])
def announce_file():
    data = request.json
    file_hash = data['file_hash']

    # Сохраняем метаданные файла
    files[file_hash] = {
        'name': data['file_name'],
        'size': data['file_size'],
        'chunks': data['chunks']
    }

    # Для каждого чанка выбираем пиры и отправляем данные
    for chunk_hash in data['chunks']:
        # Выбираем 3 случайных пира
        selected_peers = select_peers_for_chunk(chunk_hash)
        print(f"random 3 peer {selected_peers}")

        # Отправляем чанк на каждый выбранный пир
        for peer_id in selected_peers:
            success = send_chunk_to_peer(chunk_hash, peer_id)
            if success:
                chunks.setdefault(chunk_hash, []).append(peer_id)

    save_data()
    return jsonify({'status': 'ok'})"""

def select_peers_for_chunk(chunk_hash):
    # Выбираем 3 случайных активных пира
    available_peers = list(peers.keys())
    return random.sample(available_peers, min(3, len(available_peers)))

def send_chunk_to_peer(chunk_hash, peer_id):
    try:
        peer = peers[peer_id]
        response = requests.post(
            f"http://{peer['address']}:{peer['port']}/store_chunk",
            json={'chunk_hash': chunk_hash},
            timeout=5
        )
        return response.status_code == 200
    except:
        return False

@app.route('/announce_chunk', methods=['POST'])
def announce_chunk():
    data = request.json
    chunk_hash = data['chunk_hash']
    peer_id = data['peer_id']

    if peer_id not in peers:
        return jsonify({'error': 'Peer not registered'}), 400

    if chunk_hash not in chunks:
        chunks[chunk_hash] = []

    if peer_id not in chunks[chunk_hash]:
        chunks[chunk_hash].append(peer_id)
        if chunk_hash in replication_queue:
            print(f" !!! anonce {chunk_hash} {peer_id}" )
            replication_queue[chunk_hash] = [p for p in replication_queue[chunk_hash] if p != peer_id]
            if not replication_queue[chunk_hash]:
                del replication_queue[chunk_hash]

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


@app.route('/get_replication_targets/<chunk_hash>')
def get_replication_targets(chunk_hash):
    current = chunks.get(chunk_hash, [])
    needed = REPLICA_TARGET - len(current)
    candidates = [p_id for p_id in peers if p_id not in current]
    print(f"NEED !!! {candidates}")
    return jsonify({'targets': candidates[:needed]})


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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
