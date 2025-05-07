from flask import Flask, request, jsonify, send_file
import os
import requests
import argparse
import socket
import hashlib
import threading
import time
import json
import traceback

app = Flask(__name__)
REPLICA_TARGET = 3

peer_id = None
chunk_dir = 'chunks'
tracker_url = 'http://localhost:5000'
peer_data_file = 'peer_data.json'


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip


def load_chunks():
    try:
        if os.path.exists(peer_data_file):
            with open(peer_data_file, 'r') as f:
                data = json.load(f)
                return set(data.get('chunks', []))  # Всегда возвращаем set
        return set()  # Возвращаем пустой set если файла нет
    except Exception as e:
        print(f"Error loading chunks: {str(e)}")
        return set()  # Гарантируем возврат set при ошибках


# Инициализируем local_chunks ДО регистрации пира
local_chunks = load_chunks()


def save_chunks(chunks_set):
    with open(peer_data_file, 'w') as f:
        json.dump({'chunks': list(chunks_set)}, f)


def background_replicator():
    while True:
        time.sleep(15)
        try:
            print(f"!!!! {local_chunks}")
            for chunk_hash in list(local_chunks):
                response = requests.get(
                    f"{tracker_url}/get_replication_targets/{chunk_hash}",
                    timeout=5
                )
                if response.status_code == 200:
                    targets = response.json().get('targets', [])
                    print(f"!!!! {targets}")
                    for target_id in targets:
                        target = requests.get(
                            f"{tracker_url}/get_peer/{target_id}",
                            timeout=3
                        ).json()
                        target_addr = f"{target['address']}:{target['port']}"
                        try:
                            with open(os.path.join(chunk_dir, chunk_hash), 'rb') as f:
                                requests.post(
                                    f"http://{target_addr}/upload_chunk",
                                    files={'chunk': (chunk_hash, f)},
                                    timeout=10
                                )
                        except Exception as e:
                            print(f"Background replication failed: {str(e)}")
        except Exception as e:
            print(f"Replication error: {str(e)}")


@app.route('/ping')
def ping():
    return jsonify({'status': 'ok'})


@app.route('/upload_chunk', methods=['POST'])
def upload_chunk():
    try:
        if 'chunk' not in request.files:
            return jsonify({'error': 'No chunk provided'}), 400

        chunk_file = request.files['chunk']
        chunk_data = chunk_file.read()
        chunk_hash = hashlib.sha256(chunk_data).hexdigest()

        chunk_path = os.path.join(chunk_dir, chunk_hash)
        os.makedirs(os.path.dirname(chunk_path), exist_ok=True)

        with open(chunk_path, 'wb') as f:
            f.write(chunk_data)

        local_chunks.add(chunk_hash)
        save_chunks(local_chunks)

        requests.post(
            f"{tracker_url}/announce_chunk",
            json={'chunk_hash': chunk_hash, 'peer_id': peer_id},
            timeout=3
        )

        return jsonify({'status': 'ok', 'chunk_hash': chunk_hash})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/replicate_chunk', methods=['POST'])
def replicate_chunk():
    data = request.json
    chunk_hash = data['chunk_hash']
    source_peer = data['target_peer']
    print(f"!!!!!! source_peer500   {source_peer}")

    try:
        response = requests.get(
            f"http://{source_peer}/download_chunk/{chunk_hash}",
            timeout=10
        )
        if response.status_code == 200:
            chunk_data = response.content
            chunk_path = os.path.join(chunk_dir, chunk_hash)
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)

            local_chunks.add(chunk_hash)
            save_chunks(local_chunks)
            requests.post(
                f"{tracker_url}/announce_chunk",
                json={'chunk_hash': chunk_hash, 'peer_id': peer_id},
                timeout=3
            )
            return jsonify({'status': 'replicated'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/download_chunk/<chunk_hash>')
def download_chunk(chunk_hash):
    chunk_path = os.path.join(chunk_dir, chunk_hash)
    if not os.path.exists(chunk_path):
        return jsonify({'error': 'Chunk not found'}), 404
    return send_file(chunk_path)

@app.route('/store_chunk', methods=['POST'])
def store_chunk():
    chunk_hash = request.json['chunk_hash']

    # Сохраняем чанк в файловую систему
    chunk_path = os.path.join(chunk_dir, chunk_hash)
    with open(chunk_path, 'wb') as f:
        f.write(b"")  # Здесь должен быть реальный контент чанка

    # Анонсируем чанк трекеру
    requests.post(
        f"{tracker_url}/announce_chunk",
        json={'chunk_hash': chunk_hash, 'peer_id': peer_id}
    )

    return jsonify({'status': 'stored'})

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--data-dir', default='chunks')
    args = parser.parse_args()

    chunk_dir = os.path.abspath(args.data_dir)
    os.makedirs(chunk_dir, exist_ok=True)

    # Загружаем существующие чанки из директории
    existing_chunks = set(
        f for f in os.listdir(chunk_dir)
        if os.path.isfile(os.path.join(chunk_dir, f)))

    # Обновляем local_chunks
    local_chunks.update(existing_chunks)  # Теперь local_chunks - гарантированно set
    save_chunks(local_chunks)

    # Регистрация пира
    local_ip = get_local_ip()
    response = requests.post(
        f"{tracker_url}/register_peer",
        json={
            'ip': local_ip,
            'port': args.port,
            'data_dir': chunk_dir
        }
    )

    if response.status_code == 200:
        peer_id = response.json()['peer_id']
        print(f"Registered as peer {peer_id}")

        # Анонсируем все чанки
        for chunk_hash in local_chunks:
            response = requests.post(
                f"{tracker_url}/announce_chunk",
                json={'chunk_hash': chunk_hash, 'peer_id': peer_id},
                timeout=3
            )
            if response.status_code == 200:
                print(f"[+]{chunk_hash} announce true!")
            else:
                print(f"[-]{chunk_hash} false announce!")
    else:
        print("Registration failed")
        exit(1)

    replicator_thread = threading.Thread(target=background_replicator, daemon=True)
    replicator_thread.start()

    app.run(host='0.0.0.0', port=args.port)
