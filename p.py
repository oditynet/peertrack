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

peer_id = None
chunk_dir = 'chunks'
tracker_url = 'http://localhost:5000'

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


@app.route('/ping')
def ping():
    return jsonify({'status': 'ok'})

@app.route('/download_chunk/<chunk_hash>')
def download_chunk(chunk_hash):
    chunk_path = os.path.join(chunk_dir, chunk_hash)
   # print(f"[*!!!!!!!!!] Path: {chunk_path}")
    if not os.path.exists(chunk_path):
        return jsonify({'status': 'Chunk not found'}), 404
    return send_file(chunk_path)


@app.route('/replicate_chunk', methods=['POST'])
def replicate_chunk():
    data = request.json
    #print(data)
    chunk_hash = data['chunk_hash']
    source_peer = data['source_peer']
    #target_peer = data['target_peer']
    #print(f"!!!!!! source_peer500   {source_peer}")
    chunk_path=''
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

            return jsonify({'status': 'replicated'})
    except Exception as e:
        if os.path.exists(chunk_path):
           os.remove(chunk_path)
        return jsonify({'error': str(e)}), 500

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

        #local_chunks.add(chunk_hash)
        #save_chunks(local_chunks)

        ret = requests.post(
            f"{tracker_url}/announce_chunk",
            json={'chunk_hash': chunk_hash, 'peer_id': peer_id},
            timeout=3
        )

        return jsonify({'status': 'ok', 'chunk_hash': chunk_hash})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--data-dir', default='chunks')
    args = parser.parse_args()

    chunk_dir = os.path.abspath(args.data_dir)
    os.makedirs(chunk_dir, exist_ok=True)

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
    else:
        print("Registration failed")
        exit(1)

    #replicator_thread = threading.Thread(target=background_replicator, daemon=True)
    #replicator_thread.start()

    app.run(host='0.0.0.0', port=args.port)
