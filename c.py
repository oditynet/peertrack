import requests
import os
import hashlib
import argparse
import random
import time

file_size = 1024 * 4 * 1024  # 4MB chunks

class TorrentClient:
    def __init__(self, tracker_url):
        self.tracker_url = tracker_url
        self.chunk_size = file_size
        self.max_retries = 3  # Максимальное количество попыток перезаливки
        self.retry_delay = 5  # Задержка между попытками в секундах

    def upload(self, file_path, retry_count=0):
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        chunks = []
        last_successful_peer = None

        with open(file_path, 'rb') as f:
            file_hash = hashlib.sha256()
            while True:
                data = f.read(self.chunk_size)
                if not data:
                    break
                chunk_hash = hashlib.sha256(data).hexdigest()
                chunks.append(chunk_hash)
                file_hash.update(data)

                peers_response = requests.get(f"{self.tracker_url}/list_peers")
                if peers_response.status_code == 200:
                    peers = peers_response.json().get('peers', [])
                    if peers:
                        random.shuffle(peers)
                        for peer in peers:
                            peer_addr = f"{peer['address']}"
                            try:
                                response = requests.post(
                                    f"http://{peer_addr}/upload_chunk",
                                    files={'chunk': (chunk_hash, data)},
                                    timeout=5
                                )
                                if response.status_code == 200:
                                    last_successful_peer = peer_addr
                                    break
                                else:
                                    print(f"Ошибка загрузки чанка {chunk_hash} на {peer_addr}")
                            except Exception as e:
                                print(f"Ошибка подключения к пиру {peer_addr}: {str(e)}")

        # Отправка метаданных файла на трекер с возможностью повтора
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    f"{self.tracker_url}/announce_file",
                    json={
                        'file_hash': file_hash.hexdigest(),
                        'file_name': file_name,
                        'file_size': file_size,
                        'chunks': chunks
                    },
                    timeout=10
                )
                
                if response.status_code == 200:
                    if last_successful_peer:
                        print(f"Файл успешно загружен. Статус трекера: {response.status_code}. Последний успешный пир: {last_successful_peer}")
                    else:
                        print(f"Файл успешно загружен. Статус трекера: {response.status_code}")
                    return True
                elif response.status_code == 500 and attempt < self.max_retries - 1:
                    print(f"Ошибка трекера (500). Попытка {attempt + 1} из {self.max_retries}. Повтор через {self.retry_delay} сек...")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    print(f"Ошибка загрузки файла. Код статуса: {response.status_code}")
                    return False
            except Exception as e:
                print(f"Ошибка соединения с трекером: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                return False
    def download(self, file_hash):
        response = requests.get(f"{self.tracker_url}/get_file/{file_hash}")
        if response.status_code != 200:
            print("File not found")
            return

        file_info = response.json()
        os.makedirs('downloads', exist_ok=True)
        download_name = os.path.basename(file_info['name'])

        with open('downloads/'+download_name, 'wb') as f:
            for chunk_hash in file_info['chunks']:
                chunk_response = requests.get(f"{self.tracker_url}/get_chunk_peers/{chunk_hash}")
                peers = chunk_response.json().get('peers', [])
                for peer_id in peers:
                    peer_response = requests.get(f"{self.tracker_url}/get_peer/{peer_id}")
                    if peer_response.status_code == 200:
                        peer = peer_response.json()
                        try:
                            chunk_data = requests.get(
                                f"http://{peer['address']}:{peer['port']}/download_chunk/{chunk_hash}"
                            ).content
                            f.write(chunk_data)
                            break
                        except:
                            continue

    def list_files(self):
        response = requests.get(f"{self.tracker_url}/list_files")
        if response.status_code == 200:
            for file in response.json()['files']:
                print(f"{file['name']} ({file['hash']})")
    def list_peers(self):
        response = requests.get(f"{self.tracker_url}/list_peers")
        if response.status_code == 200:
            for file in response.json()['peers']:
                print(f"{file['id']} {file['address']}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tracker', default='http://localhost:5000')
    parser.add_argument('--upload', help='File to upload')
    parser.add_argument('--download', help='File hash to download')
    parser.add_argument('--list-files', action='store_true', help='List all files')
    parser.add_argument('--list-peers', action='store_true', help='List all peers')
    args = parser.parse_args()

    client = TorrentClient(args.tracker)

    if args.upload:
        client.upload(args.upload)
    elif args.download:
        client.download(args.download)
        #print(f"File saved")
    elif args.list_files:
        client.list_files()
    elif args.list_peers:
        client.list_peers()
