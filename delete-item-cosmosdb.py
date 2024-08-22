import threading
from azure.cosmos import CosmosClient
from concurrent.futures import ThreadPoolExecutor

# Azure Cosmos DBのエンドポイント、キーを設定
url = "https://cosmos-v315-msp-prd-jpeast-001.documents.azure.com:443/"
key = ""

# Cosmos DB クライアントをインスタンス化
client = CosmosClient(url, credential=key)

# データベース名とコンテナ名を設定
database_name = 'openai'
container_name = 'user_client'

database_client = client.get_database_client(database_name)
container_client = database_client.get_container_client(container_name)

# タイムスタンプを設定
timestamp = 1724218107920

# 削除対象のアイテムを取得
items_to_delete = container_client.query_items(
    query='SELECT * FROM user_client uc WHERE uc.create_timestamp >= @timestamp',
    parameters=[
        {"name": "@timestamp", "value": timestamp}
    ],
    enable_cross_partition_query=True
)

# ログ出力用のカウンタと削除関数を定義
count = 0
log_interval = 1000
lock = threading.Lock()

def delete_item(item):
    global count
    container_client.delete_item(item, partition_key=item['id'])
    with lock:
        count += 1
        if count % log_interval == 0:
            print(f"{count} items deleted")

# スレッドプールを使って並列に削除を実行
with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(delete_item, items_to_delete)

# 完了ログを出力
print("DONE")