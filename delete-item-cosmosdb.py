from azure.cosmos import CosmosClient, PartitionKey

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
timestamp = 1722241820271

# 指定したタイムスタンプ以降に作成されたアイテムを検索
for item in container_client.query_items(
        query='SELECT * FROM user_client uc WHERE uc.create_timestamp > @timestamp',
        parameters=[
            {"name": "@timestamp", "value": timestamp}
        ],
        enable_cross_partition_query=True):
    
    # アイテムを削除
    container_client.delete_item(item, partition_key=item['id'])

# 完了ログを出力
print("DONE")