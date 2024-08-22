from azure.cosmos import CosmosClient, exceptions

# Cosmos DBの設定
# Azure Cosmos DBのエンドポイント、キーを設定
endpoint = "https://cosmos-v315-msp-prd-jpeast-001.documents.azure.com:443/"
key = ""

# データベース名とコンテナ名を設定
database_name = 'openai'
container_name = 'user_client'

# Cosmos DBクライアントの初期化
client = CosmosClient(endpoint, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

# クエリを実行してユーザーを取得
query = "SELECT c.display_name FROM c WHERE CONTAINS(c.display_name, 'CSVテストユーザー')"
user_list = list(container.query_items(query, enable_cross_partition_query=True))

# ユーザー番号を抽出
user_numbers = [int(user['display_name'].replace('CSVテストユーザー', '')) for user in user_list]

# 欠けている番号を特定
all_numbers = set(range(1, 101))
existing_numbers = set(user_numbers)
missing_numbers = all_numbers - existing_numbers

print(f"欠けているユーザー番号: {missing_numbers}")