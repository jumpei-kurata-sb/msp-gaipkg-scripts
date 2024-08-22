import asyncio
import hashlib
import uuid
from datetime import datetime
from typing import Any, List

import urllib3
from azure.cosmos.aio import CosmosClient
from azure.cosmos.aio._container import ContainerProxy
from azure.cosmos.aio._database import DatabaseProxy

urllib3.disable_warnings()

# DB接続情報を編集する
COSMOS_URL = "https://cosmos-v315-msp-prd-jpeast-001.documents.azure.com:443/" # Cosmos DBのURLを指定してください
MASTER_KEY = "" # マスターキーを指定してください
COSMOS_DATABASE = "openai" # データベース名を指定してください 例: openai 
USER_COUNT = 15000 # 作成するユーザー数を指定してください
DEFAULT_ROLE_ID = "default_role" # ここは変更不要

sem = asyncio.Semaphore(10) # 並列数を指定。特に理由がなければ変更不要


class CasbinRule:
    """
    CasbinRule model
    """

    def __init__(self, ptype=None, v0=None, v1=None, *args, **kwargs) -> None:
        self.ptype = ptype
        self.v0 = v0
        self.v1 = v1

    def dict(self) -> dict[str, Any]:
        d = {"ptype": self.ptype}

        for value in dir(self):
            if getattr(self, value) is not None and value.startswith("v") and value[1:].isnumeric():
                d[value] = getattr(self, value)

        return d

    def __str__(self) -> str:
        return ", ".join(self.dict().values())


def get_sha1(line: Any) -> str:
    m = hashlib.sha1()
    m.update(str(line).encode("utf-8"))
    return m.hexdigest()


def get_cosmos_client() -> CosmosClient:
    return CosmosClient(
        url=COSMOS_URL,
        credential={
            "masterKey": MASTER_KEY
        },
        connection_verify=False,
    )


def get_database(client) -> DatabaseProxy:
    return client.get_database_client(COSMOS_DATABASE)


def get_container(client, container) -> ContainerProxy:
    database = get_database(client)
    return database.get_container_client(container)


def generate_user_items() -> List[Any]:
    items = []
    for i in range(1, USER_COUNT + 1):
        items.append(
            {
                "id": "uid-" + str(uuid.uuid4()),
                "display_name": f"dummy-user-{i:06}",
                "preferred_username": f"dummy-user-{i:06}@jtp.co.jp",
                "job_title": None,
                "image": None,
                "create_timestamp": int(datetime.now().timestamp() * 1000),
                "update_timestamp": int(datetime.now().timestamp() * 1000),
                "delete_flg": False,
                "role_group": None,
                "department": None,
                "is_admin": False,
                "groups": [],
                "auth_provider": "email-password",
                "tenant_id": None,
            }
        )
    return items


async def create_user(container, user):
    async with sem:
        await container.create_item(user)


async def save_grouping_policy(container: ContainerProxy, user_id: str) -> None:
    rule = CasbinRule(ptype="g", v0=user_id, v1=DEFAULT_ROLE_ID)
    d = {"id": get_sha1(rule)}

    async with sem:
        await container.upsert_item(dict(**d, **rule.dict()))


async def create_users() -> None:
    user_items = generate_user_items()

    async with get_cosmos_client() as client, asyncio.TaskGroup() as tg:
        user_client_container = get_container(client, "user_client")
        policies_container = get_container(client, "policies")
        for user in user_items:
            tg.create_task(create_user(user_client_container, user))
            tg.create_task(save_grouping_policy(policies_container, user.get("id")))


async def save_grouping_policy(container: ContainerProxy, user_id: str) -> None:
    rule = CasbinRule(ptype="g", v0=user_id, v1=DEFAULT_ROLE_ID)
    d = {"id": get_sha1(rule)}

    async with sem:
        # print(f"> {user_id}")
        await container.upsert_item(dict(**d, **rule.dict()))


def main():
    print("Start creating users...")
    asyncio.run(create_users())
    print("End creating users")


if __name__ == "__main__":
    main()