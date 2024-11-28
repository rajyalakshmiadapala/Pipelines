"""
title: ClickHouse Filter Pipeline
author: open-webui
date: 2024-09-27
version: 1.1
license: MIT
description: A filter pipeline that queries product information from ClickHouse.
requirements: clickhouse-connect
"""

from typing import List, Optional
import os
import uuid

from utils.pipelines.main import get_last_assistant_message
from pydantic import BaseModel
from clickhouse_connect import Client


class Pipeline:
    class Valves(BaseModel):
        pipelines: List[str] = []
        priority: int = 0
        clickhouse_host: str
        clickhouse_user: str
        clickhouse_password: str

    def __init__(self):
        self.type = "filter"
        self.name = "ClickHouse Filter"
        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "clickhouse_host": os.getenv("CLICKHOUSE_HOST", "localhost"),
                "clickhouse_user": os.getenv("CLICKHOUSE_USER", "default"),
                "clickhouse_password": os.getenv("CLICKHOUSE_PASSWORD", ""),
            }
        )
        self.client = None
        self.chat_generations = {}

    async def on_startup(self):
        print(f"on_startup:{__name__}")
        self.set_clickhouse_client()

    async def on_shutdown(self):
        print(f"on_shutdown:{__name__}")
        if self.client:
            self.client.close()

    async def on_valves_updated(self):
        self.set_clickhouse_client()

    def set_clickhouse_client(self):
        try:
            self.client = Client(
                host=self.valves.clickhouse_host,
                username=self.valves.clickhouse_user,
                password=self.valves.clickhouse_password,
            )
            print("Connected to ClickHouse.")
        except Exception as e:
            print(f"Error connecting to ClickHouse: {e}")

    async def inlet(self, body: dict, user: Optional[dict] = None) -> dict:
        print(f"inlet:{__name__}")
        print(f"Received body: {body}")
        print(f"User: {user}")

        # Check for presence of required keys and generate chat_id if missing
        if "chat_id" not in body:
            unique_id = f"SYSTEM MESSAGE {uuid.uuid4()}"
            body["chat_id"] = unique_id
            print(f"chat_id was missing, set to: {unique_id}")

        if "product_name" not in body:
            error_message = "Error: 'product_name' is required in the request body."
            print(error_message)
            raise ValueError(error_message)

        product_name = body["product_name"]
        product_info = self.get_product_info(product_name)
        if not product_info:
            print(f"No product found with name: {product_name}")
            return {"error": "Product not found"}

        body["product_info"] = product_info
        return body

    async def outlet(self, body: dict, user: Optional[dict] = None) -> dict:
        print(f"outlet:{__name__}")
        print(f"Received body: {body}")
        return body

    def get_product_info(self, product_name: str) -> Optional[dict]:
        try:
            query = """
            SELECT 
                id, name, price, discount, rating, description, image, product_link, created_at, updated_at
            FROM 
                products
            WHERE 
                name ILIKE %(name)s
            LIMIT 1
            """
            result = self.client.query(query, {"name": f"%{product_name}%"}).to_dict()
            if result:
                return result[0]
        except Exception as e:
            print(f"Error fetching product information: {e}")
        return None
