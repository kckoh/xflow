"""
CDC Service - Kafka Connect 커넥터 관리
역할: Debezium 커넥터 등록/삭제/상태 확인
"""
import os
import httpx
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect:8083")


class CDCService:
    """Debezium CDC 커넥터 관리 서비스"""
    
    @staticmethod
    async def create_connector(name: str, config: Dict) -> Dict:
        """커넥터 등록 (이미 있으면 업데이트)"""
        async with httpx.AsyncClient() as client:
            # 기존 커넥터 확인
            check = await client.get(f"{KAFKA_CONNECT_URL}/connectors/{name}")
            
            if check.status_code == 200:
                # 업데이트
                response = await client.put(
                    f"{KAFKA_CONNECT_URL}/connectors/{name}/config",
                    json=config
                )
            else:
                # 새로 생성
                response = await client.post(
                    f"{KAFKA_CONNECT_URL}/connectors",
                    json={"name": name, "config": config}
                )
            
            response.raise_for_status()
            logger.info(f"커넥터 등록 완료: {name}")
            return response.json()
    
    @staticmethod
    async def delete_connector(name: str) -> bool:
        """커넥터 삭제"""
        async with httpx.AsyncClient() as client:
            response = await client.delete(f"{KAFKA_CONNECT_URL}/connectors/{name}")
            logger.info(f"커넥터 삭제: {name}")
            return response.status_code in [200, 204, 404]
    
    @staticmethod
    async def get_status(name: str) -> Dict:
        """커넥터 상태 조회"""
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{KAFKA_CONNECT_URL}/connectors/{name}/status")
            if response.status_code == 404:
                return {"error": "not_found"}
            return response.json()
    
    @staticmethod
    def build_postgres_config(
        connector_name: str,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        table_list: List[str],
        schema: str = "public"
    ) -> Dict:
        """
        PostgreSQL 커넥터 설정 생성
        table_list: ["schema.table1", "schema.table2", ...]
        """
        return {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "plugin.name": "pgoutput",
            "database.hostname": host,
            "database.port": str(port),
            "database.user": user,
            "database.password": password,
            "database.dbname": database,
            "database.server.name": connector_name,
            "topic.prefix": connector_name,
            "table.include.list": ",".join(table_list),
            "slot.name": connector_name.replace("-", "_"),
            "snapshot.mode": "never",  # Postgres: 스냅샷 없이 WAL부터 읽기 (변경사항만)
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false"
        }
