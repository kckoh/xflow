"""
Redis Vector Search 클라이언트
의미 기반 캐싱을 위한 Vector Index 관리
"""
import os
import redis
from typing import Optional, Dict, Any
import numpy as np


class RedisVectorClient:
    """
    Redis Vector Search 전용 클라이언트

    주의: Vector 데이터는 바이너리이므로 decode_responses=False 필요
    """

    def __init__(self):
        self.client: Optional[redis.Redis] = None
        self.index_name = "semantic_cache_idx"
        self.key_prefix = "semantic:"
        self.vector_dim = 1024  # Titan Embeddings V2 dimension

    def connect(self) -> redis.Redis:
        """Redis 연결 (동기, Vector Search용)"""
        if self.client is None:
            self.client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis-master"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                password=os.getenv("REDIS_PASSWORD", "redis123"),
                decode_responses=False,  # Vector 데이터는 바이너리
            )
        return self.client

    def create_index(self):
        """
        Vector Search 인덱스 생성

        Schema:
        - question (TEXT): 원본 질문 (검색 가능)
        - embedding (VECTOR): 1024차원 벡터 (HNSW 알고리즘)
        - sql_query (TEXT): 저장된 SQL
        - timestamp (NUMERIC): 생성 시간
        """
        try:
            client = self.connect()
            # 기존 인덱스 존재 여부 확인
            client.ft(self.index_name).info()
            print(f"✓ Vector index '{self.index_name}' already exists")
        except redis.exceptions.ResponseError:
            # 인덱스 없음 - 새로 생성
            from redis.commands.search.field import TextField, VectorField, NumericField
            from redis.commands.search import IndexDefinition, IndexType

            schema = (
                TextField("question"),
                VectorField(
                    "embedding",
                    "HNSW",  # Hierarchical Navigable Small World (고속 근사 검색)
                    {
                        "TYPE": "FLOAT32",
                        "DIM": self.vector_dim,
                        "DISTANCE_METRIC": "COSINE",  # 코사인 유사도
                        "INITIAL_CAP": 10000,  # 초기 용량
                    }
                ),
                TextField("sql_query"),
                NumericField("timestamp"),
            )

            definition = IndexDefinition(
                prefix=[self.key_prefix],
                index_type=IndexType.HASH
            )

            client.ft(self.index_name).create_index(
                fields=schema,
                definition=definition
            )
            print(f"✓ Vector index '{self.index_name}' created successfully")

    def search_similar(
        self,
        query_vector: np.ndarray,
        top_k: int = 1,
        threshold: float = 0.95
    ) -> Optional[Dict[str, Any]]:
        """
        벡터 유사도 검색 (KNN)

        Args:
            query_vector: 질문 벡터 (1024 dim)
            top_k: 상위 K개 결과
            threshold: 최소 유사도 (0.95 = 95%)

        Returns:
            {question, sql_query, similarity} 또는 None
        """
        try:
            client = self.connect()
            from redis.commands.search.query import Query

            # KNN 쿼리 (COSINE distance)
            q = (
                Query(f"(*)=>[KNN {top_k} @embedding $vector AS score]")
                .return_fields("question", "sql_query", "score")
                .dialect(2)
            )

            params = {"vector": query_vector.tobytes()}
            results = client.ft(self.index_name).search(q, params)

            if results.total > 0:
                doc = results.docs[0]
                # Cosine distance → similarity (1 - distance)
                similarity = 1 - float(doc.score)

                if similarity >= threshold:
                    # Handle both bytes and str (depending on decode_responses setting)
                    question = doc.question.decode('utf-8') if isinstance(doc.question, bytes) else doc.question
                    sql_query = doc.sql_query.decode('utf-8') if isinstance(doc.sql_query, bytes) else doc.sql_query

                    return {
                        "question": question,
                        "sql_query": sql_query,
                        "similarity": similarity
                    }

            return None

        except Exception as e:
            print(f"Vector search error: {e}")
            return None

    def save_cache(
        self,
        question: str,
        embedding: np.ndarray,
        sql_query: str
    ):
        """
        벡터 캐시 저장 (LRU가 자동으로 메모리 관리)

        Args:
            question: 원본 질문
            embedding: 질문 벡터
            sql_query: 생성된 SQL
        """
        import hashlib
        import time

        client = self.connect()

        # 질문 해시를 키로 사용
        key_hash = hashlib.md5(question.encode()).hexdigest()
        key = f"{self.key_prefix}{key_hash}"

        # HASH로 저장
        mapping = {
            "question": question.encode('utf-8'),
            "embedding": embedding.tobytes(),
            "sql_query": sql_query.encode('utf-8'),
            "timestamp": str(int(time.time())).encode('utf-8')
        }

        client.hset(key, mapping=mapping)
        # TTL 설정 없음 - LRU가 알아서 관리

        print(f"✓ Cached: {question[:50]}... (key: {key_hash[:8]})")

    def get_memory_info(self) -> Dict[str, Any]:
        """Redis 메모리 사용량 조회"""
        client = self.connect()
        info = client.info('memory')
        return {
            "used_memory": info.get('used_memory_human'),
            "used_memory_rss": info.get('used_memory_rss_human'),
            "maxmemory": info.get('maxmemory_human'),
            "total_keys": client.dbsize()
        }


# 싱글톤 인스턴스
_redis_vector_client = None


def get_redis_vector_client() -> RedisVectorClient:
    """Redis Vector 클라이언트 싱글톤 반환"""
    global _redis_vector_client
    if _redis_vector_client is None:
        _redis_vector_client = RedisVectorClient()
        # 애플리케이션 시작 시 인덱스 자동 생성
        try:
            _redis_vector_client.create_index()
        except Exception as e:
            print(f"⚠️  Vector index creation failed (will retry on first use): {e}")
    return _redis_vector_client
