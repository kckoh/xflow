"""
임베딩 서비스 - AWS Bedrock Titan Embeddings 사용
질문을 1024차원 벡터로 변환하여 의미 기반 검색 지원
"""
import os
import json
import boto3
import numpy as np
from typing import Optional


class EmbeddingService:
    """
    AWS Bedrock Titan Embeddings를 사용한 텍스트 임베딩 서비스
    """

    def __init__(self):
        self.region = os.getenv('AWS_REGION', 'ap-northeast-2')
        # Titan Embeddings V2 모델 (1024 dimensions)
        self.model_id = os.getenv(
            'BEDROCK_EMBEDDING_MODEL_ID',
            'amazon.titan-embed-text-v2:0'
        )
        self._client = None

    @property
    def client(self):
        """Bedrock 클라이언트 (lazy initialization)"""
        if self._client is None:
            self._client = boto3.client(
                'bedrock-runtime',
                region_name=self.region,
                endpoint_url=None  # Force real AWS Bedrock
            )
        return self._client

    def get_embedding(self, text: str, normalize: bool = True) -> np.ndarray:
        """
        텍스트를 벡터(임베딩)로 변환

        Args:
            text: 임베딩할 텍스트 (질문)
            normalize: L2 정규화 여부 (코사인 유사도에 최적화)

        Returns:
            1024차원 float32 numpy 배열
        """
        try:
            # Titan Embeddings V2 요청
            body = json.dumps({
                "inputText": text,
                "dimensions": 1024,
                "normalize": normalize
            })

            response = self.client.invoke_model(
                modelId=self.model_id,
                body=body
            )

            result = json.loads(response['body'].read())
            embedding = result['embedding']

            # numpy array로 변환 (float32 - Redis Vector에 최적)
            return np.array(embedding, dtype=np.float32)

        except Exception as e:
            print(f"Embedding error: {e}")
            raise RuntimeError(f"Failed to generate embedding: {str(e)}")


# 싱글톤 인스턴스
_embedding_service = None


def get_embedding_service() -> EmbeddingService:
    """임베딩 서비스 싱글톤 반환"""
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = EmbeddingService()
    return _embedding_service
