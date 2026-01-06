"""
OpenSearch RAG (Retrieval Augmented Generation) 서비스
Text-to-SQL을 위한 스키마 검색 및 컨텍스트 생성
"""
from typing import List, Dict, Any, Tuple
from utils.opensearch_client import get_opensearch_client, DOMAIN_INDEX


class OpenSearchRAGService:
    """
    OpenSearch에서 관련 스키마를 검색하고 LLM 컨텍스트를 생성하는 서비스
    """

    def __init__(self):
        self.client = get_opensearch_client()
        self.index = DOMAIN_INDEX

    def search_schema(self, query: str, limit: int = 5) -> Tuple[List[Dict[str, Any]], str]:
        """
        자연어 질문에서 관련 Dataset 검색 후 스키마 컨텍스트 생성

        Args:
            query: 사용자 질문
            limit: 반환할 최대 결과 수

        Returns:
            Tuple of (검색 결과 리스트, LLM용 컨텍스트 문자열)
        """
        try:
            response = self.client.search(
                index=self.index,
                body={
                    "query": {
                        "multi_match": {
                            "query": query,
                            "fields": [
                                "name^3",           # 이름 가중치 높음
                                "description^2",    # 설명 가중치 중간
                                "column_names",     # 컬럼명
                                "column_descriptions",
                                "node_descriptions"
                            ],
                            "type": "best_fields",
                            "fuzziness": "AUTO"
                        }
                    },
                    "size": limit,
                    "_source": [
                        "doc_id", "doc_type", "name", "description",
                        "column_names", "column_descriptions", "status", "s3_path"
                    ]
                }
            )

            results = []
            context_parts = []

            for hit in response['hits']['hits']:
                source = hit['_source']
                score = hit['_score']

                # 결과 객체 생성
                s3_path = source.get('s3_path')
                result = {
                    "name": source.get('name', ''),
                    "doc_type": source.get('doc_type', 'unknown'),
                    "description": source.get('description', ''),
                    "columns": source.get('column_names', []),
                    "s3_path": s3_path,
                    "score": score
                }
                results.append(result)

                # LLM 컨텍스트 생성
                columns = source.get('column_names', [])
                column_str = ', '.join(columns) if columns else 'No columns defined'

                # S3 path 정보 추가 (Datasets have S3 paths)
                path_info = f"\nS3 Path: {s3_path}" if s3_path else ""

                context_parts.append(
                    f"Table: {source.get('name', 'Unknown')}\n"
                    f"Type: {source.get('doc_type', 'unknown')}\n"
                    f"Description: {source.get('description', 'No description')}\n"
                    f"Columns: {column_str}{path_info}"
                )

            context = "\n\n".join(context_parts) if context_parts else "No matching schemas found."

            return results, context

        except Exception as e:
            print(f"OpenSearch RAG search error: {e}")
            return [], f"Error searching schema: {str(e)}"

    def get_all_schemas(self, limit: int = 20) -> Tuple[List[Dict[str, Any]], str]:
        """
        모든 스키마 정보 조회 (컨텍스트 생성용)

        Args:
            limit: 최대 반환 개수

        Returns:
            Tuple of (결과 리스트, 컨텍스트 문자열)
        """
        try:
            response = self.client.search(
                index=self.index,
                body={
                    "query": {"match_all": {}},
                    "size": limit,
                    "_source": [
                        "doc_id", "doc_type", "name", "description",
                        "column_names", "column_descriptions"
                    ],
                    "sort": [{"_score": "desc"}]
                }
            )

            results = []
            context_parts = []

            for hit in response['hits']['hits']:
                source = hit['_source']

                result = {
                    "name": source.get('name', ''),
                    "doc_type": source.get('doc_type', 'unknown'),
                    "description": source.get('description', ''),
                    "columns": source.get('column_names', []),
                    "score": 1.0
                }
                results.append(result)

                columns = source.get('column_names', [])
                column_str = ', '.join(columns) if columns else 'No columns'

                context_parts.append(
                    f"Table: {source.get('name', 'Unknown')}\n"
                    f"Columns: {column_str}"
                )

            context = "\n\n".join(context_parts) if context_parts else "No schemas available."

            return results, context

        except Exception as e:
            print(f"OpenSearch get all schemas error: {e}")
            return [], f"Error retrieving schemas: {str(e)}"


# 싱글톤 인스턴스
_rag_service = None


def get_rag_service() -> OpenSearchRAGService:
    """RAG 서비스 싱글톤 반환"""
    global _rag_service
    if _rag_service is None:
        _rag_service = OpenSearchRAGService()
    return _rag_service
