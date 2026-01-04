"""
AWS Bedrock 서비스
Claude 모델을 사용한 Text-to-SQL 생성
"""
import os
import json
import boto3
from typing import Optional


class BedrockService:
    """
    AWS Bedrock Claude 모델을 사용한 SQL 생성 서비스
    """

    def __init__(self):
        self.region = os.getenv('AWS_REGION', 'ap-northeast-2')
        # Global inference profile for cross-region access (Claude Haiku 4.5)
        self.model_id = os.getenv(
            'BEDROCK_MODEL_ID',
            'global.anthropic.claude-haiku-4-5-20251001-v1:0'
        )
        self._client = None

    @property
    def client(self):
        """Bedrock 클라이언트 (lazy initialization)"""
        if self._client is None:
            self._client = boto3.client(
                'bedrock-runtime',
                region_name=self.region
            )
        return self._client

    def generate_sql(self, schema_context: str, question: str, additional_context: Optional[str] = None) -> str:
        """
        스키마 컨텍스트와 질문으로 SQL 생성

        Args:
            schema_context: OpenSearch에서 검색된 스키마 정보
            question: 사용자의 자연어 질문
            additional_context: 추가 컨텍스트 (선택)

        Returns:
            생성된 SQL 쿼리
        """
        prompt = self._build_prompt(schema_context, question, additional_context)

        try:
            response = self.client.invoke_model(
                modelId=self.model_id,
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 1000,
                    "temperature": 0.1,  # 낮은 temperature로 일관된 SQL 생성
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                })
            )

            result = json.loads(response['body'].read())
            generated_text = result['content'][0]['text']

            # SQL 코드 블록에서 SQL 추출
            sql = self._extract_sql(generated_text)
            return sql

        except Exception as e:
            error_msg = str(e)
            print(f"Bedrock error: {error_msg}")

            # 권한 오류 체크
            if 'AccessDeniedException' in error_msg:
                return f"-- Error: Bedrock access denied. Please check IAM permissions.\n-- Model: {self.model_id}"
            elif 'ResourceNotFoundException' in error_msg:
                return f"-- Error: Model not found. Please enable Claude 3.5 Sonnet in Bedrock console.\n-- Model: {self.model_id}"
            else:
                return f"-- Error generating SQL: {error_msg}"

    def _build_prompt(self, schema_context: str, question: str, additional_context: Optional[str] = None) -> str:
        """SQL 생성을 위한 프롬프트 구성"""
        context_section = ""
        if additional_context:
            context_section = f"\n\nAdditional Context:\n{additional_context}"

        return f"""You are a SQL expert for DuckDB querying Parquet files on S3.
Based on the available data schema below, generate a DuckDB SQL query to answer the user's question.

Available Data Schema:
{schema_context}
{context_section}

User Question: {question}

IMPORTANT Rules:
1. If the user asks about a specific table/dataset that is NOT in the Available Data Schema above, respond with: -- Error: Table '[name]' not found. Available tables: [list table names]
2. Return ONLY the SQL query, no explanation before or after
3. Use the column names exactly as provided in the schema
4. For tables with S3 Path, query the Parquet file directly using: SELECT * FROM 's3://bucket/path/*.parquet'
5. Use single quotes around S3 paths (not double quotes)
6. If no S3 Path is provided, use the table name as-is
7. Use standard SQL syntax compatible with DuckDB
8. Add appropriate WHERE, ORDER BY, GROUP BY, LIMIT clauses as needed
9. For aggregate queries, always include relevant columns in GROUP BY

Example S3 query format:
SELECT column1, column2 FROM 's3://my-bucket/data/*.parquet' WHERE condition LIMIT 10;

SQL Query:"""

    def _extract_sql(self, text: str) -> str:
        """
        생성된 텍스트에서 SQL 추출
        코드 블록이 있으면 추출, 없으면 전체 텍스트 반환
        """
        # ```sql ... ``` 블록 추출
        if '```sql' in text.lower():
            start = text.lower().find('```sql') + 6
            end = text.find('```', start)
            if end > start:
                return text[start:end].strip()

        # ``` ... ``` 블록 추출
        if '```' in text:
            start = text.find('```') + 3
            end = text.find('```', start)
            if end > start:
                return text[start:end].strip()

        # 코드 블록 없으면 전체 텍스트 정리 후 반환
        return text.strip()


# 싱글톤 인스턴스
_bedrock_service = None


def get_bedrock_service() -> BedrockService:
    """Bedrock 서비스 싱글톤 반환"""
    global _bedrock_service
    if _bedrock_service is None:
        _bedrock_service = BedrockService()
    return _bedrock_service
