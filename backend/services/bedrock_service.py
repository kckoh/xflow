"""
AWS Bedrock 서비스
Claude 모델을 사용한 Text-to-SQL 생성
"""
import os
import json
import boto3
from typing import Optional
from services.embedding_service import get_embedding_service
from utils.redis_vector_client import get_redis_vector_client


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
        # Input token limit
        self.max_input_tokens = 20000

        # Semantic Cache 설정
        self.cache_enabled = os.getenv('SEMANTIC_CACHE_ENABLED', 'true').lower() == 'true'
        self.cache_threshold = float(os.getenv('SEMANTIC_CACHE_THRESHOLD', '0.95'))

        self._client = None
        self._embedding_service = None
        self._vector_client = None
    @property
    def client(self):
        """Bedrock 클라이언트 (lazy initialization)"""
        if self._client is None:
            import boto3
            import os
            
            # Temporarily remove LocalStack environment variables
            # so boto3 will use ~/.aws/credentials file instead
            old_access_key = os.environ.pop('AWS_ACCESS_KEY_ID', None)
            old_secret_key = os.environ.pop('AWS_SECRET_ACCESS_KEY', None)
            old_endpoint = os.environ.pop('AWS_ENDPOINT', None)
            
            try:
                # Now boto3 will use ~/.aws/credentials file
                self._client = boto3.client(
                    'bedrock-runtime',
                    region_name=self.region
                )
            finally:
                # Restore environment variables for S3 (LocalStack)
                if old_access_key:
                    os.environ['AWS_ACCESS_KEY_ID'] = old_access_key
                if old_secret_key:
                    os.environ['AWS_SECRET_ACCESS_KEY'] = old_secret_key
                if old_endpoint:
                    os.environ['AWS_ENDPOINT'] = old_endpoint
                    
        return self._client

    @property
    def embedding_service(self):
        """임베딩 서비스 (lazy)"""
        if self._embedding_service is None:
            self._embedding_service = get_embedding_service()
        return self._embedding_service

    @property
    def vector_client(self):
        """Vector 클라이언트 (lazy)"""
        if self._vector_client is None:
            self._vector_client = get_redis_vector_client()
        return self._vector_client

    def generate_sql(
        self, 
        question: str, 
        prompt_type: str = 'general',
        metadata: Optional[dict] = None,
        schema_context: Optional[str] = None,
        additional_context: Optional[str] = None
    ) -> str:
        """
        Generate SQL based on prompt type
        
        Args:
            question: User's natural language question
            prompt_type: Type of prompt (query_page, field_transform, sql_transform, partition)
            metadata: Context-specific data (column info, sources, schema_context, etc.)
            schema_context: Deprecated - pass via metadata['schema_context'] instead
            additional_context: Deprecated - pass via metadata['additional_context'] instead
            
        Returns:
            Generated SQL query or expression
        """
        metadata = metadata or {}
        
        # Build prompt based on type
        if prompt_type == 'query_page':
            prompt = self._get_query_page_prompt(
                schema_context=metadata.get('schema_context', ''),
                question=question,
                engine=metadata.get('engine', 'trino'),  # Extract engine from metadata
                additional_context=metadata.get('additional_context')
            )
        elif prompt_type == 'field_transform':
            prompt = self._get_field_transform_prompt(
                column_name=metadata.get('column_name', ''),
                column_type=metadata.get('column_type', ''),
                question=question
            )
        elif prompt_type == 'sql_transform':
            prompt = self._get_sql_transform_prompt(
                sources=metadata.get('sources', []),
                question=question
            )
        elif prompt_type == 'partition':
            prompt = self._get_partition_recommendation_prompt(
                columns=metadata.get('columns', []),
                question=question
            )
        elif prompt_type == 'regex_pattern_log':
            prompt = self._get_log_regex_prompt(
                sample_logs=metadata.get('sample_logs', []),
                question=question
            )
        
        # Validate input token count
        input_tokens = self._estimate_tokens(prompt)
        if input_tokens > self.max_input_tokens:
            return f"-- Error: Input too long ({input_tokens:,} tokens). Maximum allowed: {self.max_input_tokens:,} tokens.\n-- Please reduce the context or schema size."
        
        try:
            response = self.client.invoke_model(
                modelId=self.model_id,
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 1000,
                    "temperature": 0.1,
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

    def _get_query_page_prompt(self, schema_context: str, question: str, engine: str = 'trino', additional_context: Optional[str] = None) -> str:
        """Query page prompt - uses OpenSearch RAG results for DuckDB/Trino SQL generation"""
        context_section = ""
        if additional_context:
            context_section = f"\n\nAdditional Context:\n{additional_context}"

        if engine == 'trino':
            return f"""You are a SQL expert for Trino querying data from Delta Lake tables.
Based on the available data schema below, generate a Trino SQL query to answer the user's question.

Available Data Schema:
{schema_context}
{context_section}

User Question: {question}

IMPORTANT Rules for Trino:
1. If the user asks about a specific table/dataset that is NOT in the Available Data Schema above, respond with: -- Error: Table '[name]' not found. Available tables: [list table names]
2. Return ONLY the SQL query, no explanation before or after
3. Use the column names exactly as provided in the schema
4. For tables with S3 Path, use the Delta Lake table format: lakehouse.default.table_name
5. The table name is derived from the dataset name in the schema (convert to lowercase, replace spaces with underscores)
6. Use standard Trino SQL syntax
7. Add appropriate WHERE, ORDER BY, GROUP BY, LIMIT clauses as needed
8. For aggregate queries, always include relevant columns in GROUP BY
9. Do NOT use S3 paths directly - always use lakehouse.default.table_name format

Example Trino query format:
SELECT column1, column2 FROM lakehouse.default.my_table WHERE condition LIMIT 10;

SQL Query:"""
        else:  # duckdb (default)
            return f"""You are a SQL expert for DuckDB querying Parquet files on S3.
                Based on the available data schema below, generate a DuckDB SQL query to answer the user's question.

                Available Data Schema:
                {schema_context}
                {context_section}

                User Question: {question}

                IMPORTANT Rules for DuckDB:
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

    def _get_field_transform_prompt(self, column_name: str, column_type: str, question: str) -> str:
        """Field transformation prompt - generates single SQL expression"""
        return f"""You are helping transform a SINGLE COLUMN in a data pipeline.

            **Current Column:**
            - Name: "{column_name}"
            - Type: "{column_type}"

            **IMPORTANT RULES:**
            1. Generate ONLY a single SQL expression (NOT a full SELECT query)
            2. Use the column name "{column_name}" in your expression
            3. Follow the Quick Functions pattern shown below
            4. Do NOT include SELECT, FROM, WHERE, or table names
            5. Return ONLY the transformation expression

            **Quick Functions Examples:**
            - Uppercase: UPPER({column_name})
            - Lowercase: LOWER({column_name})
            - Trim: TRIM({column_name})
            - Replace: REPLACE(CAST({column_name} AS STRING), 'old', 'new')
            - Substring: SUBSTR({column_name}, 1, 10)
            - Concatenate: CONCAT({column_name}, '-suffix')
            - Type cast: CAST({column_name} AS STRING)
            - Handle nulls: COALESCE({column_name}, 'default')
            - Date format: DATE_FORMAT({column_name}, 'yyyy-MM-dd')
            - Round: ROUND({column_name}, 2)

            **Valid Output Examples:**
            ✅ UPPER(TRIM({column_name}))
            ✅ SUBSTR({column_name}, 1, 3)
            ✅ REPLACE({column_name}, '-', '_')

            **Invalid Output Examples:**
            ❌ SELECT UPPER(column) FROM table
            ❌ SELECT * FROM 's3://...'
            ❌ Any query with SELECT/FROM

            User Request: {question}

            Generate ONLY the transformation expression:"""

    def _get_sql_transform_prompt(self, sources: list, question: str) -> str:
        """SQL transformation prompt - generates full SELECT query with source schemas"""
        sources_list = []
        for s in sources:
            name = s.get('name', 'unknown')
            cols = ', '.join([f"{col.get('name', '')} ({col.get('type', '')})" for col in s.get('schema', [])])
            sources_list.append(f"- Table: \"{name}\"\n  Columns: {cols}")
        sources_text = "\n".join(sources_list) if sources_list else "No sources available"
        
        # Determine table reference based on number of sources
        num_sources = len(sources)
        if num_sources == 1:
            table_reference = "Use 'input' as the table name in your FROM clause"
            example_from = "FROM input"
        else:
            table_names = ', '.join([s.get('name', 'unknown') for s in sources])
            table_reference = f"Use the actual table names ({table_names}) in your FROM clause for JOINs"
            example_from = f"FROM {sources[0].get('name', 'source1')}"
        
        return f"""You are helping write a SQL query to transform data in an ETL pipeline.
The query must be compatible with both DuckDB (for preview) and Spark SQL (for execution).

            **Available Source Tables:**
            {sources_text}

            **IMPORTANT RULES:**
            1. Generate a complete SELECT query
            2. {table_reference}
            3. You can JOIN multiple sources if needed (use actual table names for JOINs)
            4. Use ONLY functions compatible with BOTH DuckDB AND Spark SQL
            5. Do NOT reference tables not listed above
            6. Do NOT use 's3://' paths - use table names only

            **Compatible Date/Time Functions (USE ONLY THESE):**
            - Convert to date: CAST(column AS DATE)
            - Convert to timestamp: CAST(column AS TIMESTAMP)
            - Convert date to string (YYYY-MM-DD): CAST(CAST(column AS DATE) AS STRING)
            - Extract year: YEAR(column)
            - Extract month: MONTH(column)
            - Extract day: DAY(column)
            - String functions: UPPER(), LOWER(), TRIM(), SUBSTRING()
            - Aggregate: COUNT(), SUM(), AVG(), MIN(), MAX()

            **DO NOT USE (incompatible):**
            - DATE(column) - DuckDB only
            - TO_DATE(column) - Spark only
            - DATE_FORMAT() - different syntax in DuckDB vs Spark
            - CURRENT_DATE() vs CURRENT_DATE - different syntax

            **Example Queries:**
            - Simple select: SELECT id, name, email {example_from}
            - Date to string: SELECT id, CAST(CAST(created_at AS DATE) AS STRING) as created_date {example_from}
            - Extract date parts: SELECT id, YEAR(created_at) as year, MONTH(created_at) as month {example_from}
            - Join tables (multi-source): SELECT a.id, a.name, b.value FROM {sources[0].get('name', 'source1')} a JOIN {sources[1].get('name', 'source2') if num_sources > 1 else 'source2'} b ON a.id = b.id
            - Aggregate: SELECT category, COUNT(*) as count {example_from} GROUP BY category
            - Transform: SELECT id, UPPER(name) as name_upper, CAST(CAST(created_at AS DATE) AS STRING) as date {example_from}

            User Request: {question}

            Generate a SQL query compatible with both DuckDB and Spark SQL:"""

    def _get_sql_lab_prompt(self, engine: str, tables: list, question: str) -> str:
        """SQL Lab prompt - generates queries for DuckDB or Trino"""
        engine_name = "DuckDB (for S3 Parquet files)" if engine == 'duckdb' else "Trino (distributed query engine)"
        
        examples = ""
        if engine == 'duckdb':
            examples = """
                For DuckDB queries:
                - Use read_parquet('s3://bucket/path/*.parquet') to query S3 files
                - Example: SELECT * FROM read_parquet('s3://my-bucket/data/*.parquet') WHERE date > '2024-01-01'"""
        else:
            examples = """
                For Trino queries:
                - Use lakehouse.default.table_name format
                - Example: SELECT * FROM lakehouse.default.my_table WHERE date > DATE '2024-01-01'"""
        
        return f"""You are a SQL expert helping write queries for {engine_name}.

                Available tables and schemas in the database.
                Query engine: {engine_name}
                {examples}

                User Request: {question}

                Generate a SQL query:"""

    def _get_partition_recommendation_prompt(self, columns: list, question: str) -> str:
        """Partition recommendation prompt - recommends partition columns"""
        columns_text = ", ".join([f"{col.get('name', '')} ({col.get('type', '')})" for col in columns]) if columns else "No columns available"
        
        return f"""I need to partition a dataset for optimal query performance.

            Available columns: {columns_text}

            Please recommend 1-3 columns for partitioning based on:
            1. Time-based columns (date, timestamp) are preferred
            2. Columns with moderate cardinality (10-1000 unique values)
            3. Columns commonly used in WHERE clauses
            4. Avoid high-cardinality columns (like IDs) and very low-cardinality columns (like booleans)

            Return ONLY the column names, comma-separated (e.g., "created_date, region, status").

            User Request: {question}

            Recommended partition columns:"""

    def _get_log_regex_prompt(self, sample_logs: list, question: str) -> str:
        """Log regex pattern generation prompt - generates Python regex with named groups"""
        logs_text = "\n".join(sample_logs) if sample_logs else "No sample logs provided"
        
        return f"""You are a regex expert helping to parse log files.
            Generate a Python regex pattern with named groups to extract fields from these log lines.

            Sample Log Lines:
            {logs_text}

            IMPORTANT Rules:
            1. Use Python regex syntax with named groups: (?P<field_name>pattern)
            2. Extract ALL meaningful fields from the logs
            3. Use descriptive field names (e.g., client_ip, timestamp, http_method, request_path, status_code, bytes_sent, referrer, user_agent)
            4. Make the pattern flexible to match variations in the log format
            5. Return ONLY the regex pattern, no explanation or markdown
            6. Do NOT wrap in quotes or code blocks
            7. Common patterns:
            - IP address: (?P<client_ip>\\S+)
            - Timestamp in brackets: \\[(?P<timestamp>[^\\]]+)\\]
            - HTTP request: "(?P<method>\\S+) (?P<path>\\S+) (?P<protocol>\\S+)"
            - Status code: (?P<status_code>\\d+)
            - Bytes: (?P<bytes_sent>\\S+)
            - Quoted strings: "(?P<field_name>[^"]*)"

            Example Apache Combined Log Pattern:
            ^(?P<client_ip>\\S+) \\S+ \\S+ \\[(?P<timestamp>[^\\]]+)\\] "(?P<method>\\S+) (?P<path>\\S+) (?P<protocol>\\S+)" (?P<status_code>\\d+) (?P<bytes_sent>\\S+) "(?P<referrer>[^"]*)" "(?P<user_agent>[^"]*)"

            User Request: {question}

            Generate the regex pattern:"""

    def _estimate_tokens(self, text: str) -> int:
        """
        텍스트의 토큰 수 추정
        
        Claude 토큰화 근사치:
        - 영어: 1 token ≈ 4 characters
        - 한글/중국어: 1 token ≈ 2-3 characters
        - 보수적 추정: 1 token ≈ 3 characters (다국어 고려)
        
        Args:
            text: 추정할 텍스트
            
        Returns:
            추정 토큰 수
        """
        # Conservative estimate: 1 token per 3 characters
        return len(text) // 3
    
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

    def generate_sql_with_cache(
        self,
        question: str,
        prompt_type: str = 'general',
        metadata: Optional[dict] = None,
        schema_context: Optional[str] = None,
        additional_context: Optional[str] = None
    ) -> dict:
        """
        Semantic Cache가 적용된 SQL 생성

        Returns:
            {
                "sql": str,
                "cache_hit": bool,
                "similarity": float (캐시 히트 시),
                "cached_question": str (캐시 히트 시)
            }
        """
        # 캐시 비활성화 또는 query_page가 아니면 기존 로직
        if not self.cache_enabled or prompt_type != 'query_page':
            sql = self.generate_sql(question, prompt_type, metadata, schema_context, additional_context)
            return {
                "sql": sql,
                "cache_hit": False
            }

        try:
            # 1. 질문을 벡터로 변환
            query_vector = self.embedding_service.get_embedding(question)

            # 2. Vector Search (유사 질문 검색)
            cached_result = self.vector_client.search_similar(
                query_vector,
                top_k=1,
                threshold=self.cache_threshold
            )

            # 3. Cache Hit
            if cached_result:
                print(f"✅ Cache HIT (similarity: {cached_result['similarity']:.3f})")
                return {
                    "sql": cached_result['sql_query'],
                    "cache_hit": True,
                    "similarity": cached_result['similarity'],
                    "cached_question": cached_result['question']
                }

            # 4. Cache Miss - Bedrock 호출
            print(f"❌ Cache MISS - Calling Bedrock")
            sql = self.generate_sql(question, prompt_type, metadata, schema_context, additional_context)

            # 5. Redis에 저장 (LRU가 알아서 관리)
            self.vector_client.save_cache(
                question=question,
                embedding=query_vector,
                sql_query=sql
            )

            return {
                "sql": sql,
                "cache_hit": False
            }

        except Exception as e:
            # 캐시 오류 시 폴백 (Bedrock 직접 호출)
            print(f"⚠️  Cache error (fallback to Bedrock): {e}")
            sql = self.generate_sql(question, prompt_type, metadata, schema_context, additional_context)
            return {
                "sql": sql,
                "cache_hit": False,
                "cache_error": str(e)
            }


# 싱글톤 인스턴스
_bedrock_service = None


def get_bedrock_service() -> BedrockService:
    """Bedrock 서비스 싱글톤 반환"""
    global _bedrock_service
    if _bedrock_service is None:
        _bedrock_service = BedrockService()
    return _bedrock_service
