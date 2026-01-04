"""
AI 데이터 카탈로그 서비스
Provider 패턴을 사용하여 OpenAI/Bedrock 등 다양한 AI 서비스를 지원합니다.
"""
from typing import Dict, List, Any, Optional
from services.providers import get_provider


async def generate_table_description(
    table_name: str,
    columns: List[Dict[str, Any]],
    sample_data: Optional[List[Dict]] = None
) -> str:
    """
    테이블 설명을 AI로 자동 생성합니다.
    
    Args:
        table_name: 테이블 이름
        columns: 컬럼 정보 리스트 [{"name": "col1", "type": "string"}, ...]
        sample_data: 샘플 데이터 (선택)
    
    Returns:
        생성된 테이블 설명
    """
    provider = get_provider()
    return await provider.generate_table_description(
        table_name=table_name,
        columns=columns,
        sample_data=sample_data
    )


async def generate_column_description(
    table_name: str,
    column_name: str,
    column_type: str,
    sample_values: Optional[List[Any]] = None
) -> str:
    """
    단일 컬럼 설명을 AI로 자동 생성합니다.
    
    Args:
        table_name: 테이블 이름
        column_name: 컬럼 이름
        column_type: 컬럼 데이터 타입
        sample_values: 샘플 값 리스트 (선택)
    
    Returns:
        생성된 컬럼 설명
    """
    provider = get_provider()
    return await provider.generate_column_description(
        table_name=table_name,
        column_name=column_name,
        column_type=column_type,
        sample_values=sample_values
    )


async def generate_all_column_descriptions(
    table_name: str,
    columns: List[Dict[str, Any]]
) -> List[Dict[str, str]]:
    """
    테이블의 모든 컬럼 설명을 한 번에 생성합니다.
    
    Args:
        table_name: 테이블 이름
        columns: 컬럼 정보 리스트
    
    Returns:
        [{"column_name": "...", "description": "..."}, ...]
    """
    provider = get_provider()
    return await provider.generate_all_column_descriptions(
        table_name=table_name,
        columns=columns
    )
