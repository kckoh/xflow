"""
AI Provider 베이스 클래스
OpenAI, AWS Bedrock 등 다양한 AI 서비스를 통합하기 위한 추상 인터페이스
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional


class BaseAIProvider(ABC):
    """
    AI Provider 추상 베이스 클래스
    모든 AI 서비스 제공자는 이 클래스를 상속받아 구현합니다.
    """
    
    @abstractmethod
    async def generate_text(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 500,
        temperature: float = 0.3
    ) -> str:
        """
        텍스트 생성 (기본 메서드)
        
        Args:
            prompt: 사용자 프롬프트
            system_prompt: 시스템 프롬프트 (선택)
            max_tokens: 최대 토큰 수
            temperature: 창의성 조절 (0.0 ~ 1.0)
        
        Returns:
            생성된 텍스트
        """
        pass
    
    @abstractmethod
    async def generate_json(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 1000
    ) -> Dict[str, Any]:
        """
        JSON 형식 응답 생성
        
        Args:
            prompt: 사용자 프롬프트
            system_prompt: 시스템 프롬프트 (선택)
            max_tokens: 최대 토큰 수
        
        Returns:
            파싱된 JSON 딕셔너리
        """
        pass
    
    # ===== 카탈로그 전용 고수준 메서드 =====
    
    async def generate_table_description(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        sample_data: Optional[List[Dict]] = None
    ) -> str:
        """
        테이블 설명 자동 생성
        """
        column_info = "\n".join([
            f"- {col.get('name', 'unknown')}: {col.get('type', 'unknown')}"
            for col in columns
        ])
        
        sample_info = ""
        if sample_data and len(sample_data) > 0:
            sample_info = f"\n\n샘플 데이터 (첫 3건):\n{sample_data[:3]}"
        
        prompt = f"""다음 데이터베이스 테이블 정보를 분석하고 한국어로 테이블 설명을 생성해주세요.

테이블명: {table_name}

컬럼 구조:
{column_info}
{sample_info}

요구사항:
1. 2-3문장으로 간결하게 설명
2. 테이블의 주요 용도와 저장 데이터 유형 설명
3. 전문 용어 사용 시 쉽게 풀어서 설명
"""
        
        return await self.generate_text(
            prompt=prompt,
            system_prompt="당신은 데이터 카탈로그 전문가입니다. 간결하고 명확한 설명을 작성합니다.",
            max_tokens=300
        )
    
    async def generate_column_description(
        self,
        table_name: str,
        column_name: str,
        column_type: str,
        sample_values: Optional[List[Any]] = None
    ) -> str:
        """
        단일 컬럼 설명 자동 생성
        """
        sample_info = ""
        if sample_values:
            sample_info = f"\n샘플 값: {sample_values[:5]}"
        
        prompt = f"""다음 데이터베이스 컬럼 정보를 분석하고 한국어로 컬럼 설명을 생성해주세요.

테이블명: {table_name}
컬럼명: {column_name}
데이터 타입: {column_type}
{sample_info}

요구사항:
1. 1-2문장으로 간결하게 설명
2. 컬럼의 용도와 저장 데이터 의미 설명
"""
        
        return await self.generate_text(
            prompt=prompt,
            system_prompt="당신은 데이터 카탈로그 전문가입니다.",
            max_tokens=150
        )
    
    async def generate_all_column_descriptions(
        self,
        table_name: str,
        columns: List[Dict[str, Any]]
    ) -> List[Dict[str, str]]:
        """
        테이블의 모든 컬럼 설명을 한 번에 생성
        """
        column_list = "\n".join([
            f"- {col.get('name', 'unknown')} ({col.get('type', 'unknown')})"
            for col in columns
        ])
        
        prompt = f"""다음 테이블의 모든 컬럼에 대해 설명을 생성해주세요.

테이블명: {table_name}

컬럼 목록:
{column_list}

각 컬럼에 대해 다음 JSON 형식으로 응답해주세요:
{{
  "columns": [
    {{"column_name": "컬럼명", "description": "컬럼 설명"}},
    ...
  ]
}}

요구사항:
- 각 설명은 1문장으로 간결하게
- 한국어로 작성
- 반드시 유효한 JSON만 출력
"""
        
        result = await self.generate_json(
            prompt=prompt,
            system_prompt="당신은 데이터 카탈로그 전문가입니다. JSON 형식으로만 응답합니다."
        )
        
        if isinstance(result, dict) and "columns" in result:
            return result["columns"]
        return result if isinstance(result, list) else []
