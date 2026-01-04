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
        sample_data: Optional[List[Dict]] = None,
        transformation_context: Optional[Dict[str, Any]] = None
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

        # 리니지/변환 정보 및 노드 타입별 가이드라인 구성
        lineage_info = ""
        focus_guideline = ""
        
        if transformation_context:
            op = transformation_context.get("operation", "Unknown")
            sources = ", ".join(transformation_context.get("source_tables", []))
            node_type = transformation_context.get("node_type", "T")  # Default T
            
            # 리니지 정보 텍스트
            if sources:
                lineage_info = f"\n데이터 생성 맥락:\n- 노드 타입: {node_type} (E=Extract, T=Transform, L=Load)\n- 데이터 출처(Source Tables): {sources}\n- 변환 로직(Operation): {op}"
            
            # 노드 타입별 작성 가이드라인 분기
            if node_type == 'E':
                focus_guideline = """2. **원천 데이터 중심**: 이 데이터가 '어디서 수집되었는지', '원본 그대로인지'를 명시하세요. (예: "ERP 시스템에서 수집된 원천 주문 데이터")"""
            elif node_type == 'T':
                focus_guideline = """2. **변환 로직 강조**: 이 데이터가 '어떤 처리(필터, 조인 등)를 거쳤는지'와 '그로 인해 데이터의 성격이 어떻게 변했는지' 설명하세요. (예: "모든 주문 중 취소된 건을 제외하고 정제된...")"""
            elif node_type == 'L':
                focus_guideline = """2. **활용 목적 강조**: 이 데이터가 '최종적으로 어디에 쓰이는지', '어떤 분석을 위한 것인지' 비즈니스 가치를 설명하세요. (예: "경영진 대시보드에서 매출 추이를 분석하기 위한 최종 집계 테이블")"""
            else:
                focus_guideline = """2. **데이터 흐름 반영**: 제공된 '데이터 생성 맥락'이 있다면, 이 데이터가 어디서 왔고(Source) 어떻게 변환되었는지(Operation) 설명에 포함하세요."""

        prompt = f"""다음 데이터베이스 테이블 정보를 분석하고, 데이터 카탈로그에 등재될 설명을 작성해주세요.

테이블명: {table_name}

{lineage_info}

컬럼 구조:
{column_info}
{sample_info}

작성 가이드라인:
1. **비즈니스 관점의 요약**: 이 테이블이 어떤 비즈니스 프로세스에서 사용되는지, 어떤 핵심 데이터를 담고 있는지 첫 문장에 서술하세요.
{focus_guideline}
3. **기계적 표현 지양**: "이 테이블은~", "~로 구성되어 있습니다", "저장하는 테이블입니다" 같은 불필요한 서술어를 빼고, 핵심 내용만 자연스럽게 기술하세요.
4. **간결함**: 2~3문장 내외로 핵심만 전달하세요.

잘못된 예시:
X "Product 테이블은 제품 정보를 저장합니다. id는 정수형이고 name은 문자열입니다." (너무 기계적)

좋은 예시 (T 노드):
O "주문 정보와 고객 정보를 결합하여 생성된 통합 데이터입니다. 취소된 주문을 제외하고 유효한 매출 건만을 포함하며, 고객 등급별 구매 패턴 분석에 활용됩니다."
"""
        
        return await self.generate_text(
            prompt=prompt,
            system_prompt="당신은 노련한 데이터 아키텍트입니다. 개발자와 현업 담당자가 모두 이해할 수 있는 명확하고 통찰력 있는 데이터 설명을 작성합니다.",
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
        
        prompt = f"""다음 데이터베이스 컬럼의 의미와 용도를 명확하게 설명해주세요.

테이블: {table_name}
컬럼: {column_name} ({column_type})
{sample_info}

작성 가이드라인:
1. **데이터의 의미**: 이 컬럼이 구체적으로 어떤 값을 나타내는지 설명하세요. (예: '상태 코드'보다는 '결제 승인 여부를 나타내는 공통 코드')
2. **형식 및 제약**: 샘플 값을 보고 데이터의 패턴이나 형식을 언급하세요. (예: 'YYYY-MM-DD 형식', '3자리 국가 코드')
3. **간결한 문장**: "~하는 컬럼입니다" 대신 "~를 나타냄", "~를 저장" 등으로 끝맺거나 자연스러운 평어체 문장을 사용하세요.
. **1-2문장**으로 짧게 작성하세요.

좋은 예시:
- "사용자의 최종 접속 시간을 UTC 기준으로 기록하며, 세션 만료 판단에 사용됩니다."
- "주문 상태를 나타내는 공통 코드로, 'ORD'(주문), 'SHP'(배송), 'CMP'(완료) 등의 값을 가집니다."
"""
        
        return await self.generate_text(
            prompt=prompt,
            system_prompt="당신은 데이터 아키텍트입니다. 데이터의 비즈니스적 의미를 정확하게 해석하여 설명합니다.",
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
        
        prompt = f"""다음 테이블의 각 컬럼에 대해 데이터 카탈로그용 설명을 작성해주세요.

테이블명: {table_name}

컬럼 목록:
{column_list}

각 컬럼에 대해 다음 JSON 형식으로 응답해주세요:
{{
  "columns": [
    {{"column_name": "컬럼명", "description": "비즈니스 관점의 핵심 설명"}}
  ]
}}

작성 가이드라인:
- 기계적인 타입 설명(예: '문자열이다')은 제외하고, 데이터의 **의미와 용도**를 중심으로 작성하세요.
- "~컬럼입니다" 같은 불필요한 어미를 생략하고 간결하게 작성하세요.
- 한국어로 작성하세요.
"""
        
        result = await self.generate_json(
            prompt=prompt,
            system_prompt="당신은 데이터 카탈로그 전문가입니다. JSON 형식으로만 응답합니다."
        )
        
        if isinstance(result, dict) and "columns" in result:
            return result["columns"]
        return result if isinstance(result, list) else []
