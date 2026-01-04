"""
AI Providers 패키지
환경변수에 따라 적절한 AI Provider를 자동으로 선택합니다.
"""
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base_provider import BaseAIProvider


def get_ai_provider() -> "BaseAIProvider":
    """
    환경변수 AI_PROVIDER에 따라 적절한 AI Provider 인스턴스를 반환합니다.
    
    환경변수:
        AI_PROVIDER: "openai" (기본값) 또는 "bedrock"
    
    Returns:
        BaseAIProvider 인스턴스
    """
    provider_type = os.getenv("AI_PROVIDER", "openai").lower()
    
    if provider_type == "bedrock":
        from .bedrock_provider import BedrockProvider
        return BedrockProvider()
    else:
        # 기본값: OpenAI
        from .openai_provider import OpenAIProvider
        return OpenAIProvider()


# 싱글톤 인스턴스 (캐시)
_provider_instance = None


def get_provider() -> "BaseAIProvider":
    """
    AI Provider 싱글톤 인스턴스를 반환합니다.
    앱 시작 시 한 번만 초기화되고 재사용됩니다.
    """
    global _provider_instance
    if _provider_instance is None:
        _provider_instance = get_ai_provider()
    return _provider_instance
