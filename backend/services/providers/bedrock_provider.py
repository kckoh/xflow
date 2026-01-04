"""
AWS Bedrock Provider (스텁)
배포 시 AWS Bedrock으로 전환할 때 사용할 Provider

TODO: 배포 전 구현 필요
- boto3 클라이언트 설정
- Claude 3 모델 연동
- IAM 권한 설정
"""
import os
import json
from typing import Dict, List, Any, Optional

from .base_provider import BaseAIProvider


class BedrockProvider(BaseAIProvider):
    """
    AWS Bedrock API를 사용하는 AI Provider
    
    지원 모델:
    - anthropic.claude-3-haiku-20240307-v1:0 (빠르고 저렴)
    - anthropic.claude-3-sonnet-20240229-v1:0 (균형)
    - anthropic.claude-3-opus-20240229-v1:0 (고성능)
    """
    
    def __init__(self):
        # TODO: boto3 클라이언트 초기화
        # import boto3
        # self.client = boto3.client('bedrock-runtime', region_name='us-east-1')
        self.model_id = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
        print(f"⚠️ Bedrock Provider initialized (STUB) with model: {self.model_id}")
        print("   → 실제 사용을 위해서는 구현이 필요합니다.")
    
    async def generate_text(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 500,
        temperature: float = 0.3
    ) -> str:
        """
        AWS Bedrock으로 텍스트 생성
        
        TODO: 실제 구현 필요
        """
        raise NotImplementedError(
            "Bedrock Provider는 아직 구현되지 않았습니다. "
            "AI_PROVIDER=openai로 설정하거나, 이 클래스를 구현해주세요."
        )
        
        # 실제 구현 예시:
        # body = json.dumps({
        #     "anthropic_version": "bedrock-2023-05-31",
        #     "max_tokens": max_tokens,
        #     "system": system_prompt or "",
        #     "messages": [{"role": "user", "content": prompt}]
        # })
        # 
        # response = self.client.invoke_model(
        #     modelId=self.model_id,
        #     body=body
        # )
        # 
        # result = json.loads(response['body'].read())
        # return result['content'][0]['text']
    
    async def generate_json(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 1000
    ) -> Dict[str, Any]:
        """
        AWS Bedrock으로 JSON 형식 응답 생성
        
        TODO: 실제 구현 필요
        """
        raise NotImplementedError(
            "Bedrock Provider는 아직 구현되지 않았습니다. "
            "AI_PROVIDER=openai로 설정하거나, 이 클래스를 구현해주세요."
        )
