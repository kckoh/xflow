from typing import List, Dict, Any
from pydantic import BaseModel

class DomainCreate(BaseModel):
    name: str
    type: str

class DomainGraphUpdate(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]