from fastapi import APIRouter, HTTPException, Body
from typing import List, Optional, Dict, Any
from models import Domain
from pydantic import BaseModel
from datetime import datetime

router = APIRouter()

# --- Pydantic Schemas for Request Body ---
class DomainCreate(BaseModel):
    name: str
    type: str

class DomainGraphUpdate(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]

# --- API Endpoints ---

@router.get("/", response_model=List[Domain])
async def get_all_domains():
    """
    Get all domains (list view).
    """
    domains = await Domain.find_all().to_list()
    return domains

@router.get("/{id}", response_model=Domain)
async def get_domain(id: str):
    """
    Get a specific domain by ID (detail view).
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")
    return domain

@router.post("/", response_model=Domain)
async def create_domain(domain_data: DomainCreate):
    """
    Create a new domain.
    """
    # Check if domain with same name exists? (Optional, maybe allow dupes for now)
    
    new_domain = Domain(
        name=domain_data.name,
        type=domain_data.type,
        nodes=[],
        edges=[]
    )
    await new_domain.insert()
    return new_domain

@router.post("/{id}", response_model=Dict[str, str])
async def delete_domain(id: str):
    """
    Delete a domain (POST method as requested, normally DELETE).
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")
    
    await domain.delete()
    return {"message": "Domain deleted successfully"}

@router.post("/{id}/graph", response_model=Domain)
async def save_domain_graph(id: str, graph_data: DomainGraphUpdate):
    """
    Upsert graph state (nodes, edges) for a domain.
    """
    domain = await Domain.get(id)
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")
    
    domain.nodes = graph_data.nodes
    domain.edges = graph_data.edges
    domain.updated_at = datetime.utcnow()
    
    await domain.save()
    return domain
