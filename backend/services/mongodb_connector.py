"""
MongoDB Connector Service
Provides connection and schema inference capabilities for MongoDB sources.
"""
from typing import List, Dict, Any, Tuple
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


class MongoDBConnector:
    """MongoDB connection and schema inference service."""
    
    def __init__(self, uri: str, database: str):
        """
        Initialize MongoDB connector.
        
        Args:
            uri: MongoDB connection URI (e.g., mongodb://localhost:27017)
            database: Database name
        """
        self.uri = uri
        self.database_name = database
        self.client = None
        self.db = None
    
    def __enter__(self):
        """Context manager entry - establish connection."""
        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[self.database_name]
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        if self.client:
            self.client.close()
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test MongoDB connection.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Ping the database
            self.client.admin.command('ping')
            return True, "Connection successful"
        except ConnectionFailure as e:
            return False, f"Connection failed: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def get_collections(self) -> List[str]:
        """
        Get list of collection names in the database.
        
        Returns:
            List of collection names
        """
        return self.db.list_collection_names()
    
    def infer_schema(self, collection_name: str, sample_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Infer schema from a MongoDB collection by sampling documents.
        
        Args:
            collection_name: Name of the collection
            sample_size: Number of documents to sample (default: 1000)
        
        Returns:
            List of field info dictionaries with:
                - field: field name (dot notation for nested)
                - type: inferred type name
                - occurrence: occurrence rate (0.0 - 1.0)
                - sample_size: number of documents sampled
        """
        collection = self.db[collection_name]
        
        # Get total document count
        total_count = collection.count_documents({})
        
        # Limit sample size to available documents
        actual_sample_size = min(sample_size, total_count)
        
        if actual_sample_size == 0:
            return []
        
        # Sample documents
        samples = list(collection.find().limit(actual_sample_size))
        
        # Analyze documents to infer schema
        schema = self._analyze_documents(samples)
        
        return schema
    
    def _analyze_documents(self, docs: List[Dict]) -> List[Dict[str, Any]]:
        """
        Analyze document structure and infer schema.
        
        Args:
            docs: List of document dictionaries
        
        Returns:
            List of field information
        """
        if not docs:
            return []
        
        field_stats = {}
        total_docs = len(docs)
        
        # Collect field statistics
        for doc in docs:
            flattened_fields = self._flatten_document(doc)
            
            for field, value in flattened_fields:
                if field not in field_stats:
                    field_stats[field] = {
                        'types': {},
                        'count': 0,
                    }
                
                field_stats[field]['count'] += 1
                
                # Determine type
                type_name = self._get_type_name(value)
                field_stats[field]['types'][type_name] = \
                    field_stats[field]['types'].get(type_name, 0) + 1
        
        # Convert to schema format
        schema = []
        for field, stats in field_stats.items():
            # Get most common type
            most_common_type = max(stats['types'], key=stats['types'].get)
            occurrence_rate = stats['count'] / total_docs
            
            schema.append({
                'field': field,
                'type': most_common_type,
                'occurrence': occurrence_rate,
                'sample_size': total_docs
            })
        
        # Sort by field name for consistent output
        schema.sort(key=lambda x: x['field'])
        
        return schema
    
    def _flatten_document(self, doc: Dict, parent_key: str = '') -> List[Tuple[str, Any]]:
        """
        Flatten nested document using dot notation.
        Also extracts fields from array of objects into separate array columns.
        
        Args:
            doc: Document dictionary
            parent_key: Parent key prefix (for recursion)
        
        Returns:
            List of (field_path, value) tuples
        """
        items = []
        
        for key, value in doc.items():
            # Skip MongoDB internal _id field
            if key == '_id':
                continue
            
            # Build field path with dot notation
            new_key = f"{parent_key}.{key}" if parent_key else key
            
            # Recursively flatten nested objects
            if isinstance(value, dict) and value:  # Non-empty dict
                items.extend(self._flatten_document(value, new_key))
            # Handle array of objects: extract each field as a separate array column
            elif isinstance(value, list):
                # Check if it's array of objects or array of primitives
                if len(value) > 0 and isinstance(value[0], dict):
                    # This is an array of objects - collect ALL unique fields from ALL items
                    # Not just the first item, because different items may have different fields
                    # Example: projects: [{name, budget}, {name, budget, team}]
                    all_fields = set()
                    for item in value:
                        if isinstance(item, dict):
                            all_fields.update(item.keys())
                    
                    # Extract each field as a separate array column
                    for field_name in sorted(all_fields):  # Sort for consistent ordering
                        field_path = f"{new_key}.{field_name}"
                        # Extract this field from all array elements
                        field_values = [item.get(field_name) for item in value if isinstance(item, dict)]
                        items.append((field_path, field_values))
                    # Do NOT add the original array column
                elif len(value) == 0:
                    # Empty array - skip it (don't add to schema)
                    # This prevents empty arrays from appearing as separate columns
                    pass
                else:
                    # Array of primitives (strings, numbers, etc.) - keep as-is
                    items.append((new_key, value))
            else:
                # Leaf node (primitive value)
                items.append((new_key, value))
        
        return items
    
    def _get_type_name(self, value: Any) -> str:
        """
        Get human-readable type name for a value.
        
        Args:
            value: The value to get type for
        
        Returns:
            Type name string
        """
        if value is None:
            return 'null'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'int'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            return 'string'
        elif isinstance(value, list):
            return 'array'
        elif isinstance(value, dict):
            return 'object'
        else:
            # For other types (datetime, ObjectId, etc.)
            return type(value).__name__.lower()


# Convenience function for testing connection without context manager
def test_mongodb_connection(uri: str, database: str) -> Tuple[bool, str]:
    """
    Test MongoDB connection.
    
    Args:
        uri: MongoDB connection URI
        database: Database name
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        with MongoDBConnector(uri, database) as connector:
            return connector.test_connection()
    except Exception as e:
        return False, f"Connection error: {str(e)}"
