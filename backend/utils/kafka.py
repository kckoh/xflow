def has_kafka_source(nodes) -> bool:
    for node in nodes or []:
        data = node.get("data", {}) if isinstance(node, dict) else {}
        source_type = (data.get("sourceType") or "").lower()
        platform = (data.get("platform") or "").lower()
        if source_type == "kafka" or "kafka" in platform:
            return True
    return False
