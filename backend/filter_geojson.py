#!/usr/bin/env python3
"""
Fully hardcoded GeoJSON attribute filter.
- Edit INPUT_PATH, OUTPUT_PATH, and ATTRS_TO_KEEP below.
- Run: python filter_geojson_attributes_hardcoded.py
"""

import json
from pathlib import Path
from typing import Dict, List, Any

# ======= HARD-CODED SETTINGS (edit these three) ==============================
INPUT_PATH = Path("datasets/NE_countries.geojson")                  # e.g., Path("data/world.geojson")
OUTPUT_PATH = Path("datasets/Countries.geojson")       # e.g., Path("out/world_min.geojson")
ATTRS_TO_KEEP: List[str] = [
    "NAME_EN",
    "NAME_FR",
    "NAME_AR",
    "ECONOMY",
    "GDP_YEAR",
    "POP_RANK",
    "POSTAL",
    "CONTINENT",
    "SUBREGION",
    "INCOME_GRP",
    "MAPCOLOR9",
    "LABELRANK",
    "POSTAL",
    "NAME_LONG"
    # add more property keys you want to keep...
]

ATTRS_TO_KEEP_ROADS: List[str] = [
    "length_km",
    "continent",
    "note",
    "toll",
    "expressway",
    "name",
    "type",
    "namealt"
    "namealtt",
    "scalerank",
    "label"

]

ATTRS_TO_KEEP_Pop: List[str] =[
    "TIMEZONE",
    "ELEVATION",
    "MEGACITY",
    "NAME",
    "NATSCALE",
    "ADM0NAME",
    "ADM1NAME",
    "GN_POP",
    "CAPIN"






]
# ============================================================================

def filter_properties(props: Dict[str, Any], keep: List[str]) -> Dict[str, Any]:
    """Return a dict containing only keys in `keep`, preserving the order of `keep`."""
    if not isinstance(props, dict):
        return {}
    return {k: props[k] for k in keep if k in props}

def process_feature(feature: Dict[str, Any], keep: List[str]) -> Dict[str, Any]:
    """Produce a new Feature with filtered properties; preserve geometry/id/bbox."""
    new_feature = {
        "type": "Feature",
        "geometry": feature.get("geometry", None),
        "properties": filter_properties(feature.get("properties", {}), keep),
    }
    if "id" in feature:
        new_feature["id"] = feature["id"]
    if "bbox" in feature:
        new_feature["bbox"] = feature["bbox"]
    return new_feature

def process_geojson(data: Dict[str, Any], keep: List[str]) -> Dict[str, Any]:
    """Filter a GeoJSON FeatureCollection (or single Feature) to only keep selected attributes."""
    gtype = data.get("type")

    if gtype == "FeatureCollection" and isinstance(data.get("features"), list):
        out = {k: v for k, v in data.items() if k != "features"}
        out["type"] = "FeatureCollection"
        out["features"] = [process_feature(f, keep) for f in data["features"]]
        return out

    if gtype == "Feature":
        return process_feature(data, keep)

    raise ValueError("Input must be a GeoJSON FeatureCollection or Feature.")

def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    with INPUT_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    filtered = process_geojson(data, ATTRS_TO_KEEP)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(filtered, f, ensure_ascii=False, indent=2)

    # Simple summary
    if filtered.get("type") == "FeatureCollection":
        total = len(data.get("features", [])) if isinstance(data, dict) else 0
        print(
            f"Done. Wrote {len(filtered['features'])} features to {OUTPUT_PATH} "
            f"(from {total}); kept attributes: {ATTRS_TO_KEEP}"
        )
    else:
        print(f"Done. Wrote a single Feature to {OUTPUT_PATH}; kept attributes: {ATTRS_TO_KEEP}")

if __name__ == "__main__":
    main()
