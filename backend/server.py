# server.py
# -*- coding: utf-8 -*-
from flask import Flask, jsonify, request, send_from_directory
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, min as spark_min, max as spark_max, avg, stddev
import os
import json
import pandas as pd
from collections import Counter
import requests

# ---- Hard-coded API key for Gemini LLM (same as frontend) ----
# ---- Gemini API key (read from gemini.txt if present; fallback to hardcoded) ----
DEFAULT_GEMINI_KEY = "YOUR_API_KEY"
_GEMINI_KEY_FILE = os.path.join(os.path.dirname(__file__), "gemini.txt")

GEMINI_KEY = DEFAULT_GEMINI_KEY
try:
    with open(_GEMINI_KEY_FILE, "r", encoding="utf-8") as _f:
        _k = _f.read().strip()
        if _k:
            GEMINI_KEY = _k
except Exception:
    # Couldn't read gemini.txt; continue with DEFAULT_GEMINI_KEY
    pass

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
# --------------------------------------------------------------

# --------------------------------------------------------------

try:
    from pandas.io.json import json_normalize
except ImportError:
    json_normalize = pd.json_normalize

app = Flask(__name__, static_folder='../frontend/static')

# Root folders
ROOT_DIR = os.path.dirname(__file__)
DATASET_DIR = os.path.join(ROOT_DIR, 'datasets')
SUMMARY_DIR = os.path.join(ROOT_DIR, 'summaries')
os.makedirs(SUMMARY_DIR, exist_ok=True)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SpatialExplorer") \
    .master("local[*]") \
    .getOrCreate()

# In-memory cache
schema_cache = {}

# ---------------------------------------------------------------------------
# Utility: file paths for on-disk caches
# ---------------------------------------------------------------------------
def enhanced_schema_path(name: str) -> str:
    # Cache file that the frontend PUTs after it computes time ranges/granularity
    return os.path.join(SUMMARY_DIR, f"{name}.enhanced_schema.json")

def auto_style_path(name: str) -> str:
    # Cache file for auto attribute suggestions (AI-generated list)
    return os.path.join(SUMMARY_DIR, f"{name}.auto_style.json")

def summary_json_path(name: str) -> str:
    # Cached schema/metadata computed server-side
    return os.path.join(SUMMARY_DIR, f"{name}.summary.json")

def ensure_parent_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def try_float(x):
    try:
        return float(x)
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Datasets listing
# ---------------------------------------------------------------------------
@app.route('/datasets.json')
def list_datasets():
    files = [f for f in os.listdir(DATASET_DIR) if f.endswith(('.csv', '.geojson'))]
    files.sort()
    return jsonify([{"name": f, "path": os.path.join(DATASET_DIR, f)} for f in files])

# ---------------------------------------------------------------------------
# Sample export
# ---------------------------------------------------------------------------
@app.route('/datasets/<name>/export', methods=['POST'])
def export_sample(name):
    try:
        data_path = os.path.join(DATASET_DIR, name)
        if name.endswith('.csv'):
            pdf = pd.read_csv(data_path, nrows=20)
            sample = pdf.to_dict(orient='records')
        elif name.endswith('.geojson'):
            with open(data_path, 'r') as f:
                geojson = json.load(f)
            features = geojson.get('features', [])
            if not features:
                raise Exception("No features found in GeoJSON")
            sample = [feat.get('properties', {}) for feat in features[:5]]
        else:
            raise Exception("Unsupported file type")
        return jsonify(sample)
    except Exception as e:
        app.logger.exception("export_sample failed")
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------------------------
# Dataset info (schema + metadata). Computes synchronously if cache is stale.
# ---------------------------------------------------------------------------
@app.route('/datasets/<name>.json')
def dataset_info(name):
    try:
        data_path = os.path.join(DATASET_DIR, name)
        if not os.path.exists(data_path):
            return jsonify({"error": f"{name} not found"}), 404

        summary_path = summary_json_path(name)
        mtime = os.path.getmtime(data_path)

        # Serve fresh cache if available
        if os.path.exists(summary_path):
            with open(summary_path, 'r') as f:
                cached = json.load(f)
            if cached.get('last_modified') == mtime and cached.get('schema'):
                return jsonify({"name": name, "schema": cached["schema"]})

        # Otherwise compute now (synchronous)
        schema = compute_schema(name, data_path)
        ensure_parent_dir(summary_path)
        with open(summary_path, 'w') as f:
            json.dump({"name": name, "schema": schema, "last_modified": mtime}, f)

        return jsonify({"name": name, "schema": schema})
    except Exception as e:
        app.logger.exception("dataset_info failed")
        return jsonify({"error": str(e)}), 500

def compute_schema(name, data_path):
    """
    Compute schema + metadata, including simple datetime detection.
    Returns a list of {name, type, metadata}.
    """
    schema = []
    if name.endswith('.csv'):
        # Read CSV via Spark for numeric stats
        df = spark.read.option("header", True).csv(data_path)
        stats = {}

        for colname in df.columns:
            dtype = df.schema[colname].dataType.simpleString()
            if dtype in ['int', 'double', 'float', 'long', 'bigint', 'decimal']:
                summary = df.select(
                    spark_min(col(colname)).alias("min"),
                    spark_max(col(colname)).alias("max"),
                    avg(col(colname)).alias("mean"),
                    stddev(col(colname)).alias("stddev"),
                    countDistinct(col(colname)).alias("countDistinct")
                ).first()
                stats[colname] = {
                    "min": try_float(summary["min"]),
                    "max": try_float(summary["max"]),
                    "mean": try_float(summary["mean"]),
                    "stddev": try_float(summary["stddev"]),
                    "countDistinct": int(summary["countDistinct"])
                }

        # Build schema entries with datetime detection via pandas sample
        for f in df.schema.fields:
            name_f = f.name
            ftype = f.dataType.simpleString()
            metadata = stats.get(name_f, {}).copy()

            try:
                pdf = pd.read_csv(data_path, usecols=[name_f], nrows=100)
                parsed = pd.to_datetime(pdf[name_f], infer_datetime_format=True, errors='coerce')
                frac = parsed.notna().sum() / max(len(parsed), 1)
                if frac > 0.8:
                    metadata["isDatetime"] = True
                    metadata["datetimeSamples"] = parsed.dropna().astype(str).tolist()[:5]
                else:
                    metadata["isDatetime"] = False
            except Exception:
                metadata["isDatetime"] = False

            schema.append({"name": name_f, "type": ftype, "metadata": metadata})

    elif name.endswith('.geojson'):
        with open(data_path, 'r') as f:
            geojson = json.load(f)
        features = geojson.get("features", [])
        if not features:
            return []

        df = json_normalize([feat.get("properties", {}) for feat in features])

        for colname in df.columns:
            col_data = df[colname].dropna()
            col_type = col_data.dtype.name
            entry_meta = {}

            if col_type.startswith(("int", "float")):
                entry_meta = {
                    "min": try_float(col_data.min()),
                    "max": try_float(col_data.max()),
                    "mean": try_float(col_data.mean()),
                    "stddev": try_float(col_data.std()) if len(col_data) > 1 else 0,
                    "countDistinct": int(col_data.nunique())
                }
            else:
                topK = Counter(col_data).most_common(5)
                entry_meta = {
                    "topKValues": [v for v, _ in topK],
                    "countDistinct": int(col_data.nunique())
                }
                # Datetime detection for object-like columns
                try:
                    parsed = pd.to_datetime(col_data, infer_datetime_format=True, errors='coerce')
                    frac = parsed.notna().sum() / max(len(col_data), 1)
                    if frac > 0.8:
                        entry_meta["isDatetime"] = True
                        entry_meta["datetimeSamples"] = parsed.dropna().astype(str).tolist()[:5]
                    else:
                        entry_meta["isDatetime"] = False
                except Exception:
                    entry_meta["isDatetime"] = False

            schema.append({"name": colname, "type": col_type, "metadata": entry_meta})

    # keep in-memory copy too
    schema_cache[name] = {"schema": schema}
    return schema

# ---------------------------------------------------------------------------
# Static + dataset serving
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)

@app.route('/datasets/<name>')
def serve_geojson(name):
    if not name.endswith('.geojson'):
        return jsonify({"error": "Only .geojson preview supported"}), 400
    path = os.path.join(DATASET_DIR, name)
    if not os.path.exists(path):
        return jsonify({"error": f"{name} not found"}), 404
    with open(path, 'r') as f:
        data = f.read()
    return data, 200, {'Content-Type': 'application/geo+json'}

# ---------------------------------------------------------------------------
# LLM style suggestions (ad-hoc; no caching here)
# ---------------------------------------------------------------------------
@app.route('/datasets/<name>/style.json')
def style_suggestions(name):
    # 1) Load cached metadata (schema)
    summary_path = summary_json_path(name)
    if not os.path.exists(summary_path):
        # compute synchronously so frontend gets something on first hit
        data_path = os.path.join(DATASET_DIR, name)
        schema = compute_schema(name, data_path)
        ensure_parent_dir(summary_path)
        with open(summary_path, 'w') as f:
            json.dump({"name": name, "schema": schema, "last_modified": os.path.getmtime(data_path)}, f)
    else:
        schema = json.load(open(summary_path, 'r')).get('schema', [])

    summary = {field["name"]: field.get("metadata", {}) for field in schema}

    # 2) Sample rows
    data_path = os.path.join(DATASET_DIR, name)
    if name.endswith('.csv'):
        df = spark.read.option("header", True).csv(data_path)
        sample_rows = df.limit(20).toJSON().collect()
    elif name.endswith('.geojson'):
        with open(data_path, 'r') as f:
            gj = json.load(f)
        sample_rows = [feat.get('properties', {}) for feat in gj.get('features', [])][:20]
    else:
        return jsonify({"error": "unsupported format"}), 400

    # 3) Build instruction
    user_prompt = request.args.get('prompt', '').strip()
    if user_prompt:
        instruction = (
            "Instruction: Based on the schema, summary, sample, and user request: \"{}\".\n"
            "Return only a JSON array with the single most suitable styling configuration. "
            "Each element must have keys: attribute, type (basic|categorized|graduated|label), "
            "fillColor (hex), strokeColor (hex).\n"
        ).format(user_prompt)
    else:
        instruction = (
            "Instruction: For each attribute, return a styling configuration as a JSON array. "
            "Each element must have keys: attribute, type (basic|categorized|graduated|label), "
            "fillColor (hex), strokeColor (hex).\n"
        )

    prompt_text = (
        "Schema: " + json.dumps(schema) + "\n"
        + "Summary: " + json.dumps(summary) + "\n"
        + "Sample: " + json.dumps(sample_rows) + "\n\n"
        + instruction
    )
    llm_payload = {"contents": [{"parts": [{"text": prompt_text}]}]}

    # 4) Call Gemini
    try:
        resp = requests.post(GEMINI_URL, params={"key": GEMINI_KEY}, json=llm_payload, timeout=60)
        if resp.status_code != 200:
            app.logger.error("LLM error: %s", resp.text)
            return jsonify([]), 200
        body = resp.json()
        raw = body.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        cleaned = (raw or "").replace("```json", "").replace("```", "").strip()
        try:
            suggestions = json.loads(cleaned)
        except Exception:
            suggestions = []
        return jsonify(suggestions)
    except Exception as e:
        app.logger.error("LLM request failed: %s", e)
        return jsonify([]), 200

# ---------------------------------------------------------------------------
# Enhanced schema cache (frontend computes time ranges & granularity)
# ---------------------------------------------------------------------------
@app.route('/datasets/<name>/enhanced_schema.json', methods=['GET', 'PUT'])
def enhanced_schema_cache_api(name):
    cache_path = enhanced_schema_path(name)

    if request.method == 'GET':
        if not os.path.exists(cache_path):
            return jsonify({"message": "No enhanced schema cache"}), 404
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
            return jsonify(data)
        except Exception as e:
            return jsonify({"error": f"Failed to read cache: {e}"}), 500

    # PUT
    payload = request.get_json(silent=True, force=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "Expected JSON object"}), 400
    try:
        ensure_parent_dir(cache_path)
        with open(cache_path, 'w') as f:
            json.dump(payload, f)
        return jsonify({"status": "ok"}), 201
    except Exception as e:
        return jsonify({"error": f"Failed to write cache: {e}"}), 500

# ---------------------------------------------------------------------------
# Auto style suggestions cache & compute
#   - GET: return cached suggestions if present, else 404
#   - PUT: save suggestions payload (accepts {"suggestions":[...]} or raw array)
#   - POST: compute suggestions with Gemini using provided schema/sample/instruction,
#           return array and persist to cache
# ---------------------------------------------------------------------------
@app.route('/datasets/<name>/auto_style.json', methods=['GET', 'PUT', 'POST'])
def auto_style_cache_api(name):
    cache_path = auto_style_path(name)

    if request.method == 'GET':
        if not os.path.exists(cache_path):
            return jsonify({"message": "No auto style cache"}), 404
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
            return jsonify(data)
        except Exception as e:
            return jsonify({"error": f"Failed to read cache: {e}"}), 500

    if request.method == 'PUT':
        payload = request.get_json(silent=True, force=True)
        if payload is None:
            return jsonify({"error": "Expected JSON"}), 400

        # Normalize payload
        if isinstance(payload, list):
            normalized = {"suggestions": payload}
        elif isinstance(payload, dict) and "suggestions" in payload:
            normalized = {"suggestions": payload["suggestions"]}
        else:
            return jsonify({"error": "Expected {'suggestions':[...]} or a JSON array"}), 400

        try:
            ensure_parent_dir(cache_path)
            with open(cache_path, 'w') as f:
                json.dump(normalized, f)
            return jsonify({"status": "ok"}), 201
        except Exception as e:
            return jsonify({"error": f"Failed to write cache: {e}"}), 500

    # POST -> compute suggestions with Gemini
    body = request.get_json(silent=True, force=True) or {}
    schema = body.get("schema", [])
    sample = body.get("sample", [])
    instruction = body.get("instruction", "")

    # If client didn't provide, try to load schema/sample from disk
    if not schema:
        path = summary_json_path(name)
        if os.path.exists(path):
            schema = json.load(open(path, 'r')).get('schema', [])
        else:
            # compute on the fly
            data_path = os.path.join(DATASET_DIR, name)
            if os.path.exists(data_path):
                schema = compute_schema(name, data_path)

    if not sample:
        data_path = os.path.join(DATASET_DIR, name)
        try:
            if name.endswith('.csv'):
                df = spark.read.option("header", True).csv(data_path)
                sample = df.limit(20).toJSON().collect()
            elif name.endswith('.geojson'):
                with open(data_path, 'r') as f:
                    gj = json.load(f)
                sample = [feat.get('properties', {}) for feat in gj.get('features', [])][:20]
        except Exception as e:
            app.logger.warning("Could not load sample for %s: %s", name, e)

    prompt_text = (
        "Schema: " + json.dumps(schema) + "\n"
        + "Sample: " + json.dumps(sample) + "\n\n"
        + (instruction or "Instruction: For each attribute, suggest styling as a JSON array with keys {attribute,type,explanation}. Allowed types: basic, categorized, graduated, label. Keep it concise.")
    )
    llm_payload = {"contents": [{"parts": [{"text": prompt_text}]}]}

    try:
        resp = requests.post(GEMINI_URL, params={"key": GEMINI_KEY}, json=llm_payload, timeout=60)
        if resp.status_code != 200:
            app.logger.error("LLM error (auto_style): %s", resp.text)
            return jsonify({"suggestions": []}), 200

        body = resp.json()
        raw = body.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        cleaned = (raw or "").replace("```json", "").replace("```", "").strip()
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, dict) and "suggestions" in parsed:
                suggestions = parsed["suggestions"]
            elif isinstance(parsed, list):
                suggestions = parsed
            else:
                suggestions = []
        except Exception as e:
            app.logger.error("Invalid JSON from LLM (auto_style): %s", e)
            suggestions = []

        # Persist to cache
        try:
            ensure_parent_dir(cache_path)
            with open(cache_path, 'w') as f:
                json.dump({"suggestions": suggestions}, f)
        except Exception as e:
            app.logger.warning("Failed saving auto style cache: %s", e)

        return jsonify({"suggestions": suggestions})
    except Exception as e:
        app.logger.error("Auto style compute failed: %s", e)
        return jsonify({"suggestions": []}), 200

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    # Tip: set host='0.0.0.0' if running inside Docker and exposing the port
    app.run(debug=True)
