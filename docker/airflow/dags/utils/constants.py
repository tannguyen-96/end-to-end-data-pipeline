import os

# ==========================================
# AWS
# ==========================================
AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-1")

# ==========================================
# S3
# ==========================================
S3_BUCKET = os.environ.get("S3_BUCKET", "zomato-pipeline-s3")
UPLOAD_MAX_RETRIES = int(os.environ.get("UPLOAD_MAX_RETRIES", "3"))

# ==========================================
# SNOWFLAKE
# ==========================================
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "ZOMATO_WAREHOUSE")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "ZOMATO")

# ==========================================
# OLLAMA
# ==========================================
BASE_URL = os.environ.get("ENRICH_BASE_URL", "http://host.docker.internal:11434")
ENRICH_MODELS = os.environ.get("ENRICH_MODELS", "qwen2.5:3b")
EMBED_MODELS = os.environ.get("EMBED_MODELS", "nomic-embed-text")
EMBED_DIM = int(os.environ.get("EMBED_DIM", "768"))