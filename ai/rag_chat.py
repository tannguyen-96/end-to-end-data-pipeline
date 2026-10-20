import os
import numpy as np
import pandas as pd
import streamlit as st
import psycopg2
from dotenv import load_dotenv
import requests
from pgvector.psycopg2 import register_vector

load_dotenv()

# Local Ollama settings
EMBEDDING_MODEL = os.getenv("EMBED_MODELS", "nomic-embed-text")
CHAT_MODEL = os.getenv("CHAT_MODELS", os.getenv("ENRICH_MODELS", "qwen2.5:3b"))
# Use host if running outside Docker
OLLAMA_BASE_URL = "http://localhost:11434"
TOP_K = int(os.getenv("RAG_TOP_K", "5"))

# Postgres settings (for review_embeddings)
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "postgres")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_SSLMODE = os.getenv("POSTGRES_SSLMODE", "prefer")


def get_postgres_connection():
    conn_pg = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode=PG_SSLMODE,
    )
    register_vector(conn_pg)
    return conn_pg


def embed(texts):
    """Embed text(s) using Ollama /api/embeddings."""
    if isinstance(texts, str):
        texts = [texts]

    embeddings = []
    for text in texts:
        try:
            resp = requests.post(
                f"{OLLAMA_BASE_URL}/api/embeddings",
                json={"model": EMBEDDING_MODEL, "prompt": text},
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()
            emb = data.get("embedding")
            if emb is None:
                # Some ollama versions return 'embedding' or under different key
                if "data" in data and data["data"]:
                    emb = data["data"][0].get("embedding")
            if emb is None:
                raise ValueError("No embedding returned from Ollama")
            embeddings.append(emb)
        except Exception as e:
            st.error(f"Failed to embed text: {e}")
            raise
    return embeddings


def _to_float_list(v):
    if v is None:
        return None
    if isinstance(v, (list, tuple, np.ndarray)):
        return [float(x) for x in v]
    return None


def search_reviews(question: str, top_k: int = TOP_K):
    conn_pg = get_postgres_connection()
    try:
        cur = conn_pg.cursor()
        # Pull relevant fields; join not needed since we have review data in embeddings table structure
        cur.execute(
            """
            SELECT review_id, order_id, restaurant_id, rating, comment, embedding, 1 - (embedding <=> %s::vector) AS score
            FROM public.review_embeddings
            WHERE 1 - (embedding <=> %s::vector) > 0.0
            ORDER BY 1 - (embedding <=> %s::vector) DESC
            LIMIT %s
            """,
            (embed(question)[0], embed(question)[0], embed(question)[0], top_k),
        )
        rows = cur.fetchall()
        df = pd.DataFrame(
            rows,
            columns=["review_id", "order_id", "restaurant_id", "rating", "comment", "embedding", "score"],
        )
        # df["embedding"] = df["embedding"].apply(_to_float_list)
        # df = df.dropna(subset=["embedding"])
        return df
    finally:
        conn_pg.close()


def cosine_similarity(vec1, vec2):
    v1 = np.array(vec1, dtype=float)
    v2 = np.array(vec2, dtype=float)
    denom = (np.linalg.norm(v1) * np.linalg.norm(v2))
    if denom == 0 or np.isclose(denom, 0.0):
        return 0.0
    return float(np.dot(v1, v2) / denom)


def find_similar_reviews(question: str, df: pd.DataFrame, top_k: int = TOP_K):
    qvec = embed(question)[0]
    scores = []
    for _, row in df.iterrows():
        scores.append(cosine_similarity(row["embedding"], qvec))
    df_res = df.copy()
    df_res["score"] = scores
    return df_res.nlargest(top_k, "score")


def ask_llm(question: str, top_reviews: pd.DataFrame):
    context = ""
    for _, row in top_reviews.iterrows():
        rating = row.get("rating", "N/A")
        comment = row.get("comment", "")
        context += f"({rating} stars) {comment}\n\n"

    system_prompt = (
        "Answer ONLY using the customer reviews provided. "
        "Be concise. If the reviews don't contain the answer, say so."
    )
    user_prompt = f"Question: {question}\n\nReviews:\n{context}"

    try:
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json={
                "model": CHAT_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "stream": False,
                "options": {"temperature": 0.2},
            },
            timeout=180,
        )
        resp.raise_for_status()
        data = resp.json()
        msg = data.get("message", {})
        content = msg.get("content", "").strip()
        return content or "No response from model."
    except Exception as e:
        st.error(f"Failed to call LLM: {e}")
        raise
    
# if __name__ == "__main__":
#     prompt = "5 worst reviews"
#     qvec = embed(prompt)
#     print(qvec[0])
#     df = search_reviews(prompt, top_k=TOP_K)
#     print(df)
#     if len(df) > 0:
#         answer = ask_llm(prompt, df)
#         print("Answer:")
#         print(answer)


st.set_page_config(page_title="RAG Chat (Ollama + pgvector)", layout="wide")
st.title("RAG Chat Application")
st.caption(f"Using embeddings from Postgres (public.review_embeddings) via pgvector; LLM/embeddings via Ollama")

# review_df = load_reviews_from_postgres(limit=10000)
# st.write(f"Loaded {len(review_df)} reviews with embeddings.")
# if len(review_df) > 0:
#     dim = len(review_df.iloc[0]["embedding"]) if review_df.iloc[0]["embedding"] else 0
#     st.write(f"Embedding dimension: {dim}")

# if len(review_df) > 0:
#     st.dataframe(
#         review_df[["review_id", "restaurant_id", "rating", "comment"]].head(20),
#         hide_index=True,
#         use_container_width=True,
#     )

question = st.text_input(
    "Ask a question about your reviews:",
    placeholder="e.g. What are the most common complaints about delivery?",
)

if question:
    top_reviews = search_reviews(question, top_k=TOP_K)
    answer = ask_llm(question, top_reviews)
    st.markdown("**Answer:**")
    st.write(answer)

    with st.expander("Reviews used to build this answer"):
        st.dataframe(
            top_reviews[["restaurant_id", "rating", "comment", "score"]],
            hide_index=True,
        )
# elif question and len(review_df) == 0:
#     st.warning("No embeddings found in public.review_embeddings. Run the embedding pipeline first.")
