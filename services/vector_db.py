import os
import pandas as pd
from typing import List, Dict, Any
import asyncio

import chromadb
import google.generativeai as genai

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
genai.configure(api_key=GOOGLE_API_KEY)


chroma_client = chromadb.PersistentClient(path="./chroma_db")
collection = chroma_client.get_or_create_collection(name="data_collection")


def get_gemini_embedding(text: str) -> List[float]:
    """[동기] Gemini API로 텍스트 임베딩 생성 (query_db용)"""
    response = genai.embed_content(
        model="models/embedding-001",
        content=text
    )
    return response["embedding"]


async def get_gemini_embeddings_batch_async(texts: List[str]) -> List[List[float]]:
    """[비동기] Gemini API로 텍스트 배치의 임베딩을 생성"""
    response = await genai.embed_content_async(
        model="models/embedding-001",
        content=texts,
        task_type="RETRIEVAL_DOCUMENT"
    )
    return response["embedding"]


async def add_df_to_db_async(df: pd.DataFrame, source_name: str = "uploaded_df", batch_size: int = 100):
    """
    [비동기] DataFrame을 벡터 DB에 저장 (비동기 병렬 처리 적용)
    """
    docs, ids, metas = [], [], []
    for i, row in df.iterrows():
        row_values = [str(val) if pd.notna(val) else "" for val in row]
        row_text = ", ".join([f"{col}: {val}" for col, val in zip(df.columns, row_values)])
        doc_id = f"{source_name}_row{i}"
        docs.append(row_text)
        ids.append(doc_id)
        metas.append({"source": source_name, "row": int(i)})

    embedding_tasks = []
    for i in range(0, len(docs), batch_size):
        batch_docs_content = docs[i:i + batch_size]
        task = get_gemini_embeddings_batch_async(batch_docs_content)
        embedding_tasks.append(task)
    
    print(f"🚀 Starting concurrent embedding for {len(embedding_tasks)} batches...")
    all_embeddings_results = await asyncio.gather(*embedding_tasks)
    print("✅ All embeddings processed.")

    all_embeddings = [emb for batch_embs in all_embeddings_results for emb in batch_embs]
    
    for i in range(0, len(ids), batch_size):
        collection.add(
            ids=ids[i:i + batch_size],
            documents=docs[i:i + batch_size],
            embeddings=all_embeddings[i:i + batch_size],
            metadatas=metas[i:i + batch_size]
        )
        print(f"📦 Added batch {i // batch_size + 1} to ChromaDB.")

    print(f"✅ Total {len(docs)} rows have been added to the Vector DB. (DataFrame: {source_name})")


def add_df_to_db(df: pd.DataFrame, source_name: str = "uploaded_df"):
    """DataFrame을 비동기 방식으로 처리하는 메인 함수"""
    asyncio.run(add_df_to_db_async(df, source_name))


def add_csv_to_db(file_path: str):
    """CSV 파일을 읽어 벡터 DB에 저장"""
    df = pd.read_csv(file_path)
    source_name = os.path.basename(file_path)
    add_df_to_db(df, source_name=source_name)


def query_db(question: str, top_k: int = 3) -> List[dict]:
    """질문을 받아 유사한 문서를 DB에서 검색 (동기 방식)"""
    q_emb = get_gemini_embedding(question)
    results = collection.query(
        query_embeddings=[q_emb],
        n_results=top_k
    )
    docs = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]

    hits = []
    for i, doc in enumerate(docs):
        meta = metadatas[i] or {}
        hits.append({
            "text": doc,
            "source": meta.get("source"),
            "row": meta.get("row")
        })
    return hits


def clear_db():
    """컬렉션 전체 삭제 후 새로 생성"""
    try:
        chroma_client.delete_collection(name="data_collection")
        global collection
        collection = chroma_client.get_or_create_collection(name="data_collection")
        print("🧹 벡터 DB 완전히 초기화 완료 (컬렉션 삭제 후 재생성).")
    except Exception as e:
        print("⚠️ 벡터 DB 초기화 실패:", e)


if __name__ == "__main__":
    sample_data = {'col1': range(250), 'col2': [f'text_{i}' for i in range(250)]}
    sample_df = pd.DataFrame(sample_data)
    sample_df.to_csv("샘플데이터.csv", index=False)
    
    add_csv_to_db("샘플데이터.csv")
    q = "text_50과 관련된 내용은?"
    answers = query_db(q)
    print("검색된 유사 텍스트:", answers)