"""
ChromaDB + Gemini 기반의 벡터 저장/검색 모듈
- DataFrame 또는 CSV 데이터를 벡터화해서 Chroma DB에 저장
- 질문을 임베딩하여 유사한 텍스트 검색
- 각 row의 출처(파일명, 행 번호) 메타데이터 포함
"""

import os
import pandas as pd
from typing import List, Dict, Any

import chromadb
import google.generativeai as genai

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
genai.configure(api_key=GOOGLE_API_KEY)

chroma_client = chromadb.PersistentClient(path="./chroma_db")

collection = chroma_client.get_or_create_collection(name="data_collection")


def get_gemini_embedding(text: str) -> List[float]:
    """Gemini API로 텍스트 임베딩 생성"""
    response = genai.embed_content(
        model="models/embedding-001",
        content=text
    )
    return response["embedding"]


def add_df_to_db(df: pd.DataFrame, source_name: str = "uploaded_df"):
    """
    DataFrame을 row 단위로 벡터 DB에 저장
    각 row를 "컬럼명: 값" 형태 문자열로 변환하여 embedding
    """
    docs, ids, embs, metas = [], [], [], []

    for i, row in df.iterrows():
        row_text = ", ".join([f"{col}: {row[col]}" for col in df.columns])
        doc_id = f"{source_name}_row{i}"

        docs.append(row_text)
        ids.append(doc_id)
        embs.append(get_gemini_embedding(row_text))
        metas.append({"source": source_name, "row": int(i)})

    collection.add(
        ids=ids,
        documents=docs,
        embeddings=embs,
        metadatas=metas
    )

    print(f"✅ {len(docs)}개의 row를 벡터 DB에 추가했습니다. (DataFrame: {source_name})")


def add_csv_to_db(file_path: str):
    """CSV 파일을 읽어 벡터 DB에 저장"""
    df = pd.read_csv(file_path)
    source_name = os.path.basename(file_path)

    docs, ids, embs, metas = [], [], [], []

    for i, row in df.iterrows():
        row_text = ", ".join([f"{col}: {row[col]}" for col in df.columns])
        doc_id = f"{source_name}_row{i}"

        docs.append(row_text)
        ids.append(doc_id)
        embs.append(get_gemini_embedding(row_text))
        metas.append({"source": source_name, "row": int(i)})

    collection.add(
        ids=ids,
        documents=docs,
        embeddings=embs,
        metadatas=metas
    )

    print(f"✅ {len(docs)}개의 row를 벡터 DB에 추가했습니다. ({file_path})")


def query_db(question: str, top_k: int = 3) -> List[dict]:
    q_emb = get_gemini_embedding(question)
    results = collection.query(
        query_embeddings=[q_emb],
        n_results=top_k
    )
    docs = results.get("documents", [[]])[0]
    ids = results.get("ids", [[]])[0]

    hits = []
    for i, doc in enumerate(docs):
        hits.append({
            "text": doc,
            "source": ids[i] if i < len(ids) else None,
            "row": i
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
    add_csv_to_db("샘플데이터.csv")
    q = "시험년도에서 가장 최근 값은?"
    answers = query_db(q)
    print("검색된 유사 텍스트:", answers)

