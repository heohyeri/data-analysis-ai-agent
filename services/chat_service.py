from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationChain
import os

from services.vector_db import query_db

llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-flash",
    google_api_key=os.getenv("GOOGLE_API_KEY")
)

memory = ConversationBufferMemory(return_messages=True)

conversation = ConversationChain(
    llm=llm,
    memory=memory,
    verbose=True
)


def ask_gemini(question: str, context: str = "") -> str:
    """
    Gemini 모델에 질문하기 (대화 히스토리 포함)
    - question: 사용자의 질문
    - context: 벡터 DB에서 검색된 참고 텍스트 (선택적)
    """
    if context:
        prompt = f"""
당신은 데이터 분석 AI입니다. 아래 '참고 데이터'만을 근거로 정확히 답하세요.

[참고 데이터]
{context}

[질문]
{question}

- 숫자/통계는 그대로 인용하세요.
- 근거가 없으면 '관련 데이터를 찾을 수 없습니다'라고 답하세요.
"""
    else:
        prompt = question

    return conversation.predict(input=prompt)


def ask_with_vector(question: str, top_k: int = 5):
    """
    질문을 받아 벡터 DB에서 관련 데이터를 검색한 후,
    Gemini 모델에 context와 함께 전달하여 답변 생성
    → 답변과 출처를 함께 반환
    """
    hits = query_db(question, top_k=top_k) 
    context_text = "\n".join([h["text"] for h in hits]) if hits else ""

    answer = ask_gemini(question, context=context_text if context_text else "")

    sources = []
    for h in hits:
        preview = h["text"][:180] + "…" if len(h["text"]) > 180 else h["text"]
        sources.append({
            "source": h["source"],
            "row": h["row"],
            "preview": preview
        })

    return {"answer": answer, "sources": sources}
