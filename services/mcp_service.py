import os
import datetime
from services.chat_service import llm, memory # 기존 chat_service에서 llm과 memory 객체 import

def summarize_and_save_chat():
    """
    현재까지의 대화 기록을 요약하고, Markdown 파일로 저장합니다.
    """
    # 1. ConversationBufferMemory에서 대화 기록 가져오기
    chat_history = memory.chat_memory.messages
    if not chat_history:
        raise ValueError("저장할 대화 기록이 없습니다.")

    # 2. LLM에게 전달할 형태로 대화 기록 포맷팅
    formatted_history = ""
    for msg in chat_history:
        if msg.type == 'human':
            formatted_history += f"사용자: {msg.content}\n\n"
        elif msg.type == 'ai':
            formatted_history += f"AI: {msg.content}\n\n"

    # 3. 요약을 위한 프롬프트 정의
    prompt = f"""
다음은 데이터 분석 AI와 사용자의 대화 내용입니다.
이 대화의 핵심 내용을 바탕으로 주요 질문과 답변, 분석 결과를 요약하여 Markdown 형식의 보고서로 작성해 주세요.

---
[전체 대화 내용]
{formatted_history}
---

[요약 보고서]
"""
    # 4. LLM을 호출하여 요약 생성
    summary_response = llm.invoke(prompt)
    summary_content = summary_response.content

    # 5. 요약 내용을 파일로 저장
    save_dir = "chat_summaries"
    os.makedirs(save_dir, exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(save_dir, f"chat_summary_{timestamp}.md")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(summary_content)

    return {"file_path": file_path}