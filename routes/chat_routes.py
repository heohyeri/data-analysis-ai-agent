from flask import Blueprint, request, jsonify
from services.chat_service import ask_gemini, ask_with_vector
from services.data_service import uploaded_data, generate_status_summary

chat_bp = Blueprint("chat", __name__)

@chat_bp.route("/", methods=["POST"])
def chat():
    """
    일반 대화 라우트 (업로드된 데이터 요약을 context로 포함할 수 있음)
    """
    try:
        data = request.get_json()
        question = data.get("message", "")

        if not question:
            return jsonify({"error": "질문이 비어있습니다."}), 400

        context = ""
        if uploaded_data:
            for name, df in uploaded_data.items():
                context += f"\n파일명: {name}\n{generate_status_summary(df)}\n"

        answer = ask_gemini(question, context)

        return jsonify({"answer": answer})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@chat_bp.route("/ask", methods=["POST"])
def ask():
    """
    벡터 DB 기반 질문 라우트
    - query_db로 관련 데이터 검색
    - ask_with_vector 사용 (context 포함)
    """
    try:
        data = request.get_json()
        question = data.get("question", "")

        if not question:
            return jsonify({"error": "질문이 비어있습니다."}), 400

        rag_result = ask_with_vector(question)

        return jsonify(rag_result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
