from flask import Blueprint, jsonify
from services.mcp_service import summarize_and_save_chat

mcp_bp = Blueprint("mcp", __name__)

@mcp_bp.route("/save_chat", methods=["POST"])
def save_chat():
    """
    대화 기록을 요약하고 파일로 저장하는 라우트
    """
    try:
        result = summarize_and_save_chat()
        return jsonify({
            "status": "success",
            "message": "대화 기록이 성공적으로 요약 및 저장되었습니다.",
            "file_path": result["file_path"]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500