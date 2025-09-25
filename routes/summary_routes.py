from flask import Blueprint, jsonify
from services.data_service import get_summary

summary_bp = Blueprint("summary", __name__)

@summary_bp.route("/", methods=["GET"])
def summary():
    """
    업로드된 데이터의 요약 통계를 반환하는 엔드포인트
    """
    try:
        summaries = get_summary() 
        return jsonify({
            "status": "success",
            "summary": summaries
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
