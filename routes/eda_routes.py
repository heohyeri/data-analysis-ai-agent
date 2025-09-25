from flask import Blueprint, request, jsonify
from services.eda_service import generate_chart
from services.data_service import uploaded_data

eda_bp = Blueprint("eda", __name__)

@eda_bp.route("/visualize", methods=["POST"])
def visualize():
    """
    EDA 시각화 API
    - 프론트엔드에서 그래프 타입(histogram, boxplot, bar)과 컬럼명을 전달받음
    - 업로드된 데이터프레임을 불러와 해당 시각화를 생성
    - 생성된 차트 이미지의 URL 반환
    """
    try:
        data = request.get_json()
        graph_type = data.get("graph_type")
        columns = data.get("columns", [])

        if not graph_type:
            return jsonify({"error": "그래프 타입이 필요합니다."}), 400
        if not columns:
            return jsonify({"error": "컬럼명이 필요합니다."}), 400

        if not uploaded_data:
            return jsonify({"error": "업로드된 데이터가 없습니다."}), 400

        df = list(uploaded_data.values())[0]

        chart_url = generate_chart(df, graph_type, columns)

        return jsonify({"chart_url": chart_url})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
