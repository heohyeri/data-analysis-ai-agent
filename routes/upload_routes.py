from flask import Blueprint, request, jsonify
from services.data_service import handle_file_upload, get_summary

upload_bp = Blueprint("upload", __name__)

@upload_bp.route("/", methods=["POST"])
def upload_file():
    try:
        files = request.files.getlist("files")
        results = handle_file_upload(files)
        return jsonify({"status": "success", "files": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@upload_bp.route("/summary", methods=["GET"])
def summary():
    try:
        summaries = get_summary()
        return jsonify(summaries)
    except Exception as e:
        return jsonify({"error": str(e)}), 500



