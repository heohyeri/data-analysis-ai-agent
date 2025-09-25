from flask import Flask, render_template
from flask_cors import CORS
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

@app.route("/")
def index():
    return render_template("index.html")


from routes.upload_routes import upload_bp
from routes.chat_routes import chat_bp
from routes.summary_routes import summary_bp
from routes.eda_routes import eda_bp
from routes.mcp_routes import mcp_bp

app.register_blueprint(mcp_bp, url_prefix="/mcp")
app.register_blueprint(eda_bp, url_prefix="/eda")
app.register_blueprint(summary_bp, url_prefix="/summary")
app.register_blueprint(upload_bp, url_prefix="/upload")
app.register_blueprint(chat_bp, url_prefix="/chat")

if __name__ == "__main__":
    app.run(debug=True)
