import pandas as pd
import os

from services.vector_db import add_df_to_db, clear_db


uploaded_data = {}

def handle_file_upload(files):
    results = {}
    save_dir = "uploads"
    os.makedirs(save_dir, exist_ok=True)
    uploaded_data.clear()
    

    uploaded_data.clear()
    clear_db()

    for file in files:
        filename = file.filename
        ext = os.path.splitext(filename)[1].lower()

        save_path = os.path.join(save_dir, filename)
        file.seek(0)
        file.save(save_path)

        if ext in [".csv"]:
            try:
                df = pd.read_csv(save_path, encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(save_path, encoding="cp949")
        elif ext in [".xlsx", ".xls"]:
            df = pd.read_excel(save_path)
        elif ext in [".json"]:
            df = pd.read_json(save_path)
        else:
            raise ValueError(f"Unsupported file format: {filename}")


        uploaded_data[filename] = df

        try:
            add_df_to_db(df, source_name=filename)
        except Exception as e:
            print(f"⚠️ {filename} 벡터DB 저장 실패:", e)

        status_summary = generate_status_summary(df)

        results[filename] = {
            "path": save_path,
            "rows": df.shape[0],
            "columns": df.shape[1],
            "missing_values": int(df.isnull().sum().sum()),
            "dtypes": int(df.dtypes.nunique()),
            "columns_names": df.columns.tolist(),
            "status_summary": status_summary
        }
    print("📂 Upload result:", results)
    return results


def generate_status_summary(df):

    missing_total = int(df.isnull().sum().sum())
    missing_detail = df.isnull().sum()
    missing_cols = {col: int(val) for col, val in missing_detail.items() if val > 0}

    dtype_unique = df.dtypes.astype(str).unique().tolist()

    summary_lines = []
    summary_lines.append(f"총 {df.shape[0]}개의 행과 {df.shape[1]}개의 컬럼이 있습니다.")
    summary_lines.append(f"컬럼 목록: {', '.join(df.columns)}")

    if missing_total > 0:
        missing_info = ", ".join([f"{col}({val})" for col, val in missing_cols.items()])
        summary_lines.append(f"결측치: 총 {missing_total}개 (위치: {missing_info})")
    else:
        summary_lines.append("결측치: 없음")

    summary_lines.append(f"데이터 타입 종류: {', '.join(dtype_unique)}")

    return "\n".join(summary_lines)


def get_summary():
    if not uploaded_data:
        raise ValueError("No data uploaded")

    latest_file = list(uploaded_data.keys())[-1]
    df = uploaded_data[latest_file]

    # desc_html = df.describe(include="all").to_html(classes="table table-striped", border=0)
    desc_html = df.describe(include="all").to_html(classes="summary-table", border=0)


    return {latest_file: desc_html}
