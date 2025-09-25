import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import platform
import uuid 


if platform.system() == "Windows":
    plt.rc("font", family="Malgun Gothic")   
elif platform.system() == "Darwin":
    plt.rc("font", family="AppleGothic")     
else:
    plt.rc("font", family="NanumGothic")     


matplotlib.rcParams["axes.unicode_minus"] = False

CHART_DIR = os.path.join("static", "charts")
os.makedirs(CHART_DIR, exist_ok=True)


def generate_chart(df: pd.DataFrame, graph_type: str, columns: list) -> str:
    """
    데이터프레임에서 그래프 생성
    - df: pandas DataFrame
    - graph_type: 'histogram' | 'boxplot' | 'bar'
    - columns: 사용자가 선택한 컬럼 (리스트)
    반환: 생성된 차트 이미지 URL (ex. /static/charts/eda_chart_xxxxx.png)
    """
    plt.clf()

    filename = f"eda_chart_{uuid.uuid4().hex[:8]}.png"
    filepath = os.path.join(CHART_DIR, filename)

    try:
        if graph_type == "histogram":
            if len(columns) != 1:
                raise ValueError("히스토그램은 1개의 컬럼만 필요합니다.")
            sns.histplot(df[columns[0]].dropna(), kde=True, bins=20)
            plt.title(f"{columns[0]} 히스토그램")

        elif graph_type == "boxplot":
            if len(columns) != 1:
                raise ValueError("박스플롯은 1개의 컬럼만 필요합니다.")
            sns.boxplot(y=df[columns[0]].dropna())
            plt.title(f"{columns[0]} 박스플롯")

        elif graph_type == "bar":
            if len(columns) != 2:
                raise ValueError("막대그래프는 2개의 컬럼(x, y)이 필요합니다.")
            x, y = columns
            grouped = df.groupby(x)[y].mean().reset_index()
            sns.barplot(x=grouped[x], y=grouped[y])
            plt.title(f"{x}별 {y} 막대그래프")
            plt.xticks(rotation=45)

        else:
            raise ValueError(f"지원하지 않는 그래프 타입: {graph_type}")


        plt.tight_layout()
        plt.savefig(filepath)
        plt.close()

        return f"/static/charts/{filename}"

    except Exception as e:
        raise RuntimeError(f"그래프 생성 실패: {str(e)}")
