
async function fetchSummaryAsChat() {
  try {
    const res = await fetch("/summary");
    const data = await res.json();

    if (data.status !== "success") {
      addMessage("⚠️ 요약 불러오기 실패: " + (data.message || "알 수 없는 오류"), "bot");
      return;
    }

    let message = "📊 <b>데이터 요약 통계 결과:</b><br><br>";

    for (const [fileName, htmlTable] of Object.entries(data.summary)) {
      message += `<div>📄 <b>${fileName}</b></div>`;
      message += `<div class="table-wrapper">${htmlTable}</div>`;
      message += "<br>";
    }

    addMessage(message, "bot");

  } catch (err) {
    addMessage("⚠️ 서버 요청 실패: " + err.message, "bot");
  }
}


window.fetchSummaryAsChat = fetchSummaryAsChat;
