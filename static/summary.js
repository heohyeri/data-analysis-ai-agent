async function fetchSummaryAsChat() {
  try {
    const res = await fetch("/summary");
    const data = await res.json();

    if (data.status !== "success") {
      addMessage("⚠️ 요약 불러오기 실패: " + (data.message || "알 수 없는 오류"), "bot");
      return;
    }

    const messagesArea = document.getElementById("messagesArea");


    const welcome = document.getElementById("welcomeMessage");
    if (welcome && !welcome.classList.contains("hidden")) {
        welcome.classList.add("hidden");
    }

    const bubble = document.createElement("div");
    bubble.className = "message bot";

    let fullHtmlContent = '<div class="message-content">';
    fullHtmlContent += "📊 <b>데이터 요약 통계 결과:</b><br><br>";

    for (const [fileName, htmlTable] of Object.entries(data.summary)) {
      fullHtmlContent += `<div>📄 <b>${fileName}</b></div>`;
      fullHtmlContent += `<div class="table-wrapper">${htmlTable}</div>`;
      fullHtmlContent += "<br>";
    }

    fullHtmlContent += '</div>';
    bubble.innerHTML = fullHtmlContent;

    messagesArea.appendChild(bubble);
    messagesArea.scrollTop = messagesArea.scrollHeight;


  } catch (err) {
    addMessage("⚠️ 서버 요청 실패: " + err.message, "bot");
  }
}


window.fetchSummaryAsChat = fetchSummaryAsChat;