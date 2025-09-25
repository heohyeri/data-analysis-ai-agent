async function fetchSummaryAsChat() {
  try {
    const res = await fetch("/summary");
    const data = await res.json();

    if (data.status !== "success") {
      addMessage("⚠️ 요약 불러오기 실패: " + (data.message || "알 수 없는 오류"), "bot");
      return;
    }

    // --- 🔽 여기서부터 코드가 변경됩니다 ---

    const messagesArea = document.getElementById("messagesArea");

    // 환영 메시지가 있다면 숨깁니다.
    const welcome = document.getElementById("welcomeMessage");
    if (welcome && !welcome.classList.contains("hidden")) {
        welcome.classList.add("hidden");
    }

    // 1. 메시지를 담을 bubble div를 직접 생성합니다.
    const bubble = document.createElement("div");
    bubble.className = "message bot"; // 봇 메시지 스타일 적용

    let fullHtmlContent = '<div class="message-content">';
    fullHtmlContent += "📊 <b>데이터 요약 통계 결과:</b><br><br>";

    // 2. 서버로부터 받은 HTML(data.summary)을 보안 처리 없이 그대로 사용합니다.
    for (const [fileName, htmlTable] of Object.entries(data.summary)) {
      fullHtmlContent += `<div>📄 <b>${fileName}</b></div>`;
      fullHtmlContent += `<div class="table-wrapper">${htmlTable}</div>`; // 이 부분이 이제 안전하게 렌더링됩니다.
      fullHtmlContent += "<br>";
    }

    fullHtmlContent += '</div>';
    bubble.innerHTML = fullHtmlContent;

    // 3. 생성된 bubble을 채팅창에 추가하고 스크롤을 내립니다.
    messagesArea.appendChild(bubble);
    messagesArea.scrollTop = messagesArea.scrollHeight;


  } catch (err) {
    addMessage("⚠️ 서버 요청 실패: " + err.message, "bot");
  }
}


window.fetchSummaryAsChat = fetchSummaryAsChat;