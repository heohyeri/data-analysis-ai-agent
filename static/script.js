document.getElementById("fileInput").addEventListener("change", async (event) => {
    const files = event.target.files;
    if (files.length === 0) return;

    const formData = new FormData();
    for (let file of files) {
        formData.append("files", file);
    }

    try {
        showSpinner();

        const response = await fetch("/upload/", {
            method: "POST",
            body: formData
        });

        const result = await response.json();
        console.log("📂 Upload result:", result);

        if (result.status === "success") {
            updateMetrics(result.files);
            updateFileList(result.files);
        } else {
            alert("업로드 실패: " + result.error);
        }
    } catch (error) {
        console.error("업로드 중 오류:", error);
    } finally {
        hideSpinner();
    }
});

function updateMetrics(filesData) {
    const firstFile = Object.values(filesData)[0];
    if (!firstFile) return;

    document.getElementById("dataRows").innerText = firstFile.rows;
    document.getElementById("dataColumns").innerText = firstFile.columns;
    document.getElementById("nullValues").innerText = firstFile.missing_values;
    document.getElementById("dataTypes").innerText = firstFile.dtypes;

    if (firstFile.status_summary) {
        updateDataStatus(firstFile.status_summary);
    }
}

function updateDataStatus(statusText) {
    const statusDiv = document.getElementById("dataStatus");
    statusDiv.innerHTML = `
        <pre style="color:#e2e8f0; font-size:0.9rem; line-height:1.5; white-space:pre-wrap;">
${statusText}
        </pre>
    `;
}

function updateFileList(filesData) {
    const fileList = document.getElementById("fileList");
    fileList.innerHTML = "";

    Object.keys(filesData).forEach((filename) => {
        const div = document.createElement("div");
        div.style.color = "#e2e8f0";
        div.style.fontSize = "0.9rem";
        div.style.marginBottom = "4px";
        div.textContent = filename;
        fileList.appendChild(div);
    });
}

function getMode() {
    const checked = document.querySelector('input[name="mode"]:checked');
    return checked ? checked.value : "chat";
}

async function sendMessage() {
    const input = document.getElementById("messageInput");
    const message = input.value.trim();
    if (!message) return;

    addMessage(message, "user");
    input.value = "";

    try {
        const mode = getMode();
        const url = mode === "ask" ? "/chat/ask" : "/chat/";
        const payload = mode === "ask" ? { question: message } : { message };

        const response = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });

        const result = await response.json();
        console.log("🤖 Chat result:", result);

        if (mode === "ask") {
            if (result.answer) {
                addMessage(result.answer, "bot");
            }
            if (result.sources && result.sources.length) {
                const cites = result.sources.map(
                    (s, i) => `**${i+1}) ${s.source} (row ${s.row})**\n${s.preview}`
                ).join("\n\n");
                addMessage(`**근거**\n\n${cites}`, "bot");
            }
        } else {
            if (result.answer) {
                addMessage(result.answer, "bot");
            } else {
                addMessage("⚠️ 응답 오류: " + (result.error || "알 수 없음"), "bot");
            }
        }
    } catch (error) {
        console.error("Chat 오류:", error);
        addMessage("⚠️ 서버와 연결할 수 없습니다.", "bot");
    }
}


function addMessage(content, sender = "bot") {
    const messagesArea = document.getElementById("messagesArea");

    const welcome = document.getElementById("welcomeMessage");
    if (welcome && !welcome.classList.contains("hidden")) {
        welcome.classList.add("hidden");
    }

    const html = marked.parse(content, { breaks: true });
    const safeHtml = DOMPurify.sanitize(html);

    const bubble = document.createElement("div");
    bubble.className = `message ${sender}`;
    bubble.innerHTML = `<div class="message-content">${safeHtml}</div>`;
    messagesArea.appendChild(bubble);

    bubble.querySelectorAll("pre code").forEach((block) => {
        try { hljs.highlightElement(block); } catch (_) {}
    });

    messagesArea.scrollTop = messagesArea.scrollHeight;
}


function handleKeyPress(event) {
    if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        sendMessage();
    }
}

function showSpinner() {
    document.getElementById("loadingSpinner").style.display = "block";
}
function hideSpinner() {
    document.getElementById("loadingSpinner").style.display = "none";
}


async function requestEDA(graphType, columns) {
    try {
        const response = await fetch("/eda/visualize", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ graph_type: graphType, columns })
        });

        const result = await response.json();
        console.log("📊 EDA result:", result);

        if (result.chart_url) {
            addMessage(`📊 생성된 그래프: <br><img src="${result.chart_url}" style="max-width:100%; border-radius:8px;"/>`, "bot");
        } else {
            addMessage("⚠️ 그래프 생성 실패: " + (result.error || "알 수 없음"), "bot");
        }
    } catch (error) {
        console.error("EDA 오류:", error);
        addMessage("⚠️ EDA 요청 실패", "bot");
    }
}
