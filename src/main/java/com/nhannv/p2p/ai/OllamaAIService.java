package com.nhannv.p2p.ai;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Chiến lược AI cục bộ kết nối tới <b>Ollama Local Server ({@code localhost:11434})</b>.
 * <p>
 * Cho phép hệ thống P2P chạy <b>100% Offline trong mạng LAN nội bộ</b> mà vẫn sử dụng được
 * mô hình ngôn ngữ lớn (LLM) cục bộ, đảm bảo quyền riêng tư dữ liệu tuyệt đối.
 */
public class OllamaAIService implements AIService {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 11434;
    private static final String DEFAULT_MODEL = "qwen2.5:1.5b";

    private final HttpClient httpClient;
    private volatile String host = DEFAULT_HOST;
    private volatile int port = DEFAULT_PORT;
    private volatile String modelName = DEFAULT_MODEL;
    private volatile boolean enabled = true;

    public OllamaAIService() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    public void setModelName(String modelName) {
        if (modelName != null && !modelName.isBlank()) {
            this.modelName = modelName.trim();
        }
    }

    public String getModelName() {
        return modelName;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public String providerName() {
        return "Ollama Local (" + modelName + ")";
    }

    @Override
    public boolean isAvailable() {
        if (!enabled) {
            return false;
        }
        try (Socket probe = new Socket()) {
            probe.connect(new InetSocketAddress(host, port), 180);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public AIMetadata analyzeFileMetadata(
            File file,
            String sha256Hash,
            String extractedSnippet,
            String sensitiveWarning
    ) throws Exception {
        String prompt = "Phân tích tệp P2P sau và trả về đúng 2 dòng:\n"
                + "TAGS: <3-5 từ khóa cách nhau dấu phẩy>\n"
                + "SUMMARY: <1 câu tóm tắt tiếng Việt dưới 140 ký tự>\n"
                + "Tên tệp: " + file.getName() + "\n"
                + "Nội dung đầu: " + (extractedSnippet == null ? "" : extractedSnippet);

        String reply = callOllamaGenerate(prompt);
        return GeminiAIService.parseTagsAndSummary(sha256Hash, reply, sensitiveWarning, providerName());
    }

    @Override
    public String generatePreDownloadPreview(
            String fileName,
            long fileSize,
            String existingTags,
            String snippetText
    ) throws Exception {
        String prompt = "Tóm tắt nhanh tệp \"" + fileName + "\" (" + fileSize + " bytes) bằng tiếng Việt "
                + "dựa trên 8 KB đầu tiên sau đây để người dùng quyết định có tải về hay không:\n"
                + (snippetText == null ? "" : snippetText);
        return callOllamaGenerate(prompt);
    }

    @Override
    public String diagnoseNetworkIssue(NetworkDiagnosticContext ctx) throws Exception {
        String prompt = "Chẩn đoán lỗi mạng P2P LAN/Radmin VPN từ log sau và nêu cách khắc phục bằng tiếng Việt:\n"
                + "Firewall mở: " + ctx.firewallConfigured() + ", Peers: " + ctx.onlinePeersCount() + "\n"
                + ctx.recentNetworkLogs();
        return callOllamaGenerate(prompt);
    }

    private String callOllamaGenerate(String prompt) throws Exception {
        String endpoint = "http://" + host + ":" + port + "/api/generate";
        String body = "{\"model\":" + GeminiAIService.toJsonString(modelName)
                + ",\"prompt\":" + GeminiAIService.toJsonString(prompt)
                + ",\"stream\":false}";

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .timeout(Duration.ofSeconds(6))
                .header("Content-Type", "application/json; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IllegalStateException("Ollama HTTP " + resp.statusCode());
        }
        String marker = "\"response\"";
        String json = resp.body();
        int idx = json.indexOf(marker);
        if (idx < 0) {
            throw new IllegalStateException("Ollama không trả về trường response");
        }
        int colon = json.indexOf(':', idx + marker.length());
        int quote = json.indexOf('"', colon + 1);
        return GeminiAIService.readUnescapedJsonString(json, quote + 1);
    }
}
