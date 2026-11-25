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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Chiến lược AI cục bộ kết nối tới <b>Ollama Local Server ({@code localhost:11434})</b>.
 * <p>
 * Cho phép hệ thống P2P chạy <b>100% Offline trong mạng LAN nội bộ</b> mà vẫn sử dụng được
 * mô hình ngôn ngữ lớn (LLM) cục bộ, đảm bảo quyền riêng tư dữ liệu tuyệt đối.
 *
 * <h2>Các Cải tiến Kỹ thuật</h2>
 * <ul>
 *   <li><b>Cache kiểm tra sẵn sàng (P2-6):</b> Cache kết quả probe socket trong 8 giây để không chặn
 *       luồng giao diện khi gọi định kỳ {@code getActiveProviderBadgeText()}.</li>
 *   <li><b>Phòng vệ chỉ mục JSON (A6):</b> Kiểm tra chặt chẽ {@code idx, colon, quote != -1} trước khi giải mã.</li>
 * </ul>
 */
public class OllamaAIService implements AIService {

    private static final Logger LOG = Logger.getLogger(OllamaAIService.class.getName());

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 11434;
    private static final String DEFAULT_MODEL = "qwen2.5:1.5b";
    private static final long AVAILABILITY_CACHE_MS = 8_000L;

    private final HttpClient httpClient;
    private volatile String host = DEFAULT_HOST;
    private volatile int port = DEFAULT_PORT;
    private volatile String modelName = DEFAULT_MODEL;
    private volatile boolean enabled = true;

    // Cache kết quả thăm dò sẵn sàng
    private final AtomicBoolean cachedAvailable = new AtomicBoolean(false);
    private final AtomicLong lastProbeTime = new AtomicLong(0L);

    public OllamaAIService() {
        HttpClient client = null;
        try {
            client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .build();
        } catch (Throwable t) {
            LOG.log(Level.FINE, "Không thể khởi tạo HttpClient cho Ollama", t);
        }
        this.httpClient = client;
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
        lastProbeTime.set(0L); // Bắt buộc kiểm tra lại
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String providerName() {
        return "Ollama Local (" + modelName + ")";
    }

    @Override
    public boolean isAvailable() {
        if (!enabled || httpClient == null) {
            return false;
        }

        long now = System.currentTimeMillis();
        if (now - lastProbeTime.get() < AVAILABILITY_CACHE_MS) {
            return cachedAvailable.get();
        }

        // Hết hạn cache -> kiểm tra nhanh socket
        boolean reachable = probeSocket();
        cachedAvailable.set(reachable);
        lastProbeTime.set(now);
        return reachable;
    }

    private boolean probeSocket() {
        try (Socket probe = new Socket()) {
            probe.connect(new InetSocketAddress(host, port), 200);
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
                + "Tên tệp: " + (file != null ? file.getName() : "unknown") + "\n"
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

        String json = resp.body();
        String marker = "\"response\"";
        int idx = json.indexOf(marker);
        if (idx < 0) {
            throw new IllegalStateException("Ollama không trả về trường response");
        }

        // [A6 Fix]: Kiểm tra an toàn cho colon và quote tránh StringIndexOutOfBoundsException
        int colon = json.indexOf(':', idx + marker.length());
        if (colon < 0) {
            throw new IllegalStateException("Cú pháp phản hồi Ollama JSON không hợp lệ (thiếu dấu hai chấm)");
        }
        int quote = json.indexOf('"', colon + 1);
        if (quote < 0) {
            throw new IllegalStateException("Cú pháp phản hồi Ollama JSON không hợp lệ (thiếu dấu nháy mở)");
        }

        return GeminiAIService.readUnescapedJsonString(json, quote + 1);
    }
}
