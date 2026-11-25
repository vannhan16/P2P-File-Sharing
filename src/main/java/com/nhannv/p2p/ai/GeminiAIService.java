package com.nhannv.p2p.ai;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Chiến lược AI đám mây kết nối trực tiếp tới <b>Google Gemini REST API</b>
 * bằng {@link HttpClient} chuẩn của Java 17+.
 *
 * <h2>Các Cải tiến Bảo mật &amp; Ổn định</h2>
 * <ul>
 *   <li><b>Bảo vệ API Key (S7):</b> Truyền qua Header {@code x-goog-api-key} thay vì URL query string.</li>
 *   <li><b>Xử lý Unicode &amp; Ký tự điều khiển (A4, A5):</b> {@link #toJsonString} mã hóa Unicode escape {@code \\u00XX},
 *       {@link #readUnescapedJsonString} giải mã đầy đủ các ký tự Unicode {@code \\uXXXX}.</li>
 *   <li><b>Hạn mức Token linh hoạt (A8):</b> Metadata = 350, Preview = 1024, Chẩn đoán mạng = 2048 tokens.</li>
 *   <li><b>Circuit Breaker &amp; HTTP 429 (P2-8):</b> Tự động ngắt mạch 90 giây khi gặp lỗi liên tiếp hoặc bị rate limit.</li>
 * </ul>
 */
public class GeminiAIService implements AIService {

    private static final Logger LOG = Logger.getLogger(GeminiAIService.class.getName());

    public static final String DEFAULT_MODEL = "gemini-2.0-flash";
    private static final String BASE_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent";

    private final HttpClient httpClient;
    private volatile String apiKey;
    private volatile String modelName = DEFAULT_MODEL;

    // Circuit Breaker state
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong circuitOpenUntil = new AtomicLong(0L);

    public GeminiAIService() {
        this(resolveInitialApiKey());
    }

    public GeminiAIService(String apiKey) {
        this.apiKey = (apiKey == null) ? "" : apiKey.trim();
        HttpClient client = null;
        try {
            client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(4))
                    .build();
        } catch (Throwable t) {
            LOG.log(Level.FINE, "Không thể khởi tạo HttpClient cho Gemini API", t);
        }
        this.httpClient = client;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = (apiKey == null) ? "" : apiKey.trim();
        resetCircuitBreaker();
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setModelName(String modelName) {
        if (modelName != null && !modelName.isBlank()) {
            this.modelName = modelName.trim();
        }
    }

    public String getModelName() {
        return modelName;
    }

    public void resetCircuitBreaker() {
        consecutiveFailures.set(0);
        circuitOpenUntil.set(0L);
    }

    @Override
    public String providerName() {
        return "Gemini Cloud (" + modelName + ")";
    }

    @Override
    public boolean isAvailable() {
        if (httpClient == null || apiKey == null || apiKey.isBlank()) {
            return false;
        }
        // Circuit breaker check
        long now = System.currentTimeMillis();
        return now >= circuitOpenUntil.get();
    }

    @Override
    public AIMetadata analyzeFileMetadata(
            File file,
            String sha256Hash,
            String extractedSnippet,
            String sensitiveWarning
    ) throws Exception {
        if (!isAvailable()) {
            throw new IllegalStateException("Gemini AI không sẵn sàng (Chưa có API key hoặc Circuit Breaker đang ngắt)");
        }

        String prompt = "Bạn là trợ lý phân loại tệp kỹ thuật trong mạng P2P LAN. "
                + "Hãy phân tích tệp sau và trả về ĐÚNG 2 dòng theo định dạng:\n"
                + "TAGS: <3 đến 5 từ khóa tiếng Việt không dấu/có dấu ngắn gọn, cách nhau bởi dấu phẩy>\n"
                + "SUMMARY: <1 câu tóm tắt nội dung tệp bằng tiếng Việt dưới 140 ký tự>\n\n"
                + "Tên tệp: " + (file != null ? file.getName() : "unknown") + "\n"
                + "Trích đoạn nội dung đầu tệp:\n"
                + (extractedSnippet == null || extractedSnippet.isBlank() ? "(Tệp nhị phân/media)" : extractedSnippet);

        String responseText = callGeminiGenerateContent(prompt, 350);
        return parseTagsAndSummary(sha256Hash, responseText, sensitiveWarning, providerName());
    }

    @Override
    public String generatePreDownloadPreview(
            String fileName,
            long fileSize,
            String existingTags,
            String snippetText
    ) throws Exception {
        if (!isAvailable()) {
            throw new IllegalStateException("Gemini AI không sẵn sàng");
        }

        String prompt = "Bạn là Kỹ sư Phân tích Tệp P2P. Người dùng đang xem xét có nên tải tệp \""
                + fileName + "\" (" + fileSize + " bytes) từ một Peer khác hay không.\n"
                + "Thẻ ngữ nghĩa hiện có: " + (existingTags == null ? "" : existingTags) + "\n"
                + "Nội dung 8 KB đầu tiên trích xuất từ Peer:\n"
                + (snippetText == null || snippetText.isBlank() ? "(Không có văn bản thuần trong 8 KB đầu)" : snippetText)
                + "\n\nHãy tóm tắt ngắn gọn bằng tiếng Việt (3 gạch đầu dòng rõ ràng):\n"
                + "1. Nội dung chính của tệp là gì?\n"
                + "2. Các thành phần/hàm/mục quan trọng phát hiện được?\n"
                + "3. Đánh giá độ tin cậy và khuyến nghị tải.";

        return callGeminiGenerateContent(prompt, 1024);
    }

    @Override
    public String diagnoseNetworkIssue(NetworkDiagnosticContext ctx) throws Exception {
        if (!isAvailable()) {
            throw new IllegalStateException("Gemini AI không sẵn sàng");
        }

        String prompt = "Bạn là Senior Java Network Engineer chuyên chẩn đoán hệ thống P2P LAN / Radmin VPN / TCP Socket.\n"
                + "Dựa trên dữ liệu thực tế dưới đây, hãy chẩn đoán bằng tiếng Việt theo 3 mục ngắn gọn "
                + "(1. Nguyên nhân gốc rễ, 2. Các bước khắc phục ngay trên UI, 3. Tối ưu băng thông Swarm/QoS):\n"
                + "- Windows Firewall Rule (UDP 8888 / TCP 8889): " + (ctx.firewallConfigured() ? "ĐÃ MỞ" : "CHƯA MỞ") + "\n"
                + "- Số Peers Online: " + ctx.onlinePeersCount() + " (" + ctx.peerLatencySummary() + ")\n"
                + "- Giới hạn QoS hiện tại: " + ctx.bandwidthLimitBytesPerSec() + " B/s\n"
                + "- Card mạng phát hiện: " + ctx.detectedSubnetsSummary() + "\n"
                + "- Lịch sử truyền tải: " + ctx.transferHistorySummary() + "\n"
                + "- Nhật ký sự kiện mạng gần nhất:\n" + ctx.recentNetworkLogs();

        return callGeminiGenerateContent(prompt, 2048);
    }

    /**
     * Gửi yêu cầu generateContent tới Gemini API kèm kiểm soát Circuit Breaker và Header x-goog-api-key an toàn.
     */
    public String callGeminiGenerateContent(String prompt, int maxTokens) throws Exception {
        long now = System.currentTimeMillis();
        if (now < circuitOpenUntil.get()) {
            throw new IllegalStateException("Circuit Breaker đang mở: tạm dừng gọi Gemini Cloud do gặp lỗi liên tiếp!");
        }

        String url = String.format(Locale.ROOT, BASE_ENDPOINT, modelName);
        String requestJson = "{\"contents\":[{\"parts\":[{\"text\":" + toJsonString(prompt) + "}]}],"
                + "\"generationConfig\":{\"temperature\":0.2,\"maxOutputTokens\":" + maxTokens + "}}";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(8))
                .header("Content-Type", "application/json; charset=UTF-8")
                .header("x-goog-api-key", apiKey) // [S7 Fix]: Dùng header thay cho query parameter ?key=
                .POST(HttpRequest.BodyPublishers.ofString(requestJson, StandardCharsets.UTF_8))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int code = response.statusCode();

            if (code == 429) { // [P2-8 Fix]: Rate Limit
                long backoffMs = parseRetryAfter(response.headers()).orElse(60_000L);
                circuitOpenUntil.set(System.currentTimeMillis() + backoffMs);
                consecutiveFailures.incrementAndGet();
                throw new IllegalStateException("Gemini API bị Rate Limit (HTTP 429). Tạm dừng " + (backoffMs / 1000) + "s.");
            }

            if (code < 200 || code >= 300) {
                recordFailure();
                throw new IllegalStateException("Gemini API trả về HTTP " + code);
            }

            String extracted = extractFirstTextFieldFromJson(response.body());
            if (extracted.isBlank()) {
                recordFailure();
                throw new IllegalStateException("Phản hồi Gemini API rỗng");
            }

            // Gọi thành công: reset circuit breaker
            consecutiveFailures.set(0);
            return extracted;
        } catch (Exception e) {
            recordFailure();
            throw e;
        }
    }

    private void recordFailure() {
        int fails = consecutiveFailures.incrementAndGet();
        if (fails >= 3) { // 3 lỗi liên tiếp -> ngắt mạch 90 giây
            circuitOpenUntil.set(System.currentTimeMillis() + 90_000L);
            LOG.warning("Gemini AI gặp 3 lỗi liên tiếp -> Circuit Breaker đã kích hoạt (Ngắt 90s, tự chuyển sang Ollama/Heuristic)");
        }
    }

    private Optional<Long> parseRetryAfter(HttpHeaders headers) {
        return headers.firstValue("Retry-After").flatMap(val -> {
            try {
                return Optional.of(Long.parseLong(val.trim()) * 1000L);
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        });
    }

    /**
     * [A7 Fix]: parseTagsAndSummary dùng toUpperCase(Locale.ROOT).
     */
    public static AIMetadata parseTagsAndSummary(
            String sha256Hash,
            String rawOutput,
            String sensitiveWarning,
            String provider
    ) {
        String tags = "";
        String summary = "";
        if (rawOutput != null) {
            for (String line : rawOutput.split("\r?\n")) {
                String trimmed = line.trim();
                if (trimmed.toUpperCase(Locale.ROOT).startsWith("TAGS:")) {
                    tags = trimmed.substring(5).trim();
                } else if (trimmed.toUpperCase(Locale.ROOT).startsWith("SUMMARY:")) {
                    summary = trimmed.substring(8).trim();
                }
            }
            if (summary.isBlank()) {
                summary = rawOutput.replace('\n', ' ').trim();
            }
        }
        return new AIMetadata(sha256Hash, tags, summary, sensitiveWarning, provider);
    }

    /**
     * [A4 Fix]: Mã hóa đầy đủ ký tự điều khiển thành Unicode escape {@code \\u00XX} thay vì bỏ qua.
     */
    public static String toJsonString(String raw) {
        if (raw == null) {
            return "\"\"";
        }
        StringBuilder sb = new StringBuilder(raw.length() + 16);
        sb.append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 32 || (c >= 0x7F && c <= 0x9F)) {
                        sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    public static String extractFirstTextFieldFromJson(String json) {
        if (json == null) {
            return "";
        }
        String marker = "\"text\"";
        int idx = json.indexOf(marker);
        if (idx < 0) {
            return "";
        }
        int colon = json.indexOf(':', idx + marker.length());
        if (colon < 0) {
            return "";
        }
        int firstQuote = json.indexOf('"', colon + 1);
        if (firstQuote < 0) {
            return "";
        }
        return readUnescapedJsonString(json, firstQuote + 1);
    }

    /**
     * [A5 Fix]: Giải mã chuỗi JSON bao gồm cả các ký tự Unicode {@code \\uXXXX}.
     */
    public static String readUnescapedJsonString(String json, int startIdx) {
        StringBuilder sb = new StringBuilder();
        boolean escaping = false;
        for (int i = startIdx; i < json.length(); i++) {
            char c = json.charAt(i);
            if (escaping) {
                switch (c) {
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'u' -> {
                        if (i + 4 < json.length()) {
                            String hex = json.substring(i + 1, i + 5);
                            try {
                                int codePoint = Integer.parseInt(hex, 16);
                                sb.append((char) codePoint);
                                i += 4;
                            } catch (NumberFormatException e) {
                                sb.append('u');
                            }
                        } else {
                            sb.append('u');
                        }
                    }
                    default -> sb.append(c);
                }
                escaping = false;
            } else if (c == '\\') {
                escaping = true;
            } else if (c == '"') {
                break;
            } else {
                sb.append(c);
            }
        }
        return sb.toString().trim();
    }

    private static String resolveInitialApiKey() {
        String env = System.getenv("GEMINI_API_KEY");
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        String prop = System.getProperty("GEMINI_API_KEY", "");
        return prop.trim();
    }
}
