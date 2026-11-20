package com.nhannv.p2p.ai;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Chiến lược AI đám mây kết nối trực tiếp tới <b>Google Gemini REST API</b>
 * bằng {@link HttpClient} chuẩn của Java 17+ (không cần thư viện ngoài).
 */
public class GeminiAIService implements AIService {

    private static final String DEFAULT_MODEL = "gemini-2.0-flash";
    private static final String ENDPOINT_TEMPLATE =
            "https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s";

    private final HttpClient httpClient;
    private volatile String apiKey;
    private volatile String modelName = DEFAULT_MODEL;

    public GeminiAIService() {
        this(resolveInitialApiKey());
    }

    public GeminiAIService(String apiKey) {
        this.apiKey = (apiKey == null) ? "" : apiKey.trim();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(4))
                .build();
    }

    public void setApiKey(String apiKey) {
        this.apiKey = (apiKey == null) ? "" : apiKey.trim();
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setModelName(String modelName) {
        if (modelName != null && !modelName.isBlank()) {
            this.modelName = modelName.trim();
        }
    }

    @Override
    public String providerName() {
        return "Gemini Cloud (" + modelName + ")";
    }

    @Override
    public boolean isAvailable() {
        return apiKey != null && !apiKey.isBlank();
    }

    @Override
    public AIMetadata analyzeFileMetadata(
            File file,
            String sha256Hash,
            String extractedSnippet,
            String sensitiveWarning
    ) throws Exception {
        if (!isAvailable()) {
            throw new IllegalStateException("Chưa cấu hình GEMINI_API_KEY");
        }

        String prompt = "Bạn là trợ lý phân loại tệp kỹ thuật trong mạng P2P LAN. "
                + "Hãy phân tích tệp sau và trả về ĐÚNG 2 dòng theo định dạng:\n"
                + "TAGS: <3 đến 5 từ khóa tiếng Việt không dấu/có dấu ngắn gọn, cách nhau bởi dấu phẩy>\n"
                + "SUMMARY: <1 câu tóm tắt nội dung tệp bằng tiếng Việt dưới 140 ký tự>\n\n"
                + "Tên tệp: " + file.getName() + " (" + file.length() + " bytes)\n"
                + "Trích đoạn nội dung đầu tệp:\n"
                + (extractedSnippet == null || extractedSnippet.isBlank() ? "(Tệp nhị phân/media)" : extractedSnippet);

        String responseText = callGeminiGenerateContent(prompt);
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
            throw new IllegalStateException("Chưa cấu hình GEMINI_API_KEY");
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

        return callGeminiGenerateContent(prompt);
    }

    @Override
    public String diagnoseNetworkIssue(NetworkDiagnosticContext ctx) throws Exception {
        if (!isAvailable()) {
            throw new IllegalStateException("Chưa cấu hình GEMINI_API_KEY");
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

        return callGeminiGenerateContent(prompt);
    }

    private String callGeminiGenerateContent(String prompt) throws Exception {
        String url = String.format(ENDPOINT_TEMPLATE, modelName, apiKey);
        String requestJson = "{\"contents\":[{\"parts\":[{\"text\":" + toJsonString(prompt) + "}]}],"
                + "\"generationConfig\":{\"temperature\":0.2,\"maxOutputTokens\":350}}";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(7))
                .header("Content-Type", "application/json; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(requestJson, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("Gemini API trả về HTTP " + response.statusCode());
        }
        String extracted = extractFirstTextFieldFromJson(response.body());
        if (extracted.isBlank()) {
            throw new IllegalStateException("Phản hồi Gemini API rỗng");
        }
        return extracted;
    }

    static AIMetadata parseTagsAndSummary(
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
                if (trimmed.toUpperCase().startsWith("TAGS:")) {
                    tags = trimmed.substring(5).trim();
                } else if (trimmed.toUpperCase().startsWith("SUMMARY:")) {
                    summary = trimmed.substring(8).trim();
                }
            }
            if (summary.isBlank()) {
                summary = rawOutput.replace('\n', ' ').trim();
            }
        }
        return new AIMetadata(sha256Hash, tags, summary, sensitiveWarning, provider);
    }

    static String toJsonString(String raw) {
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
                default -> {
                    if (c >= 32) {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    static String extractFirstTextFieldFromJson(String json) {
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

    static String readUnescapedJsonString(String json, int startIdx) {
        StringBuilder sb = new StringBuilder();
        boolean escaping = false;
        for (int i = startIdx; i < json.length(); i++) {
            char c = json.charAt(i);
            if (escaping) {
                switch (c) {
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
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
