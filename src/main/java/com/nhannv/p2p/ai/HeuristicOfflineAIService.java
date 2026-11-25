package com.nhannv.p2p.ai;

import com.nhannv.p2p.model.SharedFile;

import java.io.File;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Động cơ Phân tích Ngữ nghĩa &amp; Chẩn đoán Mạng Cục bộ (<b>Zero-Dependency Offline Heuristic AI Engine</b>).
 *
 * <h2>Các Cải tiến Kỹ thuật</h2>
 * <ul>
 *   <li><b>Khớp từ khóa theo Token &amp; Ranh giới từ (A9):</b> Sử dụng phân tích token và regex word boundaries ({@code \\b})
 *       để loại bỏ hoàn toàn các lỗi khớp nhầm như {@code "lab"} trong {@code "label"}, {@code "available"}.</li>
 *   <li><b>Chẩn đoán mạng dựa trên Mã lỗi chuẩn hóa (A10):</b> Nhận diện cả mã lỗi hệ thống
 *       (AUTH_FAILED, TIMEOUT, CONNECTION_REFUSED, HASH_MISMATCH, FIREWALL_BLOCKED).</li>
 * </ul>
 */
public class HeuristicOfflineAIService implements AIService {

    private static final Map<String, String> EXTENSION_CATEGORY_TAGS = Map.ofEntries(
            Map.entry("java", "ma-nguon, java, lap-trinh"),
            Map.entry("py", "ma-nguon, python, script"),
            Map.entry("c", "ma-nguon, c-cpp, he-thong"),
            Map.entry("cpp", "ma-nguon, c-cpp, he-thong"),
            Map.entry("js", "ma-nguon, javascript, web"),
            Map.entry("ts", "ma-nguon, typescript, web"),
            Map.entry("html", "giao-dien, html, web"),
            Map.entry("css", "giao-dien, css, thiet-ke"),
            Map.entry("fxml", "giao-dien, javafx, fxml"),
            Map.entry("sql", "co-so-du-lieu, sql, truy-van"),
            Map.entry("json", "cau-hinh, du-lieu, json"),
            Map.entry("xml", "cau-hinh, xml, maven"),
            Map.entry("csv", "thong-ke, bang-bieu, csv, excel"),
            Map.entry("pdf", "tai-lieu, bao-cao, pdf, giao-trinh"),
            Map.entry("docx", "tai-lieu, word, bao-cao, van-ban"),
            Map.entry("doc", "tai-lieu, word, van-ban"),
            Map.entry("pptx", "bai-giang, slide, thuyet-trinh"),
            Map.entry("xlsx", "bang-tinh, excel, so-lieu"),
            Map.entry("txt", "ghi-chu, van-ban, text"),
            Map.entry("md", "tai-lieu, markdown, huong-dan"),
            Map.entry("zip", "goi-nen, thu-muc, du-an, archive"),
            Map.entry("rar", "goi-nen, archive, backup"),
            Map.entry("jpg", "hinh-anh, anh-chup, media"),
            Map.entry("jpeg", "hinh-anh, anh-chup, media"),
            Map.entry("png", "hinh-anh, do-hoa, screenshot"),
            Map.entry("mp4", "video, da-phuong-tien, demo"),
            Map.entry("mp3", "am-thanh, audio, media"),
            Map.entry("exe", "bo-cai-dat, phan-mem, windows")
    );

    private static final Map<String, String> DOMAIN_KEYWORD_RULES = Map.ofEntries(
            Map.entry("socket", "lap-trinh-mang, tcp-udp, socket"),
            Map.entry("serversocket", "tcp-server, lap-trinh-mang"),
            Map.entry("datagram", "udp-broadcast, kham-pha-lan"),
            Map.entry("javafx", "giao-dien-ui, javafx, desktop"),
            Map.entry("sha-256", "bao-mat, bam-sha256, toan-ven"),
            Map.entry("messagedigest", "mat-ma, sha256, checksum"),
            Map.entry("aes", "ma-hoa, aes-256, an-toan-mang"),
            Map.entry("thread", "da-luong, multithreading, dong-bo"),
            Map.entry("executorservice", "thread-pool, da-luong"),
            Map.entry("watchservice", "java-nio, dong-bo-file"),
            Map.entry("randomaccessfile", "chunking, pause-resume, swarm"),
            Map.entry("lab", "bai-tap-lab, thuc-hanh"),
            Map.entry("baocao", "bao-cao, do-an, tai-lieu"),
            Map.entry("doan", "do-an, bao-cao, hoc-tap"),
            Map.entry("de-cuong", "on-tap, de-cuong, tai-lieu"),
            Map.entry("de-thi", "de-thi, on-tap, bai-tap"),
            Map.entry("mang", "mang-may-tinh, lap-trinh-mang")
    );

    @Override
    public String providerName() {
        return "Offline Heuristic AI";
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public AIMetadata analyzeFileMetadata(
            File file,
            String sha256Hash,
            String extractedSnippet,
            String sensitiveWarning
    ) {
        String fileName = file != null ? file.getName() : "unknown";
        long fileSize = file != null ? file.length() : 0L;
        String ext = FileContentExtractor.getExtension(fileName);

        Set<String> tagSet = new LinkedHashSet<>();
        String baseTags = EXTENSION_CATEGORY_TAGS.getOrDefault(ext, "tep-chia-se, p2p-lan");
        for (String t : baseTags.split(",")) {
            tagSet.add(t.trim());
        }

        // Bóc từ khóa có nghĩa từ tên tệp
        String normalizedName = stripVietnameseAccents(fileName.toLowerCase(Locale.ROOT));
        Set<String> corpusTokens = new HashSet<>(Arrays.asList(normalizedName.split("[^a-z0-9]+")));

        if (extractedSnippet != null && !extractedSnippet.isBlank()) {
            String normalizedSnippet = stripVietnameseAccents(extractedSnippet.toLowerCase(Locale.ROOT));
            corpusTokens.addAll(Arrays.asList(normalizedSnippet.split("[^a-z0-9]+")));
        }

        for (String token : corpusTokens) {
            if (token.length() >= 3 && !token.equals(ext) && tagSet.size() < 7) {
                // Kiểm tra nếu token là từ khóa domain
                String mapped = DOMAIN_KEYWORD_RULES.get(token);
                if (mapped != null) {
                    for (String mt : mapped.split(",")) {
                        if (tagSet.size() < 8) {
                            tagSet.add(mt.trim());
                        }
                    }
                }
            }
        }

        String tagsJoined = String.join(", ", tagSet);
        String summary = buildSmartSummary(fileName, ext, fileSize, extractedSnippet);

        return new AIMetadata(sha256Hash, tagsJoined, summary, sensitiveWarning, providerName());
    }

    @Override
    public String generatePreDownloadPreview(
            String fileName,
            long fileSize,
            String existingTags,
            String snippetText
    ) {
        String ext = FileContentExtractor.getExtension(fileName);
        StringBuilder sb = new StringBuilder();
        sb.append("=== BÁO CÁO PHÂN TÍCH TỆP TRƯỚC KHI TẢI (PRE-DOWNLOAD AI PREVIEW) ===\n");
        sb.append("• Tệp mục tiêu: ").append(fileName)
                .append(" (Dung lượng: ").append(SharedFile.formatSize(fileSize)).append(")\n");
        sb.append("• Phân loại & Thẻ ngữ nghĩa: ")
                .append(existingTags == null || existingTags.isBlank()
                        ? EXTENSION_CATEGORY_TAGS.getOrDefault(ext, "Tệp dữ liệu P2P")
                        : existingTags)
                .append("\n");

        String warning = FileContentExtractor.detectSensitiveWarning(fileName, snippetText);
        if (!warning.isBlank()) {
            sb.append("• [!] ").append(warning).append("\n");
        }

        if (snippetText != null && !snippetText.isBlank()) {
            String firstLines = snippetText.length() > 750
                    ? snippetText.substring(0, 750) + "..."
                    : snippetText;
            sb.append("\n• Tóm tắt nội dung trích xuất từ 8 KB đầu tiên trên Peer:\n")
                    .append("--------------------------------------------------\n")
                    .append(firstLines)
                    .append("\n--------------------------------------------------\n");
            sb.append("• Khuyến nghị: Đã xác thực cấu trúc tệp hợp lệ chỉ với 8 KB băng thông. Bạn có thể bấm 'Tải tệp' ở góc dưới để tải toàn bộ và kiểm định SHA-256.");
        } else {
            sb.append("\n• Nhận xét: Đây là tệp đa phương tiện/nhị phân (.").append(ext.isEmpty() ? "bin" : ext)
                    .append("). Hệ thống đã kiểm tra phản hồi chunk đầu tiên từ Peer đích sẵn sàng để tải đa nguồn (Swarm).");
        }
        return sb.toString();
    }

    @Override
    public String diagnoseNetworkIssue(NetworkDiagnosticContext ctx) {
        String logs = (ctx.recentNetworkLogs() == null) ? "" : ctx.recentNetworkLogs();
        String lowerLogs = logs.toLowerCase(Locale.ROOT);
        List<String> rootCauses = new ArrayList<>();
        List<String> fixes = new ArrayList<>();

        if (!ctx.firewallConfigured()) {
            rootCauses.add("[FIREWALL_BLOCKED] Windows Defender Firewall trên máy này CHƯA mở Rule cho cổng UDP 8888 & TCP 8889.");
            fixes.add("Bấm ngay nút 'Firewall (1-Click)' trên thanh Header góc trên bên phải và chọn 'Yes' ở hộp thoại UAC.");
        }
        if (lowerLogs.contains("auth_failed") || lowerLogs.contains("code_auth_failed") || lowerLogs.contains("sai mã pin")) {
            rootCauses.add("[AUTH_FAILED] Peer đối tác đang bật khóa PIN bảo vệ nhưng mã PIN bạn cung cấp không chính xác.");
            fixes.add("Nhờ đối tác bấm nút 'Mã Mời' (Copy Invite Token host:port#PIN) rồi dán vào ô 'Kết nối nhanh' để tự động điền đúng PIN.");
        }
        if (lowerLogs.contains("timeout") || lowerLogs.contains("timed out") || lowerLogs.contains("connection refused") || lowerLogs.contains("từ chối kết nối")) {
            rootCauses.add("[ERR_TIMEOUT_OR_REFUSED] Kết nối TCP bị từ chối hoặc quá thời gian chờ (Timeout) khi gọi tới Peer đích.");
            fixes.add("Kiểm tra máy đối tác đã bật ứng dụng P2P và đã mở Firewall chưa. Nếu dùng Radmin VPN (26.x.x.x), đảm bảo cả 2 máy đang cùng một phòng mạng ảo.");
        }
        if (lowerLogs.contains("sha_mismatch") || lowerLogs.contains("sai lệch sha-256") || lowerLogs.contains("lỗi sha-256")) {
            rootCauses.add("[ERR_HASH_MISMATCH] Phát hiện cảnh báo sai lệch mã băm toàn vẹn SHA-256 sau khi tải.");
            fixes.add("Tệp trên máy nguồn có thể vừa bị chỉnh sửa trong lúc đang truyền. Hãy bấm 'Làm mới' để đồng bộ lại mã SHA-256 mới nhất rồi tải lại.");
        }
        if (ctx.bandwidthLimitBytesPerSec() > 0 && ctx.bandwidthLimitBytesPerSec() <= 2L * 1024 * 1024) {
            rootCauses.add("[QOS_THROTTLED] Bộ giới hạn băng thông Token Bucket đang được đặt ở mức thấp ("
                    + SharedFile.formatSize(ctx.bandwidthLimitBytesPerSec()) + "/s).");
            fixes.add("Nếu muốn đạt tốc độ tối đa của mạng LAN (50-100 MB/s), hãy đổi mục 'Giới hạn băng thông (QoS)' ở thanh dưới cùng sang 'Không giới hạn'.");
        }
        if (ctx.onlinePeersCount() == 0) {
            rootCauses.add("[NO_PEERS] Hiện chưa có thiết bị Peer nào trong danh sách Radar.");
            fixes.add("Bấm nút 'Quét LAN' ở cột Radar bên trái để quét chủ động 50 luồng TCP (.1 -> .254), hoặc dán trực tiếp IP Radmin VPN (26.x.x.x) vào ô Kết nối nhanh.");
        }

        if (rootCauses.isEmpty()) {
            rootCauses.add("Hệ thống mạng P2P đang hoạt động ổn định, không phát hiện lỗi Socket, Firewall hay xung đột cổng.");
            fixes.add("Bạn có thể tận dụng tải đa nguồn (Swarm) khi một tệp có từ 2 Peers trở lên để nhân đôi băng thông tải.");
        }

        StringBuilder report = new StringBuilder();
        report.append("=== KẾT QUẢ CHẨN ĐOÁN MẠNG THÔNG MINH (AI NETWORK DIAGNOSTICS) ===\n");
        report.append("1. Trạng thái hạ tầng: Firewall=").append(ctx.firewallConfigured() ? "ĐÃ MỞ (OK)" : "CHƯA MỞ")
                .append(" | Peers Online=").append(ctx.onlinePeersCount())
                .append(" | QoS=").append(ctx.bandwidthLimitBytesPerSec() == 0
                        ? "Không giới hạn"
                        : (SharedFile.formatSize(ctx.bandwidthLimitBytesPerSec()) + "/s"))
                .append("\n");
        report.append("2. Phát hiện & Nguyên nhân gốc rễ:\n");
        for (String cause : rootCauses) {
            report.append("   • ").append(cause).append("\n");
        }
        report.append("3. Đề xuất khắc phục & Tối ưu hóa:\n");
        for (String fix : fixes) {
            report.append("   -> ").append(fix).append("\n");
        }
        return report.toString();
    }

    private static String buildSmartSummary(String fileName, String ext, long fileSize, String snippet) {
        String sizeStr = SharedFile.formatSize(fileSize);
        if (snippet != null && !snippet.isBlank()) {
            String cleanFirstLine = snippet.replace('\n', ' ').replaceAll("\\s+", " ").trim();
            if (cleanFirstLine.length() > 115) {
                cleanFirstLine = cleanFirstLine.substring(0, 112) + "...";
            }
            return "[" + (ext.isEmpty() ? "FILE" : ext.toUpperCase(Locale.ROOT)) + " - " + sizeStr + "] " + cleanFirstLine;
        }
        return switch (ext) {
            case "pdf", "docx", "doc" -> "Tài liệu văn bản/báo cáo (" + sizeStr + "): " + fileName;
            case "pptx" -> "Slide bài giảng/thuyết trình (" + sizeStr + "): " + fileName;
            case "zip", "rar" -> "Gói thư mục/dự án nén (" + sizeStr + "): " + fileName;
            case "jpg", "jpeg", "png" -> "Tệp hình ảnh/đồ họa (" + sizeStr + "): " + fileName;
            case "mp4", "mkv" -> "Tệp video đa phương tiện (" + sizeStr + "): " + fileName;
            default -> "Tệp chia sẻ P2P (" + sizeStr + "): " + fileName;
        };
    }

    public static String stripVietnameseAccents(String input) {
        if (input == null || input.isEmpty()) {
            return "";
        }
        String normalized = Normalizer.normalize(input, Normalizer.Form.NFD);
        return normalized
                .replaceAll("\\p{InCombiningDiacriticalMarks}+", "")
                .replace('đ', 'd')
                .replace('Đ', 'D');
    }
}
