package com.nhannv.p2p.ai;

import java.util.Objects;

/**
 * Mô hình siêu dữ liệu thông minh (AI Metadata) gắn với một tệp chia sẻ theo mã băm {@code sha256Hash}
 * (Phase 1 — Core AI Engine &amp; SHA-256 Content-Addressed Cache).
 *
 * @param sha256Hash       mã băm SHA-256 (64 ký tự hex) làm khóa định danh duy nhất cho nội dung tệp
 * @param tags             chuỗi các thẻ ngữ nghĩa phân tách bằng dấu phẩy (vd: {@code "java-nio, socket, bao-cao"})
 * @param summary          mô tả tóm tắt ngắn gọn (1-2 câu) về nội dung tệp
 * @param sensitiveWarning cảnh báo nếu phát hiện nguy cơ lộ khóa bí mật/mật khẩu trong tệp (rỗng nếu an toàn)
 * @param provider         tên engine đã sinh metadata này ({@code "Gemini Cloud"}, {@code "Ollama Local"}, {@code "Local Heuristic"})
 */
public record AIMetadata(
        String sha256Hash,
        String tags,
        String summary,
        String sensitiveWarning,
        String provider
) {

    public AIMetadata {
        Objects.requireNonNull(sha256Hash, "sha256Hash");
        tags = sanitizeField(tags, 90);
        summary = sanitizeField(summary, 180);
        sensitiveWarning = sanitizeField(sensitiveWarning, 140);
        provider = (provider == null || provider.isBlank()) ? "Local Heuristic" : provider.trim();
    }

    public static AIMetadata empty(String sha256Hash) {
        return new AIMetadata(sha256Hash == null ? "" : sha256Hash, "", "", "", "None");
    }

    public boolean hasContent() {
        return !tags.isBlank() || !summary.isBlank() || !sensitiveWarning.isBlank();
    }

    public boolean hasSensitiveWarning() {
        return !sensitiveWarning.isBlank();
    }

    /**
     * Làm sạch chuỗi để truyền an toàn qua giao thức TCP {@code LIST_FILES} (loại bỏ ký tự {@code '|'} và xuống dòng).
     */
    public static String sanitizeField(String raw, int maxLen) {
        if (raw == null || raw.isBlank()) {
            return "";
        }
        String cleaned = raw
                .replace('|', ',')
                .replace('\r', ' ')
                .replace('\n', ' ')
                .replaceAll("\\s+", " ")
                .trim();
        if (cleaned.length() > maxLen) {
            return cleaned.substring(0, maxLen - 3).trim() + "...";
        }
        return cleaned;
    }
}
