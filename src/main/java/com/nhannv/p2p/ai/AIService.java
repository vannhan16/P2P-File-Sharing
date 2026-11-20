package com.nhannv.p2p.ai;

import java.io.File;

/**
 * Giao diện Chiến lược (Strategy Pattern) cho các nhà cung cấp dịch vụ Trí tuệ Nhân tạo trong hệ thống P2P.
 *
 * <h2>Thứ tự ưu tiên chuyển đổi tự động (Graceful Degradation Chain)</h2>
 * <ol>
 *   <li>{@link GeminiAIService}: Khi máy có kết nối Internet và được cấu hình {@code GEMINI_API_KEY}.</li>
 *   <li>{@link OllamaAIService}: Khi chạy trong mạng LAN Offline nhưng máy có bật Local LLM ({@code localhost:11434}).</li>
 *   <li>{@link HeuristicOfflineAIService}: Luôn sẵn sàng 100% Offline trên mọi máy mà không cần cấu hình,
 *       đảm bảo tính năng gắn thẻ, tóm tắt nhanh và chẩn đoán mạng không bao giờ bị lỗi.</li>
 * </ol>
 */
public interface AIService {

    /** Tên hiển thị của nhà cung cấp AI (vd: {@code "Gemini Cloud"}, {@code "Ollama Local"}, {@code "Offline Heuristic"}). */
    String providerName();

    /** Kiểm tra nhanh xem nhà cung cấp AI này hiện có sẵn sàng phục vụ hay không. */
    boolean isAvailable();

    /**
     * Phân tích tên tệp và đoạn trích nội dung (4 KB đầu) để sinh {@link AIMetadata} (gồm {@code tags} và {@code summary}).
     */
    AIMetadata analyzeFileMetadata(
            File file,
            String sha256Hash,
            String extractedSnippet,
            String sensitiveWarning
    ) throws Exception;

    /**
     * Tóm tắt chuyên sâu nội dung tệp từ đoạn trích 8 KB đầu tiên (Pre-download AI Preview)
     * giúp người dùng biết rõ tệp của Peer khác chứa gì trước khi quyết định tải toàn bộ tệp.
     */
    String generatePreDownloadPreview(
            String fileName,
            long fileSize,
            String existingTags,
            String snippetText
    ) throws Exception;

    /**
     * Đọc nhật ký lỗi mạng thực tế, trạng thái Firewall, độ trễ RTT và cấu hình QoS để chẩn đoán
     * nguyên nhân gốc rễ và đề xuất các bước khắc phục cụ thể bằng tiếng Việt.
     */
    String diagnoseNetworkIssue(NetworkDiagnosticContext context) throws Exception;
}
