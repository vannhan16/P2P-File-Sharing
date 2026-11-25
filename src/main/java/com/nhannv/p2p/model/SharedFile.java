package com.nhannv.p2p.model;

import com.nhannv.p2p.ai.AIMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Mô hình dữ liệu biểu diễn một tệp đang được chia sẻ trong mạng P2P LAN / Internet Tunnel.
 * <p>
 * Ở <b>Phase 2 (AI Integration)</b>, {@code SharedFile} được mở rộng thêm 3 thuộc tính siêu dữ liệu
 * ngữ nghĩa ({@code aiTags}, {@code aiSummary}, {@code sensitiveWarning}) và nâng cấp giao thức
 * đóng gói {@link #toWireLine()} / {@link #fromWireLine(String, String)} tương thích ngược 100%
 * với các Peer phiên bản cũ.
 *
 * @param fileName         tên tệp (vd: {@code "BaoCao_DoAn.pdf"})
 * @param fileSize         kích thước tệp tính bằng byte
 * @param sha256Hash       chuỗi hex SHA-256 (64 ký tự) của tệp
 * @param ownerIp          địa chỉ IPv4 hoặc {@code host:port} chính của Peer sở hữu tệp
 * @param ownerName        tên máy (hostname) của Peer sở hữu tệp (hoặc danh sách tên máy gộp)
 * @param peerIps          danh sách toàn bộ địa chỉ {@code host:port} các Peer đang giữ tệp có cùng mã SHA-256
 * @param aiTags           các thẻ ngữ nghĩa do AI sinh (vd: {@code "lap-trinh-mang, tcp-socket, java"})
 * @param aiSummary        mô tả tóm tắt ngắn gọn nội dung tệp (1-2 câu)
 * @param sensitiveWarning cảnh báo nếu phát hiện nguy cơ rò rỉ khóa bí mật/mật khẩu trong tệp
 */
public record SharedFile(
        String fileName,
        long fileSize,
        String sha256Hash,
        String ownerIp,
        String ownerName,
        List<String> peerIps,
        String aiTags,
        String aiSummary,
        String sensitiveWarning
) {

    private static final String WIRE_SEPARATOR = "|";

    public SharedFile {
        Objects.requireNonNull(fileName, "fileName");
        Objects.requireNonNull(sha256Hash, "sha256Hash");
        Objects.requireNonNull(ownerIp, "ownerIp");
        Objects.requireNonNull(ownerName, "ownerName");
        if (fileSize < 0) {
            throw new IllegalArgumentException("fileSize phải >= 0");
        }
        if (peerIps == null || peerIps.isEmpty()) {
            peerIps = List.of(ownerIp);
        } else {
            peerIps = Collections.unmodifiableList(new ArrayList<>(new LinkedHashSet<>(peerIps)));
        }
        aiTags = AIMetadata.sanitizeField(aiTags, 90);
        aiSummary = AIMetadata.sanitizeField(aiSummary, 180);
        sensitiveWarning = AIMetadata.sanitizeField(sensitiveWarning, 140);
    }

    /** Constructor 5 tham số tương thích ngược với Milestone 3. */
    public SharedFile(
            String fileName,
            long fileSize,
            String sha256Hash,
            String ownerIp,
            String ownerName
    ) {
        this(fileName, fileSize, sha256Hash, ownerIp, ownerName, List.of(ownerIp), "", "", "");
    }

    /** Constructor 6 tham số tương thích ngược với Milestone 4-6. */
    public SharedFile(
            String fileName,
            long fileSize,
            String sha256Hash,
            String ownerIp,
            String ownerName,
            List<String> peerIps
    ) {
        this(fileName, fileSize, sha256Hash, ownerIp, ownerName, peerIps, "", "", "");
    }

    /** Tạo bản sao đã đính kèm {@link AIMetadata}. */
    public SharedFile withAiMetadata(AIMetadata metadata) {
        if (metadata == null) {
            return this;
        }
        return new SharedFile(
                this.fileName,
                this.fileSize,
                this.sha256Hash,
                this.ownerIp,
                this.ownerName,
                this.peerIps,
                metadata.tags(),
                metadata.summary(),
                metadata.sensitiveWarning()
        );
    }

    public boolean hasAiMetadata() {
        return !aiTags.isBlank() || !aiSummary.isBlank();
    }

    public boolean hasSensitiveWarning() {
        return !sensitiveWarning.isBlank();
    }

    /** Lấy thẻ ngữ nghĩa đầu tiên (vd: {@code "#java-nio"}) để hiển thị dạng Pill gọn bên cạnh tên tệp. */
    public String primaryTagBadge() {
        if (aiTags == null || aiTags.isBlank()) {
            return "";
        }
        String[] tokens = aiTags.split(",");
        for (String t : tokens) {
            String clean = t.trim();
            if (!clean.isEmpty()) {
                return "#" + clean;
            }
        }
        return "";
    }

    /** Số lượng nguồn (Peers) đang nắm giữ tệp này. */
    public int peerCount() {
        return peerIps.size();
    }

    /** Chuỗi hiển thị cột "Số nguồn (Peers)" trên TableView. */
    public String peerCountDisplay() {
        int count = peerCount();
        return count > 1 ? count + " Peers (Swarm)" : "1 Peer";
    }

    /** Gộp thêm một nguồn Peer có cùng mã SHA-256 vào bản ghi hiện tại (giữ lại AI Metadata giàu thông tin nhất). */
    public SharedFile mergeWith(SharedFile other) {
        if (other == null) {
            return this;
        }
        Set<String> mergedIps = new LinkedHashSet<>(this.peerIps);
        mergedIps.addAll(other.peerIps);

        String mergedOwnerName = this.ownerName;
        if (!this.ownerName.contains(other.ownerName)) {
            mergedOwnerName = this.ownerName + ", " + other.ownerName;
        }
        String primaryIp = this.isLocal() && !other.isLocal() ? other.ownerIp : this.ownerIp;
        String bestTags = !this.aiTags.isBlank() ? this.aiTags : other.aiTags;
        String bestSummary = !this.aiSummary.isBlank() ? this.aiSummary : other.aiSummary;
        String bestWarning = !this.sensitiveWarning.isBlank() ? this.sensitiveWarning : other.sensitiveWarning;

        return new SharedFile(
                this.fileName,
                this.fileSize,
                this.sha256Hash,
                primaryIp,
                mergedOwnerName,
                new ArrayList<>(mergedIps),
                bestTags,
                bestSummary,
                bestWarning
        );
    }

    /**
     * Trả về danh sách các địa chỉ IP thực tế để tải tệp.
     */
    public List<String> resolvedTargetIps() {
        List<String> remoteIps = new ArrayList<>();
        for (String ip : peerIps) {
            if (!"LOCAL".equalsIgnoreCase(ip) && !"127.0.0.1".equals(ip)) {
                remoteIps.add(ip);
            }
        }
        if (!remoteIps.isEmpty()) {
            return remoteIps;
        }
        return List.of("127.0.0.1");
    }

    /** Định dạng dung lượng tệp dễ đọc (B, KB, MB, GB, TB). */
    public String formattedSize() {
        return formatSize(fileSize);
    }

    /** Hàm static tiện ích để định dạng số byte sang đơn vị dễ đọc (B, KB, MB, GB, TB). */
    public static String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        String[] units = {"KB", "MB", "GB", "TB"};
        double value = bytes;
        int unitIndex = -1;
        while (value >= 1024 && unitIndex < units.length - 1) {
            value /= 1024.0;
            unitIndex++;
        }
        return String.format(Locale.US, "%.2f %s", value, units[unitIndex]);
    }

    /** Chuỗi hiển thị cột "Máy sở hữu (Peer)" trên bảng giao diện. */
    public String ownerDisplay() {
        if (peerIps.size() == 1 && isLocal()) {
            return ownerName + " (Máy của bạn)";
        }
        if (peerIps.size() > 1) {
            return ownerName + " [" + String.join(", ", resolvedTargetIps()) + "]";
        }
        return ownerName + " - " + ownerIp;
    }

    /** Kiểm tra tệp này có phải chỉ đang nằm trên máy cục bộ hay không. */
    public boolean isLocal() {
        return "127.0.0.1".equals(ownerIp) || "LOCAL".equalsIgnoreCase(ownerIp);
    }

    /**
     * Đóng gói thông tin tệp thành 1 dòng văn bản phân tách bởi ký tự {@code '|'}
     * để truyền qua mạng khi phản hồi lệnh {@code LIST_FILES} (8 trường, tương thích ngược với 5 trường).
     * <pre>fileName|fileSize|sha256Hash|ownerIp|ownerName|aiTags|aiSummary|sensitiveWarning</pre>
     * <p>
     * <b>Chốt chặn Bảo mật (S3):</b> Nếu tệp có cảnh báo dữ liệu nhạy cảm (sensitiveWarning),
     * các thẻ tags và tóm tắt AI sẽ KHÔNG được phát tán công khai trên mạng LAN.
     */
    public String toWireLine() {
        String safeName = fileName.replace('|', '_');
        String safeOwner = ownerName.replace('|', '_');
        String wireTags = hasSensitiveWarning() ? "" : aiTags;
        String wireSummary = hasSensitiveWarning() ? "" : aiSummary;
        return String.join(WIRE_SEPARATOR,
                safeName,
                Long.toString(fileSize),
                sha256Hash,
                ownerIp,
                safeOwner,
                wireTags,
                wireSummary,
                sensitiveWarning);
    }

    public String name() {
        return fileName;
    }

    public long size() {
        return fileSize;
    }

    /**
     * Giải mã một dòng phản hồi từ lệnh {@code LIST_FILES} thành đối tượng {@link SharedFile}.
     * Hỗ trợ tương thích ngược với cả gói tin 5 trường (Milestone 3-6) và gói tin 7-8 trường (Phase 2 AI).
     */
    public static SharedFile fromWireLine(String line, String fallbackIp) {
        if (line == null || line.isBlank()) {
            return null;
        }
        String[] parts = line.split("\\|", -1);
        if (parts.length < 5) {
            return null;
        }
        try {
            String name = parts[0].trim();
            long size = Long.parseLong(parts[1].trim());
            String hash = parts[2].trim();
            String ip = parts[3].trim();
            String owner = parts[4].trim();
            String tags = parts.length > 5 ? parts[5].trim() : "";
            String summary = parts.length > 6 ? parts[6].trim() : "";
            String warning = parts.length > 7 ? parts[7].trim() : "";

            if (ip.isEmpty() || "127.0.0.1".equals(ip) || "LOCAL".equalsIgnoreCase(ip)) {
                ip = fallbackIp;
            }
            return new SharedFile(name, size, hash, ip, owner, List.of(ip), tags, summary, warning);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
