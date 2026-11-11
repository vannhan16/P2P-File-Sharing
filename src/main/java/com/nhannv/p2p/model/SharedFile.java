package com.nhannv.p2p.model;

import java.util.Locale;
import java.util.Objects;

/**
 * Mô hình dữ liệu biểu diễn một tệp đang được chia sẻ trong mạng P2P LAN.
 * <p>
 * Lưu đầy đủ metadata để hiển thị lên {@code TableView} (4 cột: Tên tệp, Dung lượng,
 * Máy sở hữu, Mã SHA-256) và để {@code FileDownloadTask} biết cần kết nối tới IP nào
 * cũng như đối chiếu mã băm SHA-256 gốc sau khi tải xong.
 *
 * @param fileName   tên tệp (vd: {@code "BaoCao_DoAn.pdf"})
 * @param fileSize   kích thước tệp tính bằng byte
 * @param sha256Hash chuỗi hex SHA-256 (64 ký tự) của tệp
 * @param ownerIp    địa chỉ IPv4 của Peer sở hữu tệp
 * @param ownerName  tên máy (hostname) của Peer sở hữu tệp
 */
public record SharedFile(
        String fileName,
        long fileSize,
        String sha256Hash,
        String ownerIp,
        String ownerName
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
        if ("127.0.0.1".equals(ownerIp) || "LOCAL".equalsIgnoreCase(ownerIp)) {
            return ownerName + " (Máy của bạn)";
        }
        return ownerName + " - " + ownerIp;
    }

    /** Kiểm tra tệp này có phải đang nằm ngay trên máy cục bộ hay không. */
    public boolean isLocal() {
        return "127.0.0.1".equals(ownerIp) || "LOCAL".equalsIgnoreCase(ownerIp);
    }

    /**
     * Đóng gói thông tin tệp thành 1 dòng văn bản phân tách bởi ký tự {@code '|'}
     * để truyền qua mạng khi phản hồi lệnh {@code LIST_FILES}.
     * <pre>fileName|fileSize|sha256Hash|ownerIp|ownerName</pre>
     */
    public String toWireLine() {
        String safeName = fileName.replace('|', '_');
        String safeOwner = ownerName.replace('|', '_');
        return String.join(WIRE_SEPARATOR,
                safeName,
                Long.toString(fileSize),
                sha256Hash,
                ownerIp,
                safeOwner);
    }

    /**
     * Giải mã một dòng phản hồi từ lệnh {@code LIST_FILES} thành đối tượng {@link SharedFile}.
     *
     * @param line       chuỗi nhận từ mạng
     * @param fallbackIp địa chỉ IP thực tế của Socket gửi (dùng khi peer gửi "LOCAL" hoặc "127.0.0.1")
     * @return đối tượng {@link SharedFile}, hoặc {@code null} nếu dòng không đúng định dạng
     */
    public static SharedFile fromWireLine(String line, String fallbackIp) {
        if (line == null || line.isBlank()) {
            return null;
        }
        String[] parts = line.split("\\|", 5);
        if (parts.length < 5) {
            return null;
        }
        try {
            String name = parts[0].trim();
            long size = Long.parseLong(parts[1].trim());
            String hash = parts[2].trim();
            String ip = parts[3].trim();
            String owner = parts[4].trim();
            if (ip.isEmpty() || "127.0.0.1".equals(ip) || "LOCAL".equalsIgnoreCase(ip)) {
                ip = fallbackIp;
            }
            return new SharedFile(name, size, hash, ip, owner);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
