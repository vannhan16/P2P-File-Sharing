package com.nhannv.p2p.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Mô hình dữ liệu biểu diễn một tệp đang được chia sẻ trong mạng P2P LAN.
 * <p>
 * Ở Milestone 4, {@code SharedFile} hỗ trợ lưu danh sách nhiều địa chỉ IP ({@code peerIps})
 * của tất cả các Peer cùng sở hữu tệp có chung mã {@code sha256Hash} để phục vụ tính năng
 * <b>Multi-source Swarm Download</b> (tải đa nguồn song song).
 *
 * @param fileName   tên tệp (vd: {@code "BaoCao_DoAn.pdf"})
 * @param fileSize   kích thước tệp tính bằng byte
 * @param sha256Hash chuỗi hex SHA-256 (64 ký tự) của tệp
 * @param ownerIp    địa chỉ IPv4 chính của Peer sở hữu tệp
 * @param ownerName  tên máy (hostname) của Peer sở hữu tệp (hoặc danh sách tên máy gộp)
 * @param peerIps    danh sách toàn bộ địa chỉ IP các Peer đang giữ tệp có cùng mã SHA-256 này
 */
public record SharedFile(
        String fileName,
        long fileSize,
        String sha256Hash,
        String ownerIp,
        String ownerName,
        List<String> peerIps
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
    }

    /** Constructor 5 tham số tương thích ngược với Milestone 3. */
    public SharedFile(
            String fileName,
            long fileSize,
            String sha256Hash,
            String ownerIp,
            String ownerName
    ) {
        this(fileName, fileSize, sha256Hash, ownerIp, ownerName, List.of(ownerIp));
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

    /** Gộp thêm một nguồn Peer có cùng mã SHA-256 vào bản ghi hiện tại. */
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
        return new SharedFile(
                this.fileName,
                this.fileSize,
                this.sha256Hash,
                primaryIp,
                mergedOwnerName,
                new ArrayList<>(mergedIps)
        );
    }

    /**
     * Trả về danh sách các địa chỉ IP thực tế để tải tệp.
     * Nếu có các Peer từ xa thì ưu tiên lấy IP từ xa; nếu chỉ có máy cục bộ thì dùng {@code 127.0.0.1}.
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
            return new SharedFile(name, size, hash, ip, owner, List.of(ip));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
