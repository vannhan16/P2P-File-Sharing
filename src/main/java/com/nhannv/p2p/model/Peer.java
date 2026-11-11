package com.nhannv.p2p.model;

import java.util.Objects;

/**
 * Đại diện cho một máy (Peer) đang online trong mạng LAN, Radmin VPN hoặc qua Internet Tunnel (Ngrok/Playit.gg).
 * <p>
 * Ở Milestone 5, {@code Peer} lưu thêm {@code tcpPort} riêng cho từng máy (thay vì cố định 8889)
 * để hỗ trợ kết nối định dạng {@code host:port} (ví dụ: {@code 0.tcp.ap.ngrok.io:14523})
 * và cờ {@code pinProtected} báo hiệu Peer có bật mã PIN bảo vệ 4 chữ số.
 *
 * @param ipAddress         địa chỉ IPv4 hoặc tên miền (hostname) của Peer (vd: {@code "192.168.1.15"} hoặc {@code "0.tcp.ap.ngrok.io"})
 * @param tcpPort           cổng TCP của Peer (mặc định {@code 8889}, hoặc cổng động khi dùng Ngrok/Playit.gg)
 * @param hostName          tên máy do Peer tự quảng bá
 * @param lastSeenTimestamp thời điểm (epoch millis) nhận được gói tin gần nhất từ Peer
 * @param pinProtected      {@code true} nếu Peer này đang bật xác thực mã PIN 4 số
 */
public record Peer(
        String ipAddress,
        int tcpPort,
        String hostName,
        long lastSeenTimestamp,
        boolean pinProtected
) {

    public static final int DEFAULT_TCP_PORT = 8889;

    /** Cấu trúc tách chuỗi nhập {@code host:port} từ người dùng. */
    public record HostPort(String host, int port) {
        public String toEndpointString() {
            return port == DEFAULT_TCP_PORT ? host : (host + ":" + port);
        }
    }

    public Peer {
        Objects.requireNonNull(ipAddress, "ipAddress");
        Objects.requireNonNull(hostName, "hostName");
        if (tcpPort < 1 || tcpPort > 65_535) {
            tcpPort = DEFAULT_TCP_PORT;
        }
    }

    /** Constructor 3 tham số tương thích ngược với các Milestone trước (mặc định port 8889). */
    public Peer(String ipAddress, String hostName, long lastSeenTimestamp) {
        this(
                parseHostPort(ipAddress, DEFAULT_TCP_PORT).host(),
                parseHostPort(ipAddress, DEFAULT_TCP_PORT).port(),
                hostName,
                lastSeenTimestamp,
                false
        );
    }

    /** Constructor 4 tham số với cổng TCP tùy chỉnh. */
    public Peer(String ipAddress, int tcpPort, String hostName, long lastSeenTimestamp) {
        this(ipAddress, tcpPort, hostName, lastSeenTimestamp, false);
    }

    /**
     * Phân tích chuỗi địa chỉ có thể chứa {@code host:port} (vd: {@code "0.tcp.ap.ngrok.io:14523"}
     * hoặc {@code "192.168.1.10:8889"}). Nếu không nhập {@code :port}, mặc định dùng {@code defaultPort}.
     */
    public static HostPort parseHostPort(String rawInput, int defaultPort) {
        if (rawInput == null || rawInput.isBlank()) {
            return new HostPort("127.0.0.1", defaultPort);
        }
        String trimmed = rawInput.trim();
        // Bỏ tiền tố tcp:// hoặc http:// nếu người dùng copy nguyên URL từ Ngrok/Playit
        if (trimmed.startsWith("tcp://")) {
            trimmed = trimmed.substring("tcp://".length());
        } else if (trimmed.startsWith("http://")) {
            trimmed = trimmed.substring("http://".length());
        } else if (trimmed.startsWith("https://")) {
            trimmed = trimmed.substring("https://".length());
        }
        if (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }

        int colonIdx = trimmed.lastIndexOf(':');
        if (colonIdx > 0 && colonIdx < trimmed.length() - 1) {
            String hostPart = trimmed.substring(0, colonIdx).trim();
            String portPart = trimmed.substring(colonIdx + 1).trim();
            try {
                int parsedPort = Integer.parseInt(portPart);
                if (parsedPort >= 1 && parsedPort <= 65_535) {
                    return new HostPort(hostPart, parsedPort);
                }
            } catch (NumberFormatException ignored) {
                // Không phải số port hợp lệ -> coi toàn bộ là host.
            }
        }
        return new HostPort(trimmed, defaultPort);
    }

    /** Trả về chuỗi định danh kết nối {@code host:port} (hoặc {@code host} nếu dùng cổng mặc định 8889). */
    public String endpoint() {
        return tcpPort == DEFAULT_TCP_PORT ? ipAddress : (ipAddress + ":" + tcpPort);
    }

    /** Tạo bản sao với thời điểm "last seen" mới. */
    public Peer withLastSeen(long timestamp) {
        return new Peer(ipAddress, tcpPort, hostName, timestamp, pinProtected);
    }

    /**
     * Hai bản ghi có cùng địa chỉ host và cùng cổng TCP được coi là cùng một Peer.
     */
    public boolean isSamePeer(Peer other) {
        return other != null
                && ipAddress.equalsIgnoreCase(other.ipAddress)
                && tcpPort == other.tcpPort;
    }

    /** Hiển thị trên ListView, vd: "DESKTOP-NHAN - 192.168.1.15" hoặc "🔒 Danh-PC - 0.tcp.ap.ngrok.io:14523". */
    @Override
    public String toString() {
        String lockPrefix = pinProtected ? "[PIN] " : "";
        return lockPrefix + hostName + " - " + endpoint();
    }
}
