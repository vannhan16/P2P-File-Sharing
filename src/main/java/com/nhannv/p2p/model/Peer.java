package com.nhannv.p2p.model;

import java.util.Objects;

/**
 * Đại diện cho một máy (Peer) đang online trong mạng LAN, Radmin VPN hoặc qua Internet Tunnel (Ngrok/Playit.gg).
 * <p>
 * Ở Milestone 5 &amp; Milestone 6, {@code Peer} lưu thêm:
 * <ul>
 *   <li>{@code tcpPort} riêng cho từng máy (hỗ trợ {@code host:port} như {@code 0.tcp.ap.ngrok.io:14523}).</li>
 *   <li>{@code pinProtected}: cờ báo hiệu Peer đang bật khóa bảo mật PIN 4 số.</li>
 *   <li>{@code latencyMs}: độ trễ phản hồi Ping RTT tính bằng mili-giây ({@code ms}) để xếp hạng ưu tiên Peer
 *       khi tải đa nguồn (Swarm).</li>
 *   <li>Hỗ trợ phân tích <b>1-Click Invite Token</b> định dạng {@code host:port#PIN}.</li>
 * </ul>
 *
 * @param ipAddress         địa chỉ IPv4 hoặc tên miền (hostname) của Peer
 * @param tcpPort           cổng TCP của Peer (mặc định {@code 8889})
 * @param hostName          tên máy do Peer tự quảng bá
 * @param lastSeenTimestamp thời điểm (epoch millis) nhận được gói tin gần nhất từ Peer
 * @param pinProtected      {@code true} nếu Peer này đang bật xác thực mã PIN 4 số
 * @param latencyMs         độ trễ RTT (mili-giây), hoặc {@code -1} nếu chưa đo
 */
public record Peer(
        String ipAddress,
        int tcpPort,
        String hostName,
        long lastSeenTimestamp,
        boolean pinProtected,
        long latencyMs
) {

    public static final int DEFAULT_TCP_PORT = 8889;

    /** Cấu trúc tách chuỗi nhập {@code host:port} từ người dùng. */
    public record HostPort(String host, int port) {
        public String toEndpointString() {
            return port == DEFAULT_TCP_PORT ? host : (host + ":" + port);
        }
    }

    /** Cấu trúc tách chuỗi <b>1-Click Invite Token</b> {@code host:port#PIN}. */
    public record InviteToken(String host, int port, String pin) {
        public HostPort toHostPort() {
            return new HostPort(host, port);
        }
    }

    public Peer {
        Objects.requireNonNull(ipAddress, "ipAddress");
        Objects.requireNonNull(hostName, "hostName");
        if (tcpPort < 1 || tcpPort > 65_535) {
            tcpPort = DEFAULT_TCP_PORT;
        }
    }

    /** Constructor 3 tham số tương thích ngược (mặc định port 8889, chưa bật PIN, chưa đo RTT). */
    public Peer(String ipAddress, String hostName, long lastSeenTimestamp) {
        this(
                parseHostPort(ipAddress, DEFAULT_TCP_PORT).host(),
                parseHostPort(ipAddress, DEFAULT_TCP_PORT).port(),
                hostName,
                lastSeenTimestamp,
                false,
                -1L
        );
    }

    /** Constructor 4 tham số với cổng TCP tùy chỉnh. */
    public Peer(String ipAddress, int tcpPort, String hostName, long lastSeenTimestamp) {
        this(ipAddress, tcpPort, hostName, lastSeenTimestamp, false, -1L);
    }

    /** Constructor 5 tham số tương thích với Milestone 5. */
    public Peer(String ipAddress, int tcpPort, String hostName, long lastSeenTimestamp, boolean pinProtected) {
        this(ipAddress, tcpPort, hostName, lastSeenTimestamp, pinProtected, -1L);
    }

    /**
     * Phân tích chuỗi <b>Invite Token</b> có thể kèm mã PIN sau dấu {@code #}
     * (ví dụ: {@code "26.45.12.88:8889#4821"} hoặc {@code "0.tcp.ap.ngrok.io:14523#9999"}).
     */
    public static InviteToken parseInviteToken(String rawInput, int defaultPort) {
        if (rawInput == null || rawInput.isBlank()) {
            return new InviteToken("127.0.0.1", defaultPort, "");
        }
        String trimmed = rawInput.trim();
        String extractedPin = "";
        int hashIdx = trimmed.indexOf('#');
        if (hashIdx >= 0) {
            extractedPin = trimmed.substring(hashIdx + 1).trim();
            trimmed = trimmed.substring(0, hashIdx).trim();
        }
        HostPort hp = parseHostPort(trimmed, defaultPort);
        return new InviteToken(hp.host(), hp.port(), extractedPin);
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
        int hashIdx = trimmed.indexOf('#');
        if (hashIdx > 0) {
            trimmed = trimmed.substring(0, hashIdx).trim();
        }
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
        return new Peer(ipAddress, tcpPort, hostName, timestamp, pinProtected, latencyMs);
    }

    /** Tạo bản sao với độ trễ RTT (mili-giây) mới đo được. */
    public Peer withLatency(long newLatencyMs) {
        return new Peer(ipAddress, tcpPort, hostName, lastSeenTimestamp, pinProtected, newLatencyMs);
    }

    /**
     * Hai bản ghi có cùng địa chỉ host và cùng cổng TCP được coi là cùng một Peer.
     */
    public boolean isSamePeer(Peer other) {
        return other != null
                && ipAddress.equalsIgnoreCase(other.ipAddress)
                && tcpPort == other.tcpPort;
    }

    /** Hiển thị trên ListView, vd: "[PIN] DESKTOP-NHAN - 192.168.1.15 (2 ms)". */
    @Override
    public String toString() {
        String lockPrefix = pinProtected ? "[PIN] " : "";
        String rttBadge = (latencyMs >= 0) ? (" (" + latencyMs + " ms)") : "";
        return lockPrefix + hostName + " - " + endpoint() + rttBadge;
    }
}
