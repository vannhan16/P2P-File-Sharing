package com.nhannv.p2p.ai;

/**
 * Ảnh chụp trạng thái thực tế của hệ thống mạng P2P tại thời điểm người dùng yêu cầu
 * <b>Chẩn đoán Lỗi Mạng &amp; Tối ưu Băng thông (AI Network Diagnostics)</b>.
 *
 * @param recentNetworkLogs         25 dòng nhật ký sự kiện mạng gần nhất từ {@code networkLogArea}
 * @param detectedSubnetsSummary    danh sách card mạng LAN vật lý &amp; Radmin VPN đang hoạt động
 * @param firewallConfigured        {@code true} nếu Windows Firewall đã mở Rule cho UDP 8888 &amp; TCP 8889
 * @param onlinePeersCount          số lượng Peer đang hiển thị trên Radar
 * @param peerLatencySummary        thông số độ trễ Ping RTT ({@code ms}) của các Peer
 * @param bandwidthLimitBytesPerSec giới hạn băng thông QoS hiện tại ({@code 0} = Không giới hạn)
 * @param transferHistorySummary    tóm tắt các phiên truyền tải gần nhất (tốc độ TB, lỗi SHA-256/PIN nếu có)
 */
public record NetworkDiagnosticContext(
        String recentNetworkLogs,
        String detectedSubnetsSummary,
        boolean firewallConfigured,
        int onlinePeersCount,
        String peerLatencySummary,
        long bandwidthLimitBytesPerSec,
        String transferHistorySummary
) {
}
