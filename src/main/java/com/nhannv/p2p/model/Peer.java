package com.nhannv.p2p.model;

import java.util.Objects;

/**
 * Đại diện cho một máy (Peer) đang online trong mạng LAN.
 * <p>
 * Là một record bất biến (immutable) nên an toàn khi truyền qua lại giữa
 * luồng mạng (background thread) và luồng giao diện JavaFX.
 *
 * @param ipAddress         địa chỉ IPv4 của Peer (vd: "192.168.1.15")
 * @param hostName          tên máy do Peer tự quảng bá
 * @param lastSeenTimestamp thời điểm (epoch millis) nhận được gói tin gần nhất từ Peer
 */
public record Peer(String ipAddress, String hostName, long lastSeenTimestamp) {

    public Peer {
        Objects.requireNonNull(ipAddress, "ipAddress");
        Objects.requireNonNull(hostName, "hostName");
    }

    /** Tạo bản sao với thời điểm "last seen" mới (record là bất biến). */
    public Peer withLastSeen(long timestamp) {
        return new Peer(ipAddress, hostName, timestamp);
    }

    /**
     * Hai bản ghi cùng IP được coi là cùng một Peer, bất kể timestamp.
     * Dùng để cập nhật/xóa đúng phần tử trong danh sách trên UI.
     */
    public boolean isSamePeer(Peer other) {
        return other != null && ipAddress.equals(other.ipAddress);
    }

    /** Hiển thị trên ListView, vd: "DESKTOP-NHAN - 192.168.1.15". */
    @Override
    public String toString() {
        return hostName + " - " + ipAddress;
    }
}
