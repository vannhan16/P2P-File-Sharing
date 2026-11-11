package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;

/**
 * Callback từ tầng mạng (Discovery) về tầng trên (UI / Service).
 * <p>
 * <b>Lưu ý quan trọng:</b> mọi phương thức đều được gọi trên luồng nền của
 * {@link DiscoveryService}, KHÔNG phải JavaFX Application Thread. Bên triển khai
 * nếu cập nhật giao diện thì bắt buộc phải bọc trong {@code Platform.runLater()}.
 */
@FunctionalInterface
public interface PeerDiscoveryListener {

    /** Phát hiện một Peer mới (hoặc Peer đổi tên máy). */
    void onPeerDiscovered(Peer peer);

    /** Peer đã offline: gửi gói BYE hoặc quá thời gian không nhận được tín hiệu. */
    default void onPeerLost(Peer peer) {
        // Mặc định không làm gì.
    }

    /** Lỗi mạng xảy ra khi service đang chạy (rớt mạng, firewall chặn gửi...). */
    default void onDiscoveryError(String message, Throwable cause) {
        // Mặc định không làm gì.
    }
}
