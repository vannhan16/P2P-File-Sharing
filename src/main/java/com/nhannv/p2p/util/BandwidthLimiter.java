package com.nhannv.p2p.util;

/**
 * Bộ điều tiết băng thông chủ động theo thuật toán <b>Token Bucket (QoS Bandwidth Throttling)</b>
 * (Milestone 6 - Gói 4: Tối ưu hiệu năng mạng).
 *
 * <h2>Giải thích Cơ chế Token Bucket cho Buổi Bảo vệ Đồ án</h2>
 * <ul>
 *   <li>Khi truyền các tệp hàng GB trong mạng LAN nội bộ, một kết nối TCP không giới hạn có thể
 *       chiếm trọn 100% băng thông card mạng, gây nghẽn các dịch vụ khác (như gọi video, lướt web).</li>
 *   <li>{@code BandwidthLimiter} chia thời gian thành các cửa sổ ngắn và cấp phát hạn mức byte
 *       tối đa mỗi giây ({@code maxBytesPerSecond}). Khi luồng đọc/ghi 64KB vượt quá hạn mức trong
 *       cửa sổ hiện tại, luồng sẽ tạm nghỉ ({@code Thread.sleep}) đúng số mili-giây cần thiết để
 *       giữ tốc độ truyền bám sát ngưỡng người dùng chọn trên giao diện.</li>
 * </ul>
 */
public final class BandwidthLimiter {

    private static final BandwidthLimiter INSTANCE = new BandwidthLimiter();

    /** 0 = Không giới hạn băng thông (Max Speed). */
    private volatile long maxBytesPerSecond = 0L;

    private long windowStartNano = System.nanoTime();
    private long bytesInCurrentWindow = 0L;

    private BandwidthLimiter() {
    }

    public static BandwidthLimiter getInstance() {
        return INSTANCE;
    }

    /**
     * Đặt giới hạn tốc độ tối đa tính bằng byte/giây ({@code <= 0} nghĩa là không giới hạn).
     */
    public synchronized void setMaxBytesPerSecond(long maxBytesPerSecond) {
        this.maxBytesPerSecond = Math.max(0L, maxBytesPerSecond);
        this.windowStartNano = System.nanoTime();
        this.bytesInCurrentWindow = 0L;
    }

    public long getMaxBytesPerSecond() {
        return maxBytesPerSecond;
    }

    /**
     * Điều tiết luồng truyền tải sau khi đọc/ghi {@code bytesTransferred} byte.
     */
    public void throttle(int bytesTransferred) {
        long limit = this.maxBytesPerSecond;
        if (limit <= 0L || bytesTransferred <= 0) {
            return;
        }

        long sleepMillis = 0L;
        synchronized (this) {
            long now = System.nanoTime();
            long elapsedNano = now - windowStartNano;

            // Reset cửa sổ mỗi 250ms để tốc độ mượt mà, không bị giật cục theo từng giây
            if (elapsedNano >= 250_000_000L) {
                windowStartNano = now;
                bytesInCurrentWindow = 0L;
                elapsedNano = 0L;
            }

            bytesInCurrentWindow += bytesTransferred;
            long expectedNanoForBytes = (bytesInCurrentWindow * 1_000_000_000L) / limit;
            if (expectedNanoForBytes > elapsedNano) {
                sleepMillis = (expectedNanoForBytes - elapsedNano) / 1_000_000L;
            }
        }

        if (sleepMillis > 1L) {
            try {
                Thread.sleep(Math.min(sleepMillis, 500L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
