package com.nhannv.p2p.util;

/**
 * Bộ điều tiết băng thông chủ động theo thuật toán <b>Token Bucket thật (Token Bucket QoS Bandwidth Limiter)</b>
 * (Milestone 6 - Gói 4: Tối ưu hiệu năng mạng).
 *
 * <h2>Nguyên lý Hoạt động Chuẩn Kỹ sư</h2>
 * <ul>
 *   <li><b>Bucket Capacity &amp; Refill Rate:</b> Thùng chứa có sức chứa {@code capacity} và được nạp đầy
 *       với tốc độ {@code maxBytesPerSecond} tokens mỗi giây. Mỗi token tương ứng với 1 byte truyền tải.</li>
 *   <li><b>Đặt chỗ trước (Time Reservation):</b> Khi nhiều luồng cùng gọi {@link #throttle(int)}, mỗi luồng
 *       sẽ đặt chỗ một mốc thời gian trong tương lai ({@code nextAvailableNano}) bên trong khối đồng bộ.
 *       Nhờ đó, các luồng không bao giờ thức dậy cùng lúc để gây đột biến lưu lượng (Spike/Burst),
 *       mà giãn cách đều đặn đúng tốc độ giới hạn mong muốn.</li>
 *   <li><b>Hỗ trợ đa luồng an toàn:</b> Luồng chỉ ngủ phần thời gian còn thiếu theo hạn mức đã đặt trước,
 *       không bị cắt gọt tùy tiện tại 500ms.</li>
 * </ul>
 */
public final class BandwidthLimiter {

    private static final BandwidthLimiter INSTANCE = new BandwidthLimiter();

    /** 0 = Không giới hạn băng thông (Full Speed). */
    private volatile long maxBytesPerSecond = 0L;

    private double availableTokens = 0.0;
    private long lastRefillNano = System.nanoTime();
    private long nextAvailableNano = System.nanoTime();

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
        this.availableTokens = Math.max(0L, maxBytesPerSecond); // Khởi tạo đầy thùng
        this.lastRefillNano = System.nanoTime();
        this.nextAvailableNano = System.nanoTime();
    }

    public long getMaxBytesPerSecond() {
        return maxBytesPerSecond;
    }

    /**
     * Điều tiết luồng truyền tải theo giải thuật Token Bucket sau khi đọc/ghi {@code bytesTransferred} byte.
     */
    public void throttle(int bytesTransferred) {
        long rate = this.maxBytesPerSecond;
        if (rate <= 0L || bytesTransferred <= 0) {
            return;
        }

        long waitNano = 0L;
        synchronized (this) {
            long now = System.nanoTime();
            long elapsedNano = now - lastRefillNano;
            lastRefillNano = now;

            // Nạp thêm token theo thời gian trôi qua
            if (elapsedNano > 0) {
                double tokensToAdd = (elapsedNano * (double) rate) / 1_000_000_000.0;
                double capacity = Math.max((double) rate, (double) bytesTransferred * 2.0);
                availableTokens = Math.min(capacity, availableTokens + tokensToAdd);
            }

            if (availableTokens >= bytesTransferred) {
                availableTokens -= bytesTransferred;
                return; // Đủ token, đi tiếp không cần chờ
            }

            // Thiếu token: tính thời gian cần chờ để thùng nạp đủ số byte này
            double missingTokens = bytesTransferred - availableTokens;
            availableTokens = 0.0;
            long requiredNano = (long) ((missingTokens * 1_000_000_000.0) / rate);

            // Đặt chỗ mốc thời gian tương lai để các luồng không thức dậy cùng lúc
            long scheduleBase = Math.max(now, nextAvailableNano);
            nextAvailableNano = scheduleBase + requiredNano;
            waitNano = nextAvailableNano - now;
        }

        if (waitNano > 1_000_000L) { // Lớn hơn 1ms
            long sleepMillis = waitNano / 1_000_000L;
            int sleepNanos = (int) (waitNano % 1_000_000L);
            try {
                Thread.sleep(sleepMillis, sleepNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
