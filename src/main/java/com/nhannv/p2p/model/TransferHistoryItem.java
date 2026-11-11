package com.nhannv.p2p.model;

import java.util.Locale;
import java.util.Objects;

/**
 * Bản ghi lịch sử và thống kê hiệu năng của một phiên truyền tải tệp P2P (Milestone 5).
 * <p>
 * Phục vụ trực tiếp cho việc trích xuất số liệu thực nghiệm ra tệp {@code .csv}
 * đưa vào chương <i>"Thực nghiệm và Đánh giá hiệu năng"</i> của báo cáo Đồ án.
 *
 * @param timestamp        thời điểm hoàn tất (định dạng {@code yyyy-MM-dd HH:mm:ss})
 * @param fileName         tên tệp đã truyền/nhận
 * @param fileSizeBytes    dung lượng tệp tính bằng byte
 * @param peerEndpoint     địa chỉ Peer đối tác (hoặc danh sách Peers nếu tải Swarm)
 * @param direction        chiều truyền tải ({@code "Gửi (Push/Serve)"} hoặc {@code "Nhận (Pull/Swarm)"})
 * @param averageSpeedMBps tốc độ truyền trung bình (MB/s)
 * @param durationMs       tổng thời gian hoàn thành tính bằng mili-giây (ms)
 * @param sha256Status     kết quả kiểm tra toàn vẹn ({@code "Hợp lệ"} hoặc {@code "Lỗi SHA-256 / Thất bại"})
 */
public record TransferHistoryItem(
        String timestamp,
        String fileName,
        long fileSizeBytes,
        String peerEndpoint,
        String direction,
        double averageSpeedMBps,
        long durationMs,
        String sha256Status
) {

    public TransferHistoryItem {
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(fileName, "fileName");
        Objects.requireNonNull(peerEndpoint, "peerEndpoint");
        Objects.requireNonNull(direction, "direction");
        Objects.requireNonNull(sha256Status, "sha256Status");
    }

    public String formattedSize() {
        return SharedFile.formatSize(fileSizeBytes);
    }

    public String formattedSpeed() {
        return String.format(Locale.US, "%.2f MB/s", averageSpeedMBps);
    }

    public String formattedDuration() {
        if (durationMs < 1_000) {
            return durationMs + " ms";
        }
        return String.format(Locale.US, "%.2f s (%d ms)", durationMs / 1000.0, durationMs);
    }

    /** Dòng tiêu đề chuẩn của tệp báo cáo CSV. */
    public static String csvHeader() {
        return "ThoiGian,TenTep,DungLuong_Bytes,DungLuong_Format,PeerDoiTac,ChieuTruyen,TocDoTrungBinh_MBps,ThoiGianHoanThanh_ms,KetQua_SHA256";
    }

    /** Xuất bản ghi này thành 1 dòng CSV chuẩn (tự động bọc dấu ngoặc kép các trường có dấu phẩy). */
    public String toCsvRow() {
        return String.join(",",
                escapeCsv(timestamp),
                escapeCsv(fileName),
                Long.toString(fileSizeBytes),
                escapeCsv(formattedSize()),
                escapeCsv(peerEndpoint),
                escapeCsv(direction),
                String.format(Locale.US, "%.4f", averageSpeedMBps),
                Long.toString(durationMs),
                escapeCsv(sha256Status)
        );
    }

    private static String escapeCsv(String field) {
        if (field == null) {
            return "";
        }
        if (field.contains(",") || field.contains("\"") || field.contains("\n")) {
            return "\"" + field.replace("\"", "\"\"") + "\"";
        }
        return field;
    }
}
