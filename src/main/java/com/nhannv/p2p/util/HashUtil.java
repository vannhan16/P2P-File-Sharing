package com.nhannv.p2p.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Tiện ích băm mật mã SHA-256 tối ưu bộ nhớ cho tệp dung lượng lớn.
 *
 * <h2>Giải thích cơ chế cho team (Huỳnh Nhân Danh)</h2>
 * <ul>
 *   <li><b>Đọc theo khối 64KB (Chunked Hashing):</b>
 *       Hàm {@link #calculateSHA256(File)} không nạp toàn bộ tệp vào RAM mà đẩy từng khối
 *       {@code 64KB} qua {@link MessageDigest#update(byte[], int, int)}. Nhờ vậy có thể băm
 *       tệp hàng chục GB với mức tiêu thụ RAM cố định chỉ 64KB.</li>
 *   <li><b>Hỗ trợ On-the-fly Hashing khi tải tệp:</b>
 *       Cung cấp {@link #newSha256Digest()} và {@link #toHexString(byte[])} để {@code FileDownloadTask}
 *       và {@code P2PServer} vừa nhận từng chunk từ Socket vừa cập nhật mã băm ngay trên luồng byte,
 *       tiết kiệm 100% thời gian đọc lại đĩa sau khi tải xong.</li>
 * </ul>
 */
public final class HashUtil {

    /** Kích thước bộ đệm đọc tệp khi băm = 64 KB. */
    public static final int HASH_BUFFER_SIZE = 64 * 1024;

    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private HashUtil() {
        // Lớp tiện ích chỉ chứa hàm static, không cho phép khởi tạo.
    }

    /**
     * Tính chuỗi hex SHA-256 (64 ký tự chữ thường) của một tệp trên đĩa theo từng khối 64KB.
     *
     * @param file tệp cần tính SHA-256
     * @return chuỗi hex 64 ký tự biểu diễn mã băm SHA-256
     * @throws IOException nếu tệp không tồn tại hoặc lỗi đọc đĩa
     */
    public static String calculateSHA256(File file) throws IOException {
        Objects.requireNonNull(file, "file");
        MessageDigest digest = newSha256Digest();
        byte[] buffer = new byte[HASH_BUFFER_SIZE];

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file), HASH_BUFFER_SIZE)) {
            int bytesRead;
            while ((bytesRead = bis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
        }
        return toHexString(digest.digest());
    }

    /** Khởi tạo đối tượng {@link MessageDigest} thuật toán SHA-256 để dùng cho On-the-fly Hashing. */
    public static MessageDigest newSha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 là thuật toán bắt buộc phải có trong mọi chuẩn JVM.
            throw new IllegalStateException("JVM không hỗ trợ thuật toán SHA-256", e);
        }
    }

    /** Chuyển mảng byte kết quả của {@link MessageDigest#digest()} sang chuỗi Hex 64 ký tự. */
    public static String toHexString(byte[] digestBytes) {
        Objects.requireNonNull(digestBytes, "digestBytes");
        return HEX_FORMAT.formatHex(digestBytes);
    }

    /** Rút gọn mã SHA-256 (lấy 8 ký tự đầu) để hiển thị thông báo ngắn gọn. */
    public static String shortHash(String sha256Hex) {
        if (sha256Hex == null || sha256Hex.isBlank()) {
            return "N/A";
        }
        return sha256Hex.length() <= 8 ? sha256Hex : sha256Hex.substring(0, 8) + "...";
    }
}
