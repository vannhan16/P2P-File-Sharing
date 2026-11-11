package com.nhannv.p2p.network;

import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.util.HashUtil;
import javafx.concurrent.Task;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tác vụ tải tệp từ một Peer khác về máy (Pull Protocol), kết hợp đo tốc độ thời gian thực (MB/s)
 * và xác thực toàn vẹn dữ liệu bằng <b>On-the-fly SHA-256</b>.
 *
 * <h2>Giải thích cơ chế On-the-fly SHA-256 & Đo tốc độ cho Huỳnh Nhân Danh</h2>
 * <ol>
 *   <li><b>Giao thức Pull ({@code DOWNLOAD_FILE}):</b>
 *       Kết nối TCP tới IP của Peer đang giữ tệp (port {@value P2PServer#DEFAULT_PORT}),
 *       gửi lệnh {@code "DOWNLOAD_FILE"} kèm tên tệp cần tải. Peer trả về kích thước tệp ({@code long})
 *       và mã băm SHA-256 gốc ({@code String}), sau đó truyền luồng dữ liệu thô theo từng khối 64KB.</li>
 *   <li><b>On-the-fly Hashing (Băm ngay trên luồng đọc):</b>
 *       Thông thường, nếu tải xong tệp 4GB rồi mới mở tệp lên đọc lại từ đầu để tính SHA-256 thì
 *       người dùng phải chờ gấp đôi thời gian I/O đĩa. Thay vào đó, ngay trong vòng lặp {@code while}
 *       đọc từng chunk {@code 64KB} từ {@code Socket}:
 *       <ul>
 *         <li>Vừa ghi {@code buffer} xuống {@link FileOutputStream}.</li>
 *         <li>Vừa đẩy chính {@code buffer} đó vào {@link MessageDigest#update(byte[], int, int)}.</li>
 *       </ul>
 *       Khi byte cuối cùng chạm đĩa cứng, mã SHA-256 cũng được tính xong tức thì (0ms chờ thêm!).</li>
 *   <li><b>Đối chiếu và tự làm sạch:</b>
 *       Nếu chuỗi hex SHA-256 tính được khớp 100% với mã gốc của người gửi -> báo thành công kèm
 *       8 ký tự đầu của SHA-256. Nếu sai lệch dù chỉ 1 bit -> xóa ngay tệp lỗi trên đĩa và cảnh báo.</li>
 * </ol>
 */
public class FileDownloadTask extends Task<Boolean> {

    private static final Logger LOG = Logger.getLogger(FileDownloadTask.class.getName());

    /** Kích thước bộ đệm đọc từng khối (Chunk size) = 64 KB. */
    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 15_000;

    private final String targetIp;
    private final int targetPort;
    private final String fileName;
    private final Path downloadDirectory;

    public FileDownloadTask(SharedFile remoteFile) {
        this(
                Objects.requireNonNull(remoteFile, "remoteFile").ownerIp(),
                P2PServer.DEFAULT_PORT,
                remoteFile.fileName()
        );
    }

    public FileDownloadTask(String targetIp, String fileName) {
        this(targetIp, P2PServer.DEFAULT_PORT, fileName);
    }

    public FileDownloadTask(String targetIp, int targetPort, String fileName) {
        this.targetIp = Objects.requireNonNull(targetIp, "targetIp");
        this.targetPort = targetPort;
        this.fileName = Objects.requireNonNull(fileName, "fileName");
        this.downloadDirectory = Path.of(P2PServer.DOWNLOADS_DIR_NAME).toAbsolutePath();
    }

    @Override
    protected Boolean call() {
        File outputFile = null;
        try {
            Files.createDirectories(downloadDirectory);

            updateProgress(0.0, 1.0);
            updateMessage("Đang kết nối tới Peer " + targetIp + " để tải: " + fileName + "...");

            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(targetIp, targetPort), CONNECT_TIMEOUT_MS);
                socket.setSoTimeout(READ_TIMEOUT_MS);
                socket.setTcpNoDelay(true);

                try (DataOutputStream dos = new DataOutputStream(
                        new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                     DataInputStream dis = new DataInputStream(
                             new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE))) {

                    // Bước 1: Gửi Command Header "DOWNLOAD_FILE" kèm tên tệp yêu cầu.
                    dos.writeUTF(P2PServer.CMD_DOWNLOAD_FILE);
                    dos.writeUTF(fileName);
                    dos.flush();

                    // Bước 2: Đọc kích thước tệp và mã SHA-256 gốc từ Peer gửi về.
                    long totalBytes = dis.readLong();
                    if (totalBytes < 0) {
                        updateProgress(0.0, 1.0);
                        updateMessage("Lỗi: Tệp \"" + fileName + "\" không còn tồn tại trên máy Peer!");
                        return false;
                    }
                    String expectedSha256 = dis.readUTF();

                    String safeFileName = Path.of(fileName).getFileName().toString();
                    outputFile = P2PServer.resolveOutputFile(downloadDirectory, safeFileName);

                    // Bước 3: Khởi tạo bộ băm SHA-256 (On-the-fly Hashing) và đọc từng chunk 64KB.
                    MessageDigest sha256Digest = HashUtil.newSha256Digest();
                    byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
                    long bytesReadTotal = 0;

                    long transferStartNano = System.nanoTime();
                    long windowStartNano = transferStartNano;
                    long bytesInWindow = 0;
                    double currentSpeedMBps = 0.0;

                    try (BufferedOutputStream bos = new BufferedOutputStream(
                            new FileOutputStream(outputFile), CHUNK_BUFFER_SIZE)) {

                        long remaining = totalBytes;
                        while (remaining > 0) {
                            if (isCancelled()) {
                                bos.close();
                                Files.deleteIfExists(outputFile.toPath());
                                updateMessage("Đã hủy tải tệp: " + fileName);
                                return false;
                            }

                            int maxToRead = (int) Math.min(buffer.length, remaining);
                            int read = dis.read(buffer, 0, maxToRead);
                            if (read == -1) {
                                break;
                            }

                            // Ghi khối byte xuống đĩa cứng.
                            bos.write(buffer, 0, read);

                            // ON-THE-FLY HASHING: Cập nhật ngay khối byte vào MessageDigest SHA-256.
                            sha256Digest.update(buffer, 0, read);

                            bytesReadTotal += read;
                            remaining -= read;
                            bytesInWindow += read;

                            // Đo tốc độ tải thời gian thực (MB/s) theo cửa sổ trượt >= 150ms
                            // (kết hợp tốc độ trung bình toàn phiên cho các tệp nhỏ tải cực nhanh).
                            long nowNano = System.nanoTime();
                            double windowSeconds = (nowNano - windowStartNano) / 1_000_000_000.0;
                            if (windowSeconds >= 0.15) {
                                currentSpeedMBps = (bytesInWindow / (1024.0 * 1024.0)) / windowSeconds;
                                windowStartNano = nowNano;
                                bytesInWindow = 0;
                            } else if (currentSpeedMBps == 0.0) {
                                double totalElapsed = Math.max((nowNano - transferStartNano) / 1_000_000_000.0, 0.001);
                                currentSpeedMBps = (bytesReadTotal / (1024.0 * 1024.0)) / totalElapsed;
                            }

                            updateProgress(bytesReadTotal, Math.max(totalBytes, 1));
                            updateMessage(String.format(Locale.US,
                                    "Đang tải: %s (%.2f MB/s)",
                                    fileName, currentSpeedMBps));
                        }
                        bos.flush();
                    }

                    // Kiểm tra xem đã nhận đủ 100% số byte hay chưa.
                    if (bytesReadTotal != totalBytes) {
                        Files.deleteIfExists(outputFile.toPath());
                        updateProgress(0.0, 1.0);
                        updateMessage("Lỗi: Ngắt kết nối giữa chừng khi đang tải " + fileName);
                        return false;
                    }

                    // Bước 4: Hoàn tất băm SHA-256 và đối chiếu với mã SHA-256 gốc của người gửi.
                    String calculatedSha256 = HashUtil.toHexString(sha256Digest.digest());
                    if (calculatedSha256.equalsIgnoreCase(expectedSha256)) {
                        updateProgress(1.0, 1.0);
                        updateMessage("Tải hoàn tất! SHA-256 hợp lệ: [" + HashUtil.shortHash(calculatedSha256) + "]");
                        LOG.info(() -> "Tải thành công tệp " + fileName + " - SHA-256 khớp: " + calculatedSha256);
                        return true;
                    } else {
                        // Sai lệch SHA-256 -> Xóa ngay tệp bị hỏng để bảo vệ người dùng!
                        Files.deleteIfExists(outputFile.toPath());
                        updateProgress(0.0, 1.0);
                        updateMessage("Cảnh báo: File bị hỏng trong quá trình truyền (Sai lệch SHA-256)!");
                        LOG.warning(() -> "Sai lệch SHA-256 cho tệp " + fileName
                                + "! Gốc=" + expectedSha256 + ", Thực tế=" + calculatedSha256);
                        return false;
                    }
                }
            }
        } catch (EOFException e) {
            cleanupCorruptedFile(outputFile);
            updateProgress(0.0, 1.0);
            updateMessage("Lỗi: Peer ngắt kết nối đột ngột khi đang tải " + fileName);
            LOG.log(Level.WARNING, "EOF khi tải tệp " + fileName, e);
            return false;
        } catch (IOException e) {
            cleanupCorruptedFile(outputFile);
            updateProgress(0.0, 1.0);
            updateMessage("Lỗi tải tệp " + fileName + ": " + e.getMessage());
            LOG.log(Level.WARNING, "Lỗi I/O khi tải tệp " + fileName, e);
            return false;
        }
    }

    private static void cleanupCorruptedFile(File file) {
        if (file != null && file.exists()) {
            try {
                Files.deleteIfExists(file.toPath());
            } catch (IOException ignored) {
                // Bỏ qua lỗi khi xóa tệp tạm.
            }
        }
    }
}
