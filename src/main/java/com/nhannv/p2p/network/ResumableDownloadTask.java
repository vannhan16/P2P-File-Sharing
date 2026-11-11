package com.nhannv.p2p.network;

import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.util.HashUtil;
import javafx.concurrent.Task;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tác vụ tải tệp hỗ trợ <b>Tạm dừng / Tiếp tục (Pause / Resume)</b> dựa trên tệp tạm {@code .part}
 * và lệnh {@code DOWNLOAD_CHUNK} kèm {@link RandomAccessFile#seek(long)}.
 *
 * <h2>Giải thích Cơ chế Pause / Resume cho vấn đáp đồ án (Huỳnh Nhân Danh)</h2>
 * <ol>
 *   <li><b>Tệp tạm {@code <fileName>.part}:</b>
 *       Khi bắt đầu tải {@code video.mp4}, dữ liệu chưa hoàn chỉnh được ghi vào
 *       {@code Downloads/video.mp4.part}. Nếu rớt mạng hoặc người dùng bấm nút <b>"Tạm dừng (Pause)"</b>,
 *       file {@code .part} này được giữ nguyên trên ổ cứng.</li>
 *   <li><b>Phát hiện điểm nối tiếp ({@code existingBytes}) và {@code RandomAccessFile.seek()}:</b>
 *       Khi bấm <b>"Tiếp tục (Resume)"</b>:
 *       <ul>
 *         <li>Kiểm tra kích thước hiện có của tệp tạm: {@code existingBytes = partFile.length()}.</li>
 *         <li>Gửi lệnh {@code "DOWNLOAD_CHUNK"} tới Peer với:
 *             {@code startOffset = existingBytes} và {@code length = totalSize - existingBytes}.</li>
 *         <li>Mở {@code new RandomAccessFile(partFile, "rw")} và gọi {@code raf.seek(existingBytes)}
 *             để đặt con trỏ ghi nối tiếp ngay tại đuôi file {@code .part} mà không làm mất dữ liệu cũ!</li>
 *       </ul>
 *   </li>
 *   <li><b>Cờ {@code volatile boolean isPaused}:</b>
 *       Biến {@code isPaused} được đánh dấu {@code volatile} để khi luồng UI gọi {@link #pauseDownload()},
 *       luồng tải nền nhìn thấy giá trị {@code true} ngay lập tức ở vòng lặp đọc chunk kế tiếp,
 *       đóng file {@code .part} an toàn và dừng lại.</li>
 *   <li><b>Hoàn tất & Xác thực SHA-256:</b>
 *       Khi {@code partFile.length() == totalSize} (đạt 100%), toàn bộ file {@code .part} được băm
 *       SHA-256 bằng {@link HashUtil#calculateSHA256(File)}. Nếu khớp với mã SHA-256 gốc của người gửi,
 *       đổi tên {@code <fileName>.part} thành {@code <fileName>} hoàn chỉnh.</li>
 * </ol>
 */
public class ResumableDownloadTask extends Task<Boolean> {

    private static final Logger LOG = Logger.getLogger(ResumableDownloadTask.class.getName());

    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 15_000;

    private final SharedFile targetFile;
    private final String targetIp;
    private final int targetPort;
    private final Path downloadDirectory;
    private final Consumer<String> eventLogger;

    /**
     * Cờ volatile điều khiển tạm dừng từ luồng giao diện.
     * Từ khóa {@code volatile} đảm bảo tính nhìn thấy (visibility) tức thời giữa UI Thread và Worker Thread.
     */
    private volatile boolean isPaused = false;
    private volatile boolean isCanceledByUser = false;

    public ResumableDownloadTask(SharedFile targetFile, String targetIp, Consumer<String> eventLogger) {
        this(targetFile, targetIp, P2PServer.DEFAULT_PORT, eventLogger);
    }

    public ResumableDownloadTask(
            SharedFile targetFile,
            String targetIp,
            int targetPort,
            Consumer<String> eventLogger
    ) {
        this.targetFile = Objects.requireNonNull(targetFile, "targetFile");
        this.targetIp = Objects.requireNonNull(targetIp, "targetIp");
        this.targetPort = targetPort;
        this.downloadDirectory = Path.of(P2PServer.DOWNLOADS_DIR_NAME).toAbsolutePath();
        this.eventLogger = eventLogger;
    }

    /** Tạm dừng phiên tải hiện tại mà KHÔNG xóa tệp {@code .part}. */
    public void pauseDownload() {
        this.isPaused = true;
    }

    public boolean isPaused() {
        return isPaused;
    }

    /** Hủy hẳn phiên tải và xóa tệp {@code .part} trên ổ cứng. */
    public void cancelAndDeletePart() {
        this.isCanceledByUser = true;
        this.isPaused = false;
        cancel(true);
    }

    public Path getPartFilePath() {
        String safeFileName = Path.of(targetFile.fileName()).getFileName().toString();
        return downloadDirectory.resolve(safeFileName + ".part");
    }

    @Override
    protected Boolean call() {
        String safeFileName = Path.of(targetFile.fileName()).getFileName().toString();
        File partFile = getPartFilePath().toFile();
        long expectedTotalSize = targetFile.fileSize();

        try {
            Files.createDirectories(downloadDirectory);

            // Bước 1: Kiểm tra tệp <fileName>.part đã tồn tại từ lần tải bị gián đoạn trước chưa.
            long existingBytes = partFile.exists() ? partFile.length() : 0L;
            if (existingBytes > expectedTotalSize && expectedTotalSize > 0) {
                // Nếu file .part lớn bất thường so với kích thước gốc -> đặt lại về 0.
                Files.deleteIfExists(partFile.toPath());
                existingBytes = 0L;
            }

            if (existingBytes > 0 && existingBytes < expectedTotalSize) {
                double resumedPercent = (existingBytes * 100.0) / expectedTotalSize;
                logEvent(String.format(Locale.US,
                        "Tiếp tục tải \"%s\" từ offset %d (%.1f%% - %s đã có sẵn)...",
                        safeFileName, existingBytes, resumedPercent, SharedFile.formatSize(existingBytes)));
            } else if (existingBytes == 0) {
                logEvent(String.format(Locale.US,
                        "Bắt đầu tải \"%s\" (%s) từ Peer %s...",
                        safeFileName, targetFile.formattedSize(), targetIp));
            }

            long bytesRemainingToRequest = Math.max(expectedTotalSize - existingBytes, 0L);

            // Nếu file .part chưa tải đủ 100% thì kết nối tới Peer gửi lệnh DOWNLOAD_CHUNK.
            String serverSha256 = targetFile.sha256Hash();
            if (bytesRemainingToRequest > 0 || expectedTotalSize == 0) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(targetIp, targetPort), CONNECT_TIMEOUT_MS);
                    socket.setSoTimeout(READ_TIMEOUT_MS);
                    socket.setTcpNoDelay(true);

                    try (DataOutputStream dos = new DataOutputStream(
                            new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                         DataInputStream dis = new DataInputStream(
                                 new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE))) {

                        // Bước 2: Gửi yêu cầu DOWNLOAD_CHUNK với startOffset = existingBytes.
                        dos.writeUTF(P2PServer.CMD_DOWNLOAD_CHUNK);
                        dos.writeUTF(safeFileName);
                        dos.writeLong(existingBytes);
                        dos.writeLong(bytesRemainingToRequest);
                        dos.flush();

                        long remoteTotalSize = dis.readLong();
                        if (remoteTotalSize < 0) {
                            updateProgress(0.0, 1.0);
                            updateMessage("Lỗi: Tệp \"" + safeFileName + "\" không tồn tại trên Peer " + targetIp);
                            logEvent("Lỗi: Peer " + targetIp + " báo không tìm thấy tệp \"" + safeFileName + "\"");
                            return false;
                        }
                        long chunkLengthToRead = dis.readLong();
                        String remoteSha = dis.readUTF();
                        if (serverSha256 == null || serverSha256.isBlank()) {
                            serverSha256 = remoteSha;
                        }

                        // Bước 3: Mở RandomAccessFile ở chế độ "rw" và dùng seek(existingBytes)
                        // để ghi nối tiếp dữ liệu vào đúng đuôi của file .part.
                        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
                        long currentTotalDownloaded = existingBytes;
                        long sessionBytesRead = 0L;
                        long sessionStartNano = System.nanoTime();

                        try (RandomAccessFile raf = new RandomAccessFile(partFile, "rw")) {
                            // Di chuyển con trỏ ghi đến đúng vị trí byte đã tải được trước đó.
                            raf.seek(existingBytes);

                            long remaining = chunkLengthToRead;
                            while (remaining > 0) {
                                // Kiểm tra nếu người dùng bấm nút "Tạm dừng (Pause)" hoặc "Hủy (Cancel)".
                                if (isPaused) {
                                    double pct = remoteTotalSize > 0
                                            ? (currentTotalDownloaded * 100.0) / remoteTotalSize : 0.0;
                                    String pauseMsg = String.format(Locale.US,
                                            "Tạm dừng tại %.1f%% (%s / %s) - offset=%d",
                                            pct,
                                            SharedFile.formatSize(currentTotalDownloaded),
                                            SharedFile.formatSize(remoteTotalSize),
                                            currentTotalDownloaded);
                                    updateMessage("Trạng thái: " + pauseMsg);
                                    logEvent(pauseMsg);
                                    return false;
                                }

                                if (isCanceledByUser || isCancelled()) {
                                    raf.close();
                                    Files.deleteIfExists(partFile.toPath());
                                    updateProgress(0.0, 1.0);
                                    updateMessage("Trạng thái: Đã hủy tải và xóa tệp tạm " + partFile.getName());
                                    logEvent("Đã hủy tải \"" + safeFileName + "\" và xóa file .part.");
                                    return false;
                                }

                                int maxToRead = (int) Math.min(buffer.length, remaining);
                                int read = dis.read(buffer, 0, maxToRead);
                                if (read == -1) {
                                    break;
                                }

                                // Ghi khối dữ liệu vừa nhận vào vị trí hiện tại của RandomAccessFile.
                                raf.write(buffer, 0, read);

                                currentTotalDownloaded += read;
                                sessionBytesRead += read;
                                remaining -= read;

                                double speedMBps = calculateSpeedMBps(sessionBytesRead, sessionStartNano);
                                int pct = remoteTotalSize > 0
                                        ? (int) ((currentTotalDownloaded * 100) / remoteTotalSize) : 100;
                                updateProgress(currentTotalDownloaded, Math.max(remoteTotalSize, 1));
                                updateMessage(String.format(Locale.US,
                                        "Đang tải: %s (%d%% - %.2f MB/s)",
                                        safeFileName, pct, speedMBps));
                            }
                        }

                        if (currentTotalDownloaded < remoteTotalSize) {
                            double pct = (currentTotalDownloaded * 100.0) / remoteTotalSize;
                            String msg = String.format(Locale.US,
                                    "Gián đoạn kết nối tại %.1f%% (offset=%d). Bấm 'Tiếp tục (Resume)' để tải tiếp!",
                                    pct, currentTotalDownloaded);
                            updateMessage("Trạng thái: " + msg);
                            logEvent(msg);
                            return false;
                        }
                    }
                }
            }

            // Bước 4: Đã tải đủ 100% số byte -> Băm SHA-256 toàn bộ file .part để xác thực toàn vẹn.
            updateMessage("Đang kiểm tra mã băm SHA-256 cho tệp " + partFile.getName() + "...");
            String actualSha256 = HashUtil.calculateSHA256(partFile);

            if (serverSha256 != null && !serverSha256.isBlank()
                    && !actualSha256.equalsIgnoreCase(serverSha256)) {
                Files.deleteIfExists(partFile.toPath());
                updateProgress(0.0, 1.0);
                String err = "Cảnh báo: File bị hỏng trong quá trình truyền (Sai lệch SHA-256)!";
                updateMessage(err);
                logEvent(err + " [Mong đợi: " + HashUtil.shortHash(serverSha256)
                        + ", Thực tế: " + HashUtil.shortHash(actualSha256) + "]");
                return false;
            }

            // Bước 5: SHA-256 hợp lệ -> Đổi tên <fileName>.part thành <fileName> hoàn chỉnh!
            File finalFile = P2PServer.resolveOutputFile(downloadDirectory, safeFileName);
            Files.move(partFile.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            updateProgress(1.0, 1.0);
            String okMsg = "Tải hoàn tất! SHA-256 hợp lệ: [" + HashUtil.shortHash(actualSha256)
                    + "] -> " + finalFile.getName();
            updateMessage(okMsg);
            logEvent(okMsg);
            return true;

        } catch (IOException e) {
            if (isPaused) {
                logEvent("Đã tạm dừng tải \"" + safeFileName + "\" tại offset " + partFile.length());
                return false;
            }
            if (isCanceledByUser || isCancelled()) {
                try {
                    Files.deleteIfExists(partFile.toPath());
                } catch (IOException ignored) {
                }
                return false;
            }
            LOG.log(Level.WARNING, "Lỗi I/O khi tải ResumableDownloadTask: " + safeFileName, e);
            String errMsg = "Gián đoạn mạng khi tải \"" + safeFileName + "\" (đã lưu "
                    + SharedFile.formatSize(partFile.length()) + " vào .part). Có thể bấm Resume!";
            updateMessage("Trạng thái: " + errMsg);
            logEvent(errMsg);
            return false;
        }
    }

    private void logEvent(String message) {
        if (eventLogger != null) {
            eventLogger.accept(message);
        }
    }

    private static double calculateSpeedMBps(long bytesTransferred, long startTimeNano) {
        double elapsedSeconds = (System.nanoTime() - startTimeNano) / 1_000_000_000.0;
        if (elapsedSeconds <= 0.0001) {
            return 0.0;
        }
        return (bytesTransferred / (1024.0 * 1024.0)) / elapsedSeconds;
    }
}
