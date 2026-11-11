package com.nhannv.p2p.network;

import javafx.concurrent.Task;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Locale;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Luồng nền (TCP Client) chịu trách nhiệm cắt nhỏ tệp (Chunking) và truyền sang một Peer đích.
 *
 * <h2>Giải thích kiến trúc cho team (Huỳnh Nhân Danh)</h2>
 * <ol>
 *   <li><b>Tách biệt hoàn toàn khỏi UI Thread:</b>
 *       Mỗi lần người dùng chọn tệp để gửi, một đối tượng {@code FileSenderTask} mới được khởi tạo
 *       và chạy trên luồng nền riêng. Giao diện JavaFX dùng {@code progressProperty().bind(...)}
 *       và {@code messageProperty().bind(...)} để tự động phản chiếu tiến độ gửi lên màn hình.</li>
 *   <li><b>Cơ chế Chunking (Đọc/Gửi theo khối 64KB):</b>
 *       Tệp nguồn được đọc từ đĩa cứng qua {@link FileInputStream} từng khối {@code 64KB} vào bộ đệm
 *       tĩnh {@code byte[] buffer}, rồi đẩy ngay qua {@link DataOutputStream} của Socket.
 *       Bộ nhớ Heap không bao giờ bị phình to dù gửi tệp video/ISO hàng chục GB.</li>
 * </ol>
 */
public class FileSenderTask extends Task<Void> {

    private static final Logger LOG = Logger.getLogger(FileSenderTask.class.getName());

    /** Kích thước khối (Chunk size) = 64 KB, đồng bộ với bên nhận (FileReceiverTask). */
    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;

    /** Thời gian chờ kết nối TCP tối đa (5 giây) để phát hiện sớm Peer offline hoặc bị Firewall chặn. */
    private static final int CONNECT_TIMEOUT_MS = 5_000;

    private final File selectedFile;
    private final String targetIp;
    private final int targetPort;

    public FileSenderTask(File selectedFile, String targetIp) {
        this(selectedFile, targetIp, FileReceiverTask.DEFAULT_PORT);
    }

    public FileSenderTask(File selectedFile, String targetIp, int targetPort) {
        this.selectedFile = Objects.requireNonNull(selectedFile, "selectedFile");
        this.targetIp = Objects.requireNonNull(targetIp, "targetIp");
        this.targetPort = targetPort;
    }

    @Override
    protected Void call() throws Exception {
        if (!selectedFile.exists() || !selectedFile.isFile()) {
            String errorMsg = "Trạng thái: Lỗi - Tệp không tồn tại: " + selectedFile.getName();
            updateMessage(errorMsg);
            throw new IOException(errorMsg);
        }

        long totalBytes = selectedFile.length();
        String fileName = selectedFile.getName();

        updateProgress(0, Math.max(totalBytes, 1));
        updateMessage("Trạng thái: Đang kết nối tới " + targetIp + ":" + targetPort + "...");

        try (Socket socket = new Socket()) {
            // Kết nối có timeout để không treo luồng nếu máy đích chặn Firewall.
            socket.connect(new InetSocketAddress(targetIp, targetPort), CONNECT_TIMEOUT_MS);
            socket.setTcpNoDelay(true);

            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                 BufferedInputStream fis = new BufferedInputStream(
                         new FileInputStream(selectedFile), CHUNK_BUFFER_SIZE)) {

                // Bước 1: Gửi tên tệp (UTF-8).
                dos.writeUTF(fileName);

                // Bước 2: Gửi kích thước tệp (8-byte long).
                dos.writeLong(totalBytes);

                // Bước 3 & 4: CHUNKING - Đọc tệp từng khối 64KB và đẩy qua Socket.
                byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
                long bytesSent = 0;
                int bytesRead;
                long startTimeNano = System.nanoTime();

                while ((bytesRead = fis.read(buffer)) != -1) {
                    if (isCancelled()) {
                        updateMessage("Trạng thái: Đã hủy gửi tệp \"" + fileName + "\"");
                        return null;
                    }

                    dos.write(buffer, 0, bytesRead);
                    bytesSent += bytesRead;

                    // Cập nhật tiến trình gửi về thanh ProgressBar và Label trên UI.
                    updateProgress(bytesSent, Math.max(totalBytes, 1));
                    double speedMbps = calculateSpeedMBps(bytesSent, startTimeNano);
                    int percent = totalBytes > 0 ? (int) ((bytesSent * 100) / totalBytes) : 100;
                    updateMessage(String.format(Locale.US,
                            "Trạng thái: Đang gửi \"%s\" tới %s (%d%% - %.2f MB/s)",
                            fileName, targetIp, percent, speedMbps));
                }

                dos.flush();

                updateProgress(1.0, 1.0);
                double avgSpeed = calculateSpeedMBps(bytesSent, startTimeNano);
                updateMessage(String.format(Locale.US,
                        "Trạng thái: Đã gửi xong \"%s\" tới %s (%.2f MB/s)",
                        fileName, targetIp, avgSpeed));
                LOG.info(() -> "Đã gửi thành công tệp " + fileName + " tới " + targetIp);
            }
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Lỗi khi gửi tệp " + fileName + " tới " + targetIp, e);
            updateProgress(0.0, 1.0);
            updateMessage("Trạng thái: Lỗi gửi tệp tới " + targetIp + " - " + e.getMessage());
            throw e;
        }
        return null;
    }

    private static double calculateSpeedMBps(long bytesTransferred, long startTimeNano) {
        double elapsedSeconds = (System.nanoTime() - startTimeNano) / 1_000_000_000.0;
        if (elapsedSeconds <= 0.0001) {
            return 0.0;
        }
        return (bytesTransferred / (1024.0 * 1024.0)) / elapsedSeconds;
    }
}
