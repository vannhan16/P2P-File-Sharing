package com.nhannv.p2p.network;

import com.nhannv.p2p.util.HashUtil;
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
 * Luồng nền (TCP Client - Push Protocol) chủ động đẩy một tệp sang Peer đích.
 * <p>
 * Đã nâng cấp lên giao thức đa lệnh của Milestone 3: gửi Command Header {@code "PUSH_FILE"}
 * kèm mã băm SHA-256 gốc của tệp để bên nhận ({@link P2PServer}) kiểm tra toàn vẹn sau khi nhận.
 */
public class FileSenderTask extends Task<Void> {

    private static final Logger LOG = Logger.getLogger(FileSenderTask.class.getName());

    /** Kích thước khối (Chunk size) = 64 KB. */
    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 5_000;

    private final File selectedFile;
    private final String targetIp;
    private final int targetPort;

    public FileSenderTask(File selectedFile, String targetIp) {
        this(selectedFile, targetIp, P2PServer.DEFAULT_PORT);
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
        updateMessage("Trạng thái: Đang tính SHA-256 cho \"" + fileName + "\" trước khi gửi...");
        String sha256Hash = HashUtil.calculateSHA256(selectedFile);

        updateMessage("Trạng thái: Đang kết nối tới " + targetIp + ":" + targetPort + "...");

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(targetIp, targetPort), CONNECT_TIMEOUT_MS);
            socket.setTcpNoDelay(true);

            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                 BufferedInputStream fis = new BufferedInputStream(
                         new FileInputStream(selectedFile), CHUNK_BUFFER_SIZE)) {

                // Bước 1: Gửi Command Header "PUSH_FILE" + Tên tệp + Kích thước + Mã SHA-256 gốc.
                dos.writeUTF(P2PServer.CMD_PUSH_FILE);
                dos.writeUTF(fileName);
                dos.writeLong(totalBytes);
                dos.writeUTF(sha256Hash);

                // Bước 2: CHUNKING - Đọc tệp từng khối 64KB và đẩy qua Socket.
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

                    updateProgress(bytesSent, Math.max(totalBytes, 1));
                    double speedMbps = calculateSpeedMBps(bytesSent, startTimeNano);
                    updateMessage(String.format(Locale.US,
                            "Đang gửi: %s (%.2f MB/s)",
                            fileName, speedMbps));
                }

                dos.flush();

                updateProgress(1.0, 1.0);
                double avgSpeed = calculateSpeedMBps(bytesSent, startTimeNano);
                updateMessage(String.format(Locale.US,
                        "Đã gửi hoàn tất: %s (%.2f MB/s) | SHA-256: [%s]",
                        fileName, avgSpeed, HashUtil.shortHash(sha256Hash)));
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
