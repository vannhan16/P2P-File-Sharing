package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.TransferHistoryItem;
import com.nhannv.p2p.util.HashUtil;
import javafx.concurrent.Task;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Luồng nền (TCP Client - Push Protocol) chủ động đẩy một tệp sang Peer đích.
 * <p>
 * Ở Milestone 5, {@code FileSenderTask} hỗ trợ kết nối {@code host:port} (Ngrok/Playit),
 * gửi kèm mã PIN xác thực 4 chữ số, cập nhật biểu đồ tốc độ {@code AreaChart} và ghi nhận
 * lịch sử truyền tải {@link TransferHistoryItem}.
 */
public class FileSenderTask extends Task<Void> {

    private static final Logger LOG = Logger.getLogger(FileSenderTask.class.getName());
    private static final DateTimeFormatter HISTORY_TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 5_000;

    private final File selectedFile;
    private final String targetHost;
    private final int targetPort;
    private final String authPin;
    private final DoubleConsumer speedSampleCallback;
    private final Consumer<TransferHistoryItem> historyCallback;
    private final Consumer<String> eventLogger;

    public FileSenderTask(File selectedFile, String targetEndpoint) {
        this(selectedFile, targetEndpoint, "", null, null, null);
    }

    public FileSenderTask(File selectedFile, String targetHost, int targetPort) {
        this(selectedFile, targetHost + ":" + targetPort, "", null, null, null);
    }

    public FileSenderTask(
            File selectedFile,
            String targetEndpoint,
            String authPin,
            DoubleConsumer speedSampleCallback,
            Consumer<TransferHistoryItem> historyCallback,
            Consumer<String> eventLogger
    ) {
        this.selectedFile = Objects.requireNonNull(selectedFile, "selectedFile");
        Peer.HostPort hp = Peer.parseHostPort(targetEndpoint, P2PServer.DEFAULT_PORT);
        this.targetHost = hp.host();
        this.targetPort = hp.port();
        this.authPin = (authPin == null) ? "" : authPin.trim();
        this.speedSampleCallback = speedSampleCallback;
        this.historyCallback = historyCallback;
        this.eventLogger = eventLogger;
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
        String endpointStr = targetPort == P2PServer.DEFAULT_PORT ? targetHost : (targetHost + ":" + targetPort);

        updateProgress(0, Math.max(totalBytes, 1));
        updateMessage("Trạng thái: Đang tính SHA-256 cho \"" + fileName + "\" trước khi gửi...");
        String sha256Hash = HashUtil.calculateSHA256(selectedFile);

        updateMessage("Trạng thái: Đang kết nối tới " + endpointStr + "...");

        long startTimeNano = System.nanoTime();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(targetHost, targetPort), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(15_000);
            socket.setTcpNoDelay(true);

            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                 DataInputStream dis = new DataInputStream(
                         new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE));
                 BufferedInputStream fis = new BufferedInputStream(
                         new FileInputStream(selectedFile), CHUNK_BUFFER_SIZE)) {

                // Bước 1: Gửi Command Header "PUSH_FILE" + Tên tệp + Kích thước + SHA-256 + Mã PIN.
                dos.writeUTF(P2PServer.CMD_PUSH_FILE);
                dos.writeUTF(fileName);
                dos.writeLong(totalBytes);
                dos.writeUTF(sha256Hash);
                dos.writeUTF(authPin);
                dos.flush();

                // Đọc phản hồi xác thực PIN từ P2PServer.
                String authResponse = dis.readUTF();
                if (P2PServer.STATUS_AUTH_FAILED.equals(authResponse)) {
                    updateProgress(0.0, 1.0);
                    String msg = "Từ chối gửi tệp (AUTH_FAILED): Sai mã PIN bảo mật của Peer " + endpointStr + "!";
                    updateMessage("Trạng thái: " + msg);
                    logEvent(msg);
                    recordHistory(fileName, totalBytes, endpointStr, 0.0, 1L, "Lỗi: Sai mã PIN (AUTH_FAILED)");
                    throw new IOException(msg);
                }

                // Bước 2: CHUNKING - Đọc tệp từng khối 64KB và đẩy qua Socket.
                byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
                long bytesSent = 0;
                int bytesRead;
                startTimeNano = System.nanoTime();

                while ((bytesRead = fis.read(buffer)) != -1) {
                    if (isCancelled()) {
                        reportSpeed(0.0);
                        updateMessage("Trạng thái: Đã hủy gửi tệp \"" + fileName + "\"");
                        return null;
                    }

                    dos.write(buffer, 0, bytesRead);
                    bytesSent += bytesRead;

                    updateProgress(bytesSent, Math.max(totalBytes, 1));
                    double speedMbps = calculateSpeedMBps(bytesSent, startTimeNano);
                    reportSpeed(speedMbps);
                    updateMessage(String.format(Locale.US,
                            "Đang gửi: %s (%.2f MB/s)",
                            fileName, speedMbps));
                }

                dos.flush();
                reportSpeed(0.0);

                long durationMs = Math.max(1L, (System.nanoTime() - startTimeNano) / 1_000_000L);
                double avgSpeed = calculateSpeedMBps(bytesSent, startTimeNano);

                updateProgress(1.0, 1.0);
                String doneMsg = String.format(Locale.US,
                        "Đã gửi hoàn tất: %s (%.2f MB/s) | SHA-256: [%s]",
                        fileName, avgSpeed, HashUtil.shortHash(sha256Hash));
                updateMessage(doneMsg);
                logEvent(doneMsg);
                recordHistory(fileName, totalBytes, endpointStr, avgSpeed, durationMs,
                        "Hợp lệ [" + HashUtil.shortHash(sha256Hash) + "]");
                LOG.info(() -> "Đã gửi thành công tệp " + fileName + " tới " + endpointStr);
            }
        } catch (IOException e) {
            reportSpeed(0.0);
            LOG.log(Level.WARNING, "Lỗi khi gửi tệp " + fileName + " tới " + endpointStr, e);
            updateProgress(0.0, 1.0);
            updateMessage("Trạng thái: Lỗi gửi tệp tới " + endpointStr + " - " + e.getMessage());
            logEvent("Lỗi gửi tệp \"" + fileName + "\" tới " + endpointStr + ": " + e.getMessage());
            throw e;
        }
        return null;
    }

    private void reportSpeed(double speedMBps) {
        if (speedSampleCallback != null) {
            speedSampleCallback.accept(speedMBps);
        }
    }

    private void recordHistory(
            String fileName,
            long sizeBytes,
            String peerEndpoint,
            double avgSpeedMBps,
            long durationMs,
            String shaStatus
    ) {
        if (historyCallback != null) {
            historyCallback.accept(new TransferHistoryItem(
                    LocalDateTime.now().format(HISTORY_TIME_FMT),
                    fileName,
                    sizeBytes,
                    peerEndpoint,
                    "Gửi (Push)",
                    avgSpeedMBps,
                    durationMs,
                    shaStatus
            ));
        }
    }

    private void logEvent(String msg) {
        if (eventLogger != null) {
            eventLogger.accept(msg);
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
