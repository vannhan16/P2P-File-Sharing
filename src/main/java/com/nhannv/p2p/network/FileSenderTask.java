package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.TransferHistoryItem;
import com.nhannv.p2p.util.BandwidthLimiter;
import com.nhannv.p2p.util.HashUtil;
import com.nhannv.p2p.util.PathUtil;
import com.nhannv.p2p.util.TransferUtil;
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
import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Luồng nền (TCP Client - Push Protocol) chủ động đẩy một tệp sang Peer đích.
 * <p>
 * <b>Các Cải tiến:</b>
 * <ul>
 *   <li><b>Xác thực Challenge-Response HMAC (S1):</b> Mã PIN không truyền dạng thô qua mạng.</li>
 *   <li><b>Xác nhận hai chiều (B6):</b> Sau khi gửi xong toàn bộ byte, người gửi chờ phản hồi {@code OK}
 *       từ máy nhận (đã kiểm tra SHA-256) rồi mới ghi nhận trạng thái "Hợp lệ".</li>
 *   <li><b>Chuẩn hóa tên tệp (S8):</b> Dùng {@link PathUtil#sanitizeFileName}.</li>
 * </ul>
 */
public class FileSenderTask extends Task<Void> {

    private static final Logger LOG = Logger.getLogger(FileSenderTask.class.getName());
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
        String safeFileName = PathUtil.sanitizeFileName(selectedFile.getName());
        String endpointStr = targetPort == P2PServer.DEFAULT_PORT ? targetHost : (targetHost + ":" + targetPort);

        updateProgress(0, Math.max(totalBytes, 1));
        updateMessage("Trạng thái: Đang tính SHA-256 cho \"" + safeFileName + "\" trước khi gửi...");
        String sha256Hash = HashUtil.calculateSHA256(selectedFile);

        updateMessage("Trạng thái: Đang kết nối tới " + endpointStr + "...");

        long startTimeNano = System.nanoTime();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(targetHost, targetPort), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(25_000);
            socket.setTcpNoDelay(true);

            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                 DataInputStream dis = new DataInputStream(
                         new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE));
                 BufferedInputStream fis = new BufferedInputStream(
                         new FileInputStream(selectedFile), CHUNK_BUFFER_SIZE)) {

                // Bước 1: Gửi lệnh PUSH_FILE + Header tệp
                dos.writeUTF(P2PServer.CMD_PUSH_FILE);
                dos.writeUTF(safeFileName);
                dos.writeLong(totalBytes);
                dos.writeUTF(sha256Hash);
                dos.flush();

                // Xác thực Challenge-Response HMAC (S1)
                if (!P2PServer.performClientAuth(dis, dos, authPin, P2PServer.CMD_PUSH_FILE)) {
                    updateProgress(0.0, 1.0);
                    String msg = "Từ chối gửi tệp (AUTH_FAILED): Sai mã PIN bảo mật của Peer " + endpointStr + "!";
                    updateMessage("Trạng thái: " + msg);
                    logEvent(msg);
                    TransferUtil.recordHistory(historyCallback, safeFileName, totalBytes, endpointStr, "Gửi (Push)", 0.0, 1L, "Lỗi: Sai mã PIN (AUTH_FAILED)");
                    throw new IOException(msg);
                }

                // Đọc phản hồi phê duyệt từ P2PServer
                String pushApproval = dis.readUTF();
                if (P2PServer.STATUS_REJECTED.equals(pushApproval)) {
                    updateProgress(0.0, 1.0);
                    String msg = "Peer " + endpointStr + " đã từ chối nhận tệp \"" + safeFileName + "\"!";
                    updateMessage("Trạng thái: " + msg);
                    logEvent(msg);
                    TransferUtil.recordHistory(historyCallback, safeFileName, totalBytes, endpointStr, "Gửi (Push)", 0.0, 1L, "Từ chối bởi người nhận");
                    throw new IOException(msg);
                }
                if (P2PServer.STATUS_INSUFFICIENT_SPACE.equals(pushApproval)) {
                    updateProgress(0.0, 1.0);
                    String msg = "Peer " + endpointStr + " không đủ dung lượng ổ cứng để nhận tệp này!";
                    updateMessage("Trạng thái: " + msg);
                    logEvent(msg);
                    TransferUtil.recordHistory(historyCallback, safeFileName, totalBytes, endpointStr, "Gửi (Push)", 0.0, 1L, "Ổ cứng nhận đầy");
                    throw new IOException(msg);
                }

                // Bước 2: CHUNKING - Đọc tệp từng khối 64KB và đẩy qua Socket
                byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
                long bytesSent = 0;
                int bytesRead;
                startTimeNano = System.nanoTime();

                while ((bytesRead = fis.read(buffer)) != -1) {
                    if (isCancelled()) {
                        reportSpeed(0.0);
                        updateMessage("Trạng thái: Đã hủy gửi tệp \"" + safeFileName + "\"");
                        return null;
                    }

                    dos.write(buffer, 0, bytesRead);
                    BandwidthLimiter.getInstance().throttle(bytesRead);
                    bytesSent += bytesRead;

                    updateProgress(bytesSent, Math.max(totalBytes, 1));
                    double speedMbps = TransferUtil.calculateSpeedMBps(bytesSent, startTimeNano);
                    reportSpeed(speedMbps);
                    updateMessage(String.format(Locale.US,
                            "Đang gửi: %s (%.2f MB/s)",
                            safeFileName, speedMbps));
                }

                dos.flush();
                reportSpeed(0.0);

                // Bước 3 [B6 Fix]: Chờ xác nhận thực tế từ phía người nhận sau khi người nhận đã băm SHA-256
                updateMessage("Trạng thái: Đang đợi Peer " + endpointStr + " xác thực mã băm SHA-256...");
                String receiverStatus = dis.readUTF();

                long durationMs = Math.max(1L, (System.nanoTime() - startTimeNano) / 1_000_000L);
                double avgSpeed = TransferUtil.calculateSpeedMBps(bytesSent, startTimeNano);

                if (!P2PServer.STATUS_OK.equals(receiverStatus)) {
                    String errMsg = "Người nhận báo lỗi kiểm tra toàn vẹn: " + receiverStatus;
                    updateProgress(0.0, 1.0);
                    updateMessage("Trạng thái: " + errMsg);
                    logEvent(errMsg);
                    TransferUtil.recordHistory(historyCallback, safeFileName, totalBytes, endpointStr, "Gửi (Push)", avgSpeed, durationMs, "Lỗi: " + receiverStatus);
                    throw new IOException(errMsg);
                }

                updateProgress(1.0, 1.0);
                String doneMsg = String.format(Locale.US,
                        "Đã gửi hoàn tất và người nhận xác nhận OK: %s (%.2f MB/s) | SHA-256: [%s]",
                        safeFileName, avgSpeed, HashUtil.shortHash(sha256Hash));
                updateMessage(doneMsg);
                logEvent(doneMsg);
                TransferUtil.recordHistory(historyCallback, safeFileName, totalBytes, endpointStr, "Gửi (Push)", avgSpeed, durationMs,
                        "Hợp lệ [" + HashUtil.shortHash(sha256Hash) + "]");
                LOG.info(() -> "Đã gửi thành công tệp " + safeFileName + " tới " + endpointStr);
            }
        } catch (IOException e) {
            reportSpeed(0.0);
            LOG.log(Level.WARNING, "Lỗi khi gửi tệp " + safeFileName + " tới " + endpointStr, e);
            updateProgress(0.0, 1.0);
            updateMessage("Trạng thái: Lỗi gửi tệp tới " + endpointStr + " - " + e.getMessage());
            logEvent("Lỗi gửi tệp \"" + safeFileName + "\" tới " + endpointStr + ": " + e.getMessage());
            throw e;
        }
        return null;
    }

    private void reportSpeed(double speedMBps) {
        if (speedSampleCallback != null) {
            speedSampleCallback.accept(speedMBps);
        }
    }

    private void logEvent(String msg) {
        if (eventLogger != null) {
            eventLogger.accept(msg);
        }
    }
}
