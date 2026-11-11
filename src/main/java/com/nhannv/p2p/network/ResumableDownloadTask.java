package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.model.TransferHistoryItem;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tác vụ tải tệp hỗ trợ <b>Pause / Resume</b> dựa trên tệp tạm {@code .part},
 * hỗ trợ kết nối <b>Host:Port (Ngrok/Playit)</b> và xác thực <b>Security PIN (4 số)</b>.
 */
public class ResumableDownloadTask extends Task<Boolean> {

    private static final Logger LOG = Logger.getLogger(ResumableDownloadTask.class.getName());
    private static final DateTimeFormatter HISTORY_TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 15_000;

    private final SharedFile targetFile;
    private final String targetHost;
    private final int targetPort;
    private final String authPin;
    private final Path downloadDirectory;
    private final Consumer<String> eventLogger;
    private final DoubleConsumer speedSampleCallback;
    private final Consumer<TransferHistoryItem> historyCallback;

    private volatile boolean isPaused = false;
    private volatile boolean isCanceledByUser = false;

    public ResumableDownloadTask(SharedFile targetFile, String targetEndpoint, Consumer<String> eventLogger) {
        this(targetFile, targetEndpoint, "", eventLogger, null, null);
    }

    public ResumableDownloadTask(
            SharedFile targetFile,
            String targetEndpoint,
            String authPin,
            Consumer<String> eventLogger,
            DoubleConsumer speedSampleCallback,
            Consumer<TransferHistoryItem> historyCallback
    ) {
        this.targetFile = Objects.requireNonNull(targetFile, "targetFile");
        Peer.HostPort hp = Peer.parseHostPort(targetEndpoint, P2PServer.DEFAULT_PORT);
        this.targetHost = hp.host();
        this.targetPort = hp.port();
        this.authPin = (authPin == null) ? "" : authPin.trim();
        this.downloadDirectory = Path.of(P2PServer.DOWNLOADS_DIR_NAME).toAbsolutePath();
        this.eventLogger = eventLogger;
        this.speedSampleCallback = speedSampleCallback;
        this.historyCallback = historyCallback;
    }

    public void pauseDownload() {
        this.isPaused = true;
    }

    public boolean isPaused() {
        return isPaused;
    }

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
        String endpointStr = targetPort == P2PServer.DEFAULT_PORT ? targetHost : (targetHost + ":" + targetPort);
        long startNanoOverall = System.nanoTime();
        long sessionBytesRead = 0L;

        try {
            Files.createDirectories(downloadDirectory);

            long existingBytes = partFile.exists() ? partFile.length() : 0L;
            if (existingBytes > expectedTotalSize && expectedTotalSize > 0) {
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
                        safeFileName, targetFile.formattedSize(), endpointStr));
            }

            long bytesRemainingToRequest = Math.max(expectedTotalSize - existingBytes, 0L);
            String serverSha256 = targetFile.sha256Hash();

            if (bytesRemainingToRequest > 0 || expectedTotalSize == 0) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(targetHost, targetPort), CONNECT_TIMEOUT_MS);
                    socket.setSoTimeout(READ_TIMEOUT_MS);
                    socket.setTcpNoDelay(true);

                    try (DataOutputStream dos = new DataOutputStream(
                            new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                         DataInputStream dis = new DataInputStream(
                                 new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE))) {

                        // Gửi yêu cầu DOWNLOAD_CHUNK kèm startOffset, length và mã PIN xác thực.
                        dos.writeUTF(P2PServer.CMD_DOWNLOAD_CHUNK);
                        dos.writeUTF(safeFileName);
                        dos.writeLong(existingBytes);
                        dos.writeLong(bytesRemainingToRequest);
                        dos.writeUTF(authPin);
                        dos.flush();

                        long remoteTotalSize = dis.readLong();
                        if (remoteTotalSize == P2PServer.CODE_AUTH_FAILED) {
                            String authStatus = dis.readUTF();
                            updateProgress(0.0, 1.0);
                            String msg = "Từ chối truy cập (" + authStatus + "): Sai mã PIN bảo mật của Peer " + endpointStr + "!";
                            updateMessage("Trạng thái: " + msg);
                            logEvent(msg);
                            recordHistory(safeFileName, expectedTotalSize, endpointStr, 0.0, 1L, "Lỗi: Sai mã PIN (AUTH_FAILED)");
                            return false;
                        }
                        if (remoteTotalSize < 0) {
                            updateProgress(0.0, 1.0);
                            updateMessage("Lỗi: Tệp \"" + safeFileName + "\" không tồn tại trên Peer " + endpointStr);
                            logEvent("Lỗi: Peer " + endpointStr + " báo không tìm thấy tệp \"" + safeFileName + "\"");
                            return false;
                        }

                        long chunkLengthToRead = dis.readLong();
                        String remoteSha = dis.readUTF();
                        if (serverSha256 == null || serverSha256.isBlank()) {
                            serverSha256 = remoteSha;
                        }

                        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
                        long currentTotalDownloaded = existingBytes;
                        long sessionStartNano = System.nanoTime();

                        try (RandomAccessFile raf = new RandomAccessFile(partFile, "rw")) {
                            raf.seek(existingBytes);

                            long remaining = chunkLengthToRead;
                            while (remaining > 0) {
                                if (isPaused) {
                                    reportSpeed(0.0);
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
                                    reportSpeed(0.0);
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

                                raf.write(buffer, 0, read);

                                currentTotalDownloaded += read;
                                sessionBytesRead += read;
                                remaining -= read;

                                double speedMBps = calculateSpeedMBps(sessionBytesRead, sessionStartNano);
                                reportSpeed(speedMBps);
                                int pct = remoteTotalSize > 0
                                        ? (int) ((currentTotalDownloaded * 100) / remoteTotalSize) : 100;
                                updateProgress(currentTotalDownloaded, Math.max(remoteTotalSize, 1));
                                updateMessage(String.format(Locale.US,
                                        "Đang tải: %s (%d%% - %.2f MB/s)",
                                        safeFileName, pct, speedMBps));
                            }
                        }

                        if (currentTotalDownloaded < remoteTotalSize) {
                            reportSpeed(0.0);
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

            reportSpeed(0.0);
            updateMessage("Đang kiểm tra mã băm SHA-256 cho tệp " + partFile.getName() + "...");
            String actualSha256 = HashUtil.calculateSHA256(partFile);
            long durationMs = Math.max(1L, (System.nanoTime() - startNanoOverall) / 1_000_000L);
            double avgSpeed = calculateSpeedMBps(Math.max(sessionBytesRead, expectedTotalSize), startNanoOverall);

            if (serverSha256 != null && !serverSha256.isBlank()
                    && !actualSha256.equalsIgnoreCase(serverSha256)) {
                Files.deleteIfExists(partFile.toPath());
                updateProgress(0.0, 1.0);
                String err = "Cảnh báo: File bị hỏng trong quá trình truyền (Sai lệch SHA-256)!";
                updateMessage(err);
                logEvent(err + " [Mong đợi: " + HashUtil.shortHash(serverSha256)
                        + ", Thực tế: " + HashUtil.shortHash(actualSha256) + "]");
                recordHistory(safeFileName, expectedTotalSize, endpointStr, avgSpeed, durationMs, "Lỗi SHA-256");
                return false;
            }

            File finalFile = P2PServer.resolveOutputFile(downloadDirectory, safeFileName);
            Files.move(partFile.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            updateProgress(1.0, 1.0);
            String okMsg = "Tải hoàn tất! SHA-256 hợp lệ: [" + HashUtil.shortHash(actualSha256)
                    + "] -> " + finalFile.getName();
            updateMessage(okMsg);
            logEvent(okMsg);
            recordHistory(finalFile.getName(), expectedTotalSize, endpointStr, avgSpeed, durationMs,
                    "Hợp lệ [" + HashUtil.shortHash(actualSha256) + "]");
            return true;

        } catch (IOException e) {
            reportSpeed(0.0);
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
                    "Nhận (Pull/Resume)",
                    avgSpeedMBps,
                    durationMs,
                    shaStatus
            ));
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
