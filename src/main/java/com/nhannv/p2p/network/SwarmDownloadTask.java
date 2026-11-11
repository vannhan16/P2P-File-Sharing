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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tác vụ tải tệp Đa Nguồn Song Song (<b>Multi-source / Swarm Download</b>),
 * hỗ trợ kết nối {@code host:port}, xác thực mã PIN 4 chữ số, biểu đồ tốc độ thời gian thực
 * và ghi nhận lịch sử thực nghiệm {@link TransferHistoryItem}.
 */
public class SwarmDownloadTask extends Task<Boolean> {

    private static final Logger LOG = Logger.getLogger(SwarmDownloadTask.class.getName());
    private static final DateTimeFormatter HISTORY_TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 15_000;

    private final SharedFile targetFile;
    private final List<String> peerEndpoints;
    private final String authPin;
    private final Path downloadDirectory;
    private final Consumer<String> eventLogger;
    private final DoubleConsumer speedSampleCallback;
    private final Consumer<TransferHistoryItem> historyCallback;

    private final long[] partProgressBytes;

    private volatile boolean isPaused = false;
    private volatile boolean isCanceledByUser = false;
    private volatile ExecutorService swarmExecutor;

    public SwarmDownloadTask(
            SharedFile targetFile,
            List<String> peerEndpoints,
            long[] existingPartProgress,
            Consumer<String> eventLogger
    ) {
        this(targetFile, peerEndpoints, "", existingPartProgress, eventLogger, null, null);
    }

    public SwarmDownloadTask(
            SharedFile targetFile,
            List<String> peerEndpoints,
            String authPin,
            long[] existingPartProgress,
            Consumer<String> eventLogger,
            DoubleConsumer speedSampleCallback,
            Consumer<TransferHistoryItem> historyCallback
    ) {
        this.targetFile = Objects.requireNonNull(targetFile, "targetFile");
        if (peerEndpoints == null || peerEndpoints.isEmpty()) {
            throw new IllegalArgumentException("Danh sách peerEndpoints không được rỗng");
        }
        this.peerEndpoints = List.copyOf(peerEndpoints);
        this.authPin = (authPin == null) ? "" : authPin.trim();
        this.downloadDirectory = Path.of(P2PServer.DOWNLOADS_DIR_NAME).toAbsolutePath();
        this.eventLogger = eventLogger;
        this.speedSampleCallback = speedSampleCallback;
        this.historyCallback = historyCallback;

        if (existingPartProgress != null && existingPartProgress.length == this.peerEndpoints.size()) {
            this.partProgressBytes = existingPartProgress.clone();
        } else {
            this.partProgressBytes = new long[this.peerEndpoints.size()];
        }
    }

    public synchronized long[] getPartProgressSnapshot() {
        return partProgressBytes.clone();
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
        ExecutorService exec = this.swarmExecutor;
        if (exec != null) {
            exec.shutdownNow();
        }
    }

    public Path getPartFilePath() {
        String safeFileName = Path.of(targetFile.fileName()).getFileName().toString();
        return downloadDirectory.resolve(safeFileName + ".part");
    }

    @Override
    protected Boolean call() {
        String safeFileName = Path.of(targetFile.fileName()).getFileName().toString();
        File partFile = getPartFilePath().toFile();
        long totalSize = targetFile.fileSize();
        int peerCount = peerEndpoints.size();
        String peersSummary = peerCount + " Peers " + peerEndpoints;

        try {
            Files.createDirectories(downloadDirectory);

            try (RandomAccessFile preAllocRaf = new RandomAccessFile(partFile, "rw")) {
                if (preAllocRaf.length() != totalSize) {
                    preAllocRaf.setLength(totalSize);
                }
            }

            long alreadyDownloaded = 0L;
            for (long done : partProgressBytes) {
                alreadyDownloaded += done;
            }

            final AtomicLong totalBytesDownloaded = new AtomicLong(alreadyDownloaded);
            final AtomicLong sessionBytesDownloaded = new AtomicLong(0L);
            final long startNano = System.nanoTime();

            logEvent(String.format(Locale.US,
                    "Bắt đầu tải đa nguồn (Swarm) \"%s\" (%s) từ %d nguồn: %s...",
                    safeFileName, targetFile.formattedSize(), peerCount, peerEndpoints));

            AtomicInteger workerIdx = new AtomicInteger(1);
            ExecutorService executor = Executors.newFixedThreadPool(peerCount, r -> {
                Thread t = new Thread(r, "p2p-swarm-worker-" + workerIdx.getAndIncrement());
                t.setDaemon(true);
                return t;
            });
            this.swarmExecutor = executor;

            long basePartSize = totalSize / peerCount;
            List<Callable<Boolean>> partTasks = new ArrayList<>(peerCount);

            for (int i = 0; i < peerCount; i++) {
                final int partIndex = i;
                final String peerEndpoint = peerEndpoints.get(i);
                final long partStartOffset = i * basePartSize;
                final long partTotalLength = (i == peerCount - 1)
                        ? (totalSize - partStartOffset)
                        : basePartSize;

                partTasks.add(() -> downloadSinglePart(
                        partIndex,
                        peerEndpoint,
                        safeFileName,
                        partFile,
                        partStartOffset,
                        partTotalLength,
                        totalSize,
                        peerCount,
                        totalBytesDownloaded,
                        sessionBytesDownloaded,
                        startNano
                ));
            }

            List<Future<Boolean>> futures = executor.invokeAll(partTasks);
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            reportSpeed(0.0);

            if (isPaused) {
                long currentDone = totalBytesDownloaded.get();
                double pct = totalSize > 0 ? (currentDone * 100.0) / totalSize : 0.0;
                String pauseMsg = String.format(Locale.US,
                        "Tạm dừng tải đa nguồn tại %.1f%% (%s / %s) - offset tổng=%d",
                        pct, SharedFile.formatSize(currentDone), SharedFile.formatSize(totalSize), currentDone);
                updateMessage("Trạng thái: " + pauseMsg);
                logEvent(pauseMsg);
                return false;
            }

            if (isCanceledByUser || isCancelled()) {
                Files.deleteIfExists(partFile.toPath());
                updateProgress(0.0, 1.0);
                updateMessage("Trạng thái: Đã hủy tải đa nguồn và xóa tệp tạm " + partFile.getName());
                logEvent("Đã hủy tải đa nguồn \"" + safeFileName + "\" và xóa file .part.");
                return false;
            }

            for (Future<Boolean> future : futures) {
                if (!Boolean.TRUE.equals(future.get())) {
                    long currentDone = totalBytesDownloaded.get();
                    double pct = totalSize > 0 ? (currentDone * 100.0) / totalSize : 0.0;
                    String msg = String.format(Locale.US,
                            "Tải đa nguồn bị gián đoạn tại %.1f%%. Bấm 'Tiếp tục (Resume)' để tải tiếp phần còn thiếu!",
                            pct);
                    updateMessage("Trạng thái: " + msg);
                    logEvent(msg);
                    return false;
                }
            }

            updateMessage("Đang xác thực SHA-256 toàn bộ tệp đa nguồn " + partFile.getName() + "...");
            String actualSha256 = HashUtil.calculateSHA256(partFile);
            String expectedSha256 = targetFile.sha256Hash();
            long durationMs = Math.max(1L, (System.nanoTime() - startNano) / 1_000_000L);
            double avgSpeed = calculateSpeedMBps(Math.max(sessionBytesDownloaded.get(), totalSize), startNano);

            if (expectedSha256 != null && !expectedSha256.isBlank()
                    && !actualSha256.equalsIgnoreCase(expectedSha256)) {
                Files.deleteIfExists(partFile.toPath());
                updateProgress(0.0, 1.0);
                String err = "Cảnh báo: File đa nguồn bị hỏng (Sai lệch SHA-256)!";
                updateMessage(err);
                logEvent(err);
                recordHistory(safeFileName, totalSize, peersSummary, avgSpeed, durationMs, "Lỗi SHA-256");
                return false;
            }

            File finalFile = P2PServer.resolveOutputFile(downloadDirectory, safeFileName);
            Files.move(partFile.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            updateProgress(1.0, 1.0);
            String okMsg = String.format(Locale.US,
                    "Tải đa nguồn hoàn tất từ %d Peers (%.2f MB/s)! SHA-256 hợp lệ: [%s] -> %s",
                    peerCount, avgSpeed, HashUtil.shortHash(actualSha256), finalFile.getName());
            updateMessage(okMsg);
            logEvent(okMsg);
            recordHistory(finalFile.getName(), totalSize, peersSummary, avgSpeed, durationMs,
                    "Hợp lệ [" + HashUtil.shortHash(actualSha256) + "]");
            return true;

        } catch (Exception e) {
            reportSpeed(0.0);
            if (isCanceledByUser || isCancelled()) {
                try {
                    Files.deleteIfExists(partFile.toPath());
                } catch (IOException ignored) {
                }
                return false;
            }
            LOG.log(Level.WARNING, "Lỗi trong SwarmDownloadTask: " + safeFileName, e);
            updateMessage("Trạng thái: Lỗi tải đa nguồn - " + e.getMessage());
            logEvent("Lỗi tải đa nguồn \"" + safeFileName + "\": " + e.getMessage());
            return false;
        }
    }

    private Boolean downloadSinglePart(
            int partIndex,
            String peerEndpoint,
            String safeFileName,
            File partFile,
            long partStartOffset,
            long partTotalLength,
            long totalFileSize,
            int peerCount,
            AtomicLong totalBytesDownloaded,
            AtomicLong sessionBytesDownloaded,
            long startNano
    ) {
        long doneInPart;
        synchronized (this) {
            doneInPart = partProgressBytes[partIndex];
        }
        long remainingInPart = partTotalLength - doneInPart;
        if (remainingInPart <= 0) {
            return true;
        }

        Peer.HostPort hp = Peer.parseHostPort(peerEndpoint, P2PServer.DEFAULT_PORT);
        long requestOffset = partStartOffset + doneInPart;
        logEvent(String.format(Locale.US,
                " -> [Part %d/%d] Kết nối Peer %s xin đoạn [offset=%d, length=%d bytes]",
                partIndex + 1, peerCount, hp.toEndpointString(), requestOffset, remainingInPart));

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(hp.host(), hp.port()), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            socket.setTcpNoDelay(true);

            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                 DataInputStream dis = new DataInputStream(
                         new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE))) {

                dos.writeUTF(P2PServer.CMD_DOWNLOAD_CHUNK);
                dos.writeUTF(safeFileName);
                dos.writeLong(requestOffset);
                dos.writeLong(remainingInPart);
                dos.writeUTF(authPin);
                dos.flush();

                long remoteTotalSize = dis.readLong();
                if (remoteTotalSize == P2PServer.CODE_AUTH_FAILED) {
                    String status = dis.readUTF();
                    logEvent(" -> [Part " + (partIndex + 1) + "] Từ chối truy cập (" + status
                            + "): Sai mã PIN bảo mật trên Peer " + hp.toEndpointString());
                    return false;
                }
                if (remoteTotalSize < 0) {
                    return false;
                }
                long actualChunkLen = dis.readLong();
                dis.readUTF(); // SHA-256 header

                byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
                try (RandomAccessFile raf = new RandomAccessFile(partFile, "rw")) {
                    raf.seek(requestOffset);

                    long left = actualChunkLen;
                    while (left > 0) {
                        if (isPaused || isCanceledByUser || isCancelled()) {
                            return false;
                        }
                        int maxToRead = (int) Math.min(buffer.length, left);
                        int read = dis.read(buffer, 0, maxToRead);
                        if (read == -1) {
                            return false;
                        }

                        raf.write(buffer, 0, read);
                        left -= read;

                        synchronized (this) {
                            partProgressBytes[partIndex] += read;
                        }

                        long totalDone = totalBytesDownloaded.addAndGet(read);
                        long sessionDone = sessionBytesDownloaded.addAndGet(read);
                        double speedMBps = calculateSpeedMBps(sessionDone, startNano);
                        reportSpeed(speedMBps);

                        updateProgress(totalDone, Math.max(totalFileSize, 1));
                        updateMessage(String.format(Locale.US,
                                "Đang tải đa nguồn từ %d Peers (%.2f MB/s)",
                                peerCount, speedMBps));
                    }
                }
                logEvent(String.format(Locale.US,
                        " -> [Part %d/%d] Hoàn tất phân đoạn từ Peer %s (%s)",
                        partIndex + 1, peerCount, hp.toEndpointString(), SharedFile.formatSize(partTotalLength)));
                return true;
            }
        } catch (IOException e) {
            if (!isPaused && !isCanceledByUser) {
                logEvent(String.format(Locale.US,
                        " -> [Part %d/%d] Lỗi kết nối với Peer %s: %s",
                        partIndex + 1, peerCount, hp.toEndpointString(), e.getMessage()));
            }
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
                    "Nhận (Swarm Multi-source)",
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
