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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tác vụ tải tệp Đa Nguồn Song Song (<b>Multi-source / Swarm Download</b>) tương tự BitTorrent/IDM.
 *
 * <h2>Giải thích Cơ chế Chia Phần (Multi-source Partitioning) & Ghi File Song Song cho vấn đáp đồ án</h2>
 * Khi một tệp có cùng mã {@code sha256Hash} xuất hiện trên {@code N} Peer khác nhau:
 * <ol>
 *   <li><b>Cấp phát trước không gian đĩa ({@code RandomAccessFile.setLength(fileSize)}):</b>
 *       Trước khi các luồng bắt đầu tải, ta tạo tệp tạm {@code <fileName>.part} và gọi
 *       {@code raf.setLength(fileSize)} để hệ điều hành cấp phát ngay một khung tệp rỗng có độ dài
 *       đúng bằng kích thước tệp gốc.</li>
 *   <li><b>Thuật toán chia tệp thành N phần không giao nhau (Non-overlapping Byte Ranges):</b>
 *       Giả sử tệp nặng {@code totalSize} byte và có {@code N} Peer:
 *       <ul>
 *         <li>Kích thước chuẩn mỗi phần: {@code basePartSize = totalSize / N}.</li>
 *         <li>Phần thứ {@code i} (với {@code i} chạy từ {@code 0} đến {@code N - 1}) có:
 *           <pre>
 *           startOffset[i] = i * basePartSize
 *           length[i]      = (i == N - 1) ? (totalSize - startOffset[i]) : basePartSize
 *           </pre>
 *           <i>(Phần cuối cùng {@code i = N - 1} ôm trọn toàn bộ số byte lẻ còn dư của phép chia nguyên
 *           để tổng các phần luôn bằng chính xác 100% {@code totalSize}).</i>
 *         </li>
 *       </ul>
 *   </li>
 *   <li><b>Ghi song song không xung đột (Lock-free Parallel Disk Write):</b>
 *       Mỗi luồng trong {@link ExecutorService} mở <b>một instance {@link RandomAccessFile} riêng biệt</b>
 *       cùng trỏ vào file {@code <fileName>.part}, rồi gọi {@code raf.seek(startOffset[i] + partProgress[i])}.
 *       Vì miền byte {@code [startOffset[i], startOffset[i] + length[i])} của các luồng hoàn toàn
 *       tách rời nhau (không chồng lấn), các luồng có thể ghi xuống đĩa đồng thời ở tốc độ tối đa
 *       mà không bao giờ ghi đè lên dữ liệu của nhau!</li>
 *   <li><b>Tổng hợp tiến độ bằng {@link AtomicLong}:</b>
 *       Biến đếm nguyên tử {@code totalBytesDownloaded.addAndGet(read)} cộng dồn số byte từ cả {@code N}
 *       luồng theo thời gian thực để cập nhật lên thanh {@code ProgressBar} kèm thông báo:
 *       {@code "Đang tải đa nguồn từ X Peers (Y MB/s)"}.</li>
 * </ol>
 */
public class SwarmDownloadTask extends Task<Boolean> {

    private static final Logger LOG = Logger.getLogger(SwarmDownloadTask.class.getName());

    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 15_000;

    private final SharedFile targetFile;
    private final List<String> peerIps;
    private final int targetPort;
    private final Path downloadDirectory;
    private final Consumer<String> eventLogger;

    /** Tiến độ từng phần để hỗ trợ Pause/Resume ngay cả khi đang tải đa nguồn (Swarm). */
    private final long[] partProgressBytes;

    private volatile boolean isPaused = false;
    private volatile boolean isCanceledByUser = false;
    private volatile ExecutorService swarmExecutor;

    public SwarmDownloadTask(
            SharedFile targetFile,
            List<String> peerIps,
            long[] existingPartProgress,
            Consumer<String> eventLogger
    ) {
        this(targetFile, peerIps, P2PServer.DEFAULT_PORT, existingPartProgress, eventLogger);
    }

    public SwarmDownloadTask(
            SharedFile targetFile,
            List<String> peerIps,
            int targetPort,
            long[] existingPartProgress,
            Consumer<String> eventLogger
    ) {
        this.targetFile = Objects.requireNonNull(targetFile, "targetFile");
        if (peerIps == null || peerIps.isEmpty()) {
            throw new IllegalArgumentException("Danh sách peerIps không được rỗng");
        }
        this.peerIps = List.copyOf(peerIps);
        this.targetPort = targetPort;
        this.downloadDirectory = Path.of(P2PServer.DOWNLOADS_DIR_NAME).toAbsolutePath();
        this.eventLogger = eventLogger;

        if (existingPartProgress != null && existingPartProgress.length == this.peerIps.size()) {
            this.partProgressBytes = existingPartProgress.clone();
        } else {
            this.partProgressBytes = new long[this.peerIps.size()];
        }
    }

    /** Trả về mảng số byte đã tải của từng phần để lưu trạng thái khi bấm Pause. */
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
        int peerCount = peerIps.size();

        try {
            Files.createDirectories(downloadDirectory);

            // Bước 1: Tạo trước file <fileName>.part với kích thước bằng fileSize gốc
            // bằng RandomAccessFile.setLength(totalSize).
            try (RandomAccessFile preAllocRaf = new RandomAccessFile(partFile, "rw")) {
                if (preAllocRaf.length() != totalSize) {
                    preAllocRaf.setLength(totalSize);
                }
            }

            // Tính tổng số byte đã tải sẵn từ các phần (nếu đây là phiên Resume của Swarm).
            long alreadyDownloaded = 0L;
            for (long done : partProgressBytes) {
                alreadyDownloaded += done;
            }

            final AtomicLong totalBytesDownloaded = new AtomicLong(alreadyDownloaded);
            final AtomicLong sessionBytesDownloaded = new AtomicLong(0L);
            final long startNano = System.nanoTime();

            logEvent(String.format(Locale.US,
                    "Bắt đầu tải đa nguồn (Swarm) \"%s\" (%s) từ %d nguồn: %s...",
                    safeFileName, targetFile.formattedSize(), peerCount, peerIps));

            // Bước 2: Chia tổng kích thước file thành N phần (Part 1..Part N)
            // và mở ExecutorService gồm N luồng song song.
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
                final String peerIp = peerIps.get(i);
                final long partStartOffset = i * basePartSize;
                // Phần cuối cùng (i == peerCount - 1) nhận toàn bộ phần dư còn lại.
                final long partTotalLength = (i == peerCount - 1)
                        ? (totalSize - partStartOffset)
                        : basePartSize;

                partTasks.add(() -> downloadSinglePart(
                        partIndex,
                        peerIp,
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

            // Bước 5: Khi cả N phần đã tải xong 100%, băm SHA-256 toàn bộ file .part để xác thực.
            updateMessage("Đang xác thực SHA-256 toàn bộ tệp đa nguồn " + partFile.getName() + "...");
            String actualSha256 = HashUtil.calculateSHA256(partFile);
            String expectedSha256 = targetFile.sha256Hash();

            if (expectedSha256 != null && !expectedSha256.isBlank()
                    && !actualSha256.equalsIgnoreCase(expectedSha256)) {
                Files.deleteIfExists(partFile.toPath());
                updateProgress(0.0, 1.0);
                String err = "Cảnh báo: File đa nguồn bị hỏng (Sai lệch SHA-256)!";
                updateMessage(err);
                logEvent(err);
                return false;
            }

            File finalFile = P2PServer.resolveOutputFile(downloadDirectory, safeFileName);
            Files.move(partFile.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            updateProgress(1.0, 1.0);
            double avgSpeed = calculateSpeedMBps(sessionBytesDownloaded.get(), startNano);
            String okMsg = String.format(Locale.US,
                    "Tải đa nguồn hoàn tất từ %d Peers (%.2f MB/s)! SHA-256 hợp lệ: [%s] -> %s",
                    peerCount, avgSpeed, HashUtil.shortHash(actualSha256), finalFile.getName());
            updateMessage(okMsg);
            logEvent(okMsg);
            return true;

        } catch (Exception e) {
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

    /**
     * Hàm chạy trên từng luồng con của {@code ExecutorService}:
     * Kết nối tới 1 Peer, xin đúng phân đoạn {@code [partStartOffset + doneInPart, remainingInPart]}
     * qua lệnh {@code DOWNLOAD_CHUNK}, và mở một instance {@link RandomAccessFile} độc lập
     * để {@code seek()} thẳng tới vùng byte của phần đó trên file {@code .part}.
     */
    private Boolean downloadSinglePart(
            int partIndex,
            String peerIp,
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
            return true; // Phần này đã tải xong từ trước.
        }

        long requestOffset = partStartOffset + doneInPart;
        logEvent(String.format(Locale.US,
                " -> [Part %d/%d] Kết nối Peer %s xin đoạn [offset=%d, length=%d bytes]",
                partIndex + 1, peerCount, peerIp, requestOffset, remainingInPart));

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(peerIp, targetPort), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            socket.setTcpNoDelay(true);

            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                 DataInputStream dis = new DataInputStream(
                         new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE))) {

                // Gửi lệnh DOWNLOAD_CHUNK cho phân đoạn được giao.
                dos.writeUTF(P2PServer.CMD_DOWNLOAD_CHUNK);
                dos.writeUTF(safeFileName);
                dos.writeLong(requestOffset);
                dos.writeLong(remainingInPart);
                dos.flush();

                long remoteTotalSize = dis.readLong();
                if (remoteTotalSize < 0) {
                    return false;
                }
                long actualChunkLen = dis.readLong();
                dis.readUTF(); // Đọc qua chuỗi SHA-256 trong header.

                // Mở instance RandomAccessFile riêng cho luồng này, nhảy tới đúng requestOffset.
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

                        // Ghi trực tiếp vào phân vùng của Part này trên đĩa.
                        raf.write(buffer, 0, read);
                        left -= read;

                        synchronized (this) {
                            partProgressBytes[partIndex] += read;
                        }

                        long totalDone = totalBytesDownloaded.addAndGet(read);
                        long sessionDone = sessionBytesDownloaded.addAndGet(read);
                        double speedMBps = calculateSpeedMBps(sessionDone, startNano);

                        updateProgress(totalDone, Math.max(totalFileSize, 1));
                        updateMessage(String.format(Locale.US,
                                "Đang tải đa nguồn từ %d Peers (%.2f MB/s)",
                                peerCount, speedMBps));
                    }
                }
                logEvent(String.format(Locale.US,
                        " -> [Part %d/%d] Hoàn tất phân đoạn từ Peer %s (%s)",
                        partIndex + 1, peerCount, peerIp, SharedFile.formatSize(partTotalLength)));
                return true;
            }
        } catch (IOException e) {
            if (!isPaused && !isCanceledByUser) {
                logEvent(String.format(Locale.US,
                        " -> [Part %d/%d] Lỗi kết nối với Peer %s: %s",
                        partIndex + 1, peerCount, peerIp, e.getMessage()));
            }
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
