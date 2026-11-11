package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;
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
 * Tác vụ tải tệp từ một Peer khác về máy (Pull Protocol), kết hợp đo tốc độ thời gian thực (MB/s),
 * hỗ trợ {@code host:port}, mã PIN xác thực 4 chữ số và <b>On-the-fly SHA-256</b>.
 */
public class FileDownloadTask extends Task<Boolean> {

    private static final Logger LOG = Logger.getLogger(FileDownloadTask.class.getName());

    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 15_000;

    private final String targetHost;
    private final int targetPort;
    private final String fileName;
    private final String authPin;
    private final Path downloadDirectory;

    public FileDownloadTask(SharedFile remoteFile) {
        this(
                Peer.parseHostPort(Objects.requireNonNull(remoteFile, "remoteFile").ownerIp(), P2PServer.DEFAULT_PORT).host(),
                Peer.parseHostPort(remoteFile.ownerIp(), P2PServer.DEFAULT_PORT).port(),
                remoteFile.fileName(),
                ""
        );
    }

    public FileDownloadTask(String targetEndpoint, String fileName) {
        this(
                Peer.parseHostPort(targetEndpoint, P2PServer.DEFAULT_PORT).host(),
                Peer.parseHostPort(targetEndpoint, P2PServer.DEFAULT_PORT).port(),
                fileName,
                ""
        );
    }

    public FileDownloadTask(String targetHost, int targetPort, String fileName) {
        this(targetHost, targetPort, fileName, "");
    }

    public FileDownloadTask(String targetHost, int targetPort, String fileName, String authPin) {
        this.targetHost = Objects.requireNonNull(targetHost, "targetHost");
        this.targetPort = targetPort;
        this.fileName = Objects.requireNonNull(fileName, "fileName");
        this.authPin = (authPin == null) ? "" : authPin.trim();
        this.downloadDirectory = Path.of(P2PServer.DOWNLOADS_DIR_NAME).toAbsolutePath();
    }

    @Override
    protected Boolean call() {
        File outputFile = null;
        try {
            Files.createDirectories(downloadDirectory);

            updateProgress(0.0, 1.0);
            updateMessage("Đang kết nối tới Peer " + targetHost + ":" + targetPort + " để tải: " + fileName + "...");

            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(targetHost, targetPort), CONNECT_TIMEOUT_MS);
                socket.setSoTimeout(READ_TIMEOUT_MS);
                socket.setTcpNoDelay(true);

                try (DataOutputStream dos = new DataOutputStream(
                        new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                     DataInputStream dis = new DataInputStream(
                             new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE))) {

                    // Gửi Command Header "DOWNLOAD_FILE" kèm tên tệp và mã PIN xác thực.
                    dos.writeUTF(P2PServer.CMD_DOWNLOAD_FILE);
                    dos.writeUTF(fileName);
                    dos.writeUTF(authPin);
                    dos.flush();

                    long totalBytes = dis.readLong();
                    if (totalBytes == P2PServer.CODE_AUTH_FAILED) {
                        String status = dis.readUTF();
                        updateProgress(0.0, 1.0);
                        updateMessage("Từ chối truy cập (" + status + "): Sai mã PIN bảo mật của Peer!");
                        return false;
                    }
                    if (totalBytes < 0) {
                        updateProgress(0.0, 1.0);
                        updateMessage("Lỗi: Tệp \"" + fileName + "\" không còn tồn tại trên máy Peer!");
                        return false;
                    }
                    String expectedSha256 = dis.readUTF();

                    String safeFileName = Path.of(fileName).getFileName().toString();
                    outputFile = P2PServer.resolveOutputFile(downloadDirectory, safeFileName);

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

                            bos.write(buffer, 0, read);
                            sha256Digest.update(buffer, 0, read);

                            bytesReadTotal += read;
                            remaining -= read;
                            bytesInWindow += read;

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

                    if (bytesReadTotal != totalBytes) {
                        Files.deleteIfExists(outputFile.toPath());
                        updateProgress(0.0, 1.0);
                        updateMessage("Lỗi: Ngắt kết nối giữa chừng khi đang tải " + fileName);
                        return false;
                    }

                    String calculatedSha256 = HashUtil.toHexString(sha256Digest.digest());
                    if (calculatedSha256.equalsIgnoreCase(expectedSha256)) {
                        updateProgress(1.0, 1.0);
                        updateMessage("Tải hoàn tất! SHA-256 hợp lệ: [" + HashUtil.shortHash(calculatedSha256) + "]");
                        return true;
                    } else {
                        Files.deleteIfExists(outputFile.toPath());
                        updateProgress(0.0, 1.0);
                        updateMessage("Cảnh báo: File bị hỏng trong quá trình truyền (Sai lệch SHA-256)!");
                        return false;
                    }
                }
            }
        } catch (EOFException e) {
            cleanupCorruptedFile(outputFile);
            updateProgress(0.0, 1.0);
            updateMessage("Lỗi: Peer ngắt kết nối đột ngột khi đang tải " + fileName);
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
            }
        }
    }
}
