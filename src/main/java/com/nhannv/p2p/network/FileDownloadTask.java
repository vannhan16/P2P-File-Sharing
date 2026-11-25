package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.util.BandwidthLimiter;
import com.nhannv.p2p.util.HashUtil;
import com.nhannv.p2p.util.PathUtil;
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
 * Tác vụ tải tệp cũ (Legacy Download Task).
 * <p>
 * <b>Ghi chú Kiến trúc (B11):</b> Lớp này là phiên bản sơ khai từ Milestone 3, không hỗ trợ
 * {@code .part}, không hỗ trợ Resume và không được quản lý bởi {@link DownloadManager}.
 * Toàn bộ tính năng tải tệp hiện đại của hệ thống đã chuyển sang {@link ResumableDownloadTask}
 * và {@link SwarmDownloadTask}.
 *
 * @deprecated Mã nguồn cũ giữ lại làm tài liệu đối chiếu (Legacy Reference), không sử dụng trong luồng chạy chính.
 */
@Deprecated(since = "legacy", forRemoval = false)
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

                    dos.writeUTF(P2PServer.CMD_DOWNLOAD_FILE);
                    dos.writeUTF(PathUtil.sanitizeFileName(fileName));
                    dos.flush();

                    if (!P2PServer.performClientAuth(dis, dos, authPin, P2PServer.CMD_DOWNLOAD_FILE)) {
                        updateProgress(0.0, 1.0);
                        updateMessage("Từ chối truy cập: Sai mã PIN bảo mật!");
                        return false;
                    }

                    long totalBytes = dis.readLong();
                    if (totalBytes < 0) {
                        updateProgress(0.0, 1.0);
                        updateMessage("Lỗi: Tệp \"" + fileName + "\" không tồn tại trên Peer!");
                        return false;
                    }

                    String expectedSha256 = dis.readUTF();
                    outputFile = PathUtil.resolveSafeOutputFile(downloadDirectory, fileName);

                    MessageDigest digest = HashUtil.newSha256Digest();
                    byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
                    long totalBytesRead = 0;
                    long startNano = System.nanoTime();

                    try (BufferedOutputStream bos = new BufferedOutputStream(
                            new FileOutputStream(outputFile), CHUNK_BUFFER_SIZE)) {

                        long remaining = totalBytes;
                        while (remaining > 0) {
                            if (isCancelled()) {
                                updateMessage("Trạng thái: Đã hủy tải tệp \"" + fileName + "\"");
                                bos.close();
                                Files.deleteIfExists(outputFile.toPath());
                                return false;
                            }

                            int maxToRead = (int) Math.min(buffer.length, remaining);
                            int bytesRead = dis.read(buffer, 0, maxToRead);
                            if (bytesRead == -1) {
                                break;
                            }

                            bos.write(buffer, 0, bytesRead);
                            digest.update(buffer, 0, bytesRead);
                            BandwidthLimiter.getInstance().throttle(bytesRead);

                            totalBytesRead += bytesRead;
                            remaining -= bytesRead;

                            updateProgress(totalBytesRead, Math.max(totalBytes, 1));
                            double elapsedSec = (System.nanoTime() - startNano) / 1_000_000_000.0;
                            double speedMbps = elapsedSec > 0.0001 ? (totalBytesRead / (1024.0 * 1024.0)) / elapsedSec : 0.0;
                            updateMessage(String.format(Locale.US, "Đang tải: %s (%.2f MB/s)", fileName, speedMbps));
                        }
                        bos.flush();
                    }

                    if (totalBytesRead != totalBytes) {
                        Files.deleteIfExists(outputFile.toPath());
                        throw new EOFException("Dữ liệu nhận chưa hoàn chỉnh");
                    }

                    String actualSha256 = HashUtil.toHexString(digest.digest());
                    if (!expectedSha256.isBlank() && !expectedSha256.equalsIgnoreCase(actualSha256)) {
                        Files.deleteIfExists(outputFile.toPath());
                        updateMessage("Cảnh báo: File bị hỏng (Sai lệch SHA-256)!");
                        return false;
                    }

                    updateProgress(1.0, 1.0);
                    updateMessage("Tải hoàn tất! SHA-256: [" + HashUtil.shortHash(actualSha256) + "]");
                    return true;
                }
            }
        } catch (Exception e) {
            if (outputFile != null && outputFile.exists()) {
                try {
                    Files.deleteIfExists(outputFile.toPath());
                } catch (IOException ignored) {
                }
            }
            updateMessage("Trạng thái: Lỗi tải tệp - " + e.getMessage());
            return false;
        }
    }
}
