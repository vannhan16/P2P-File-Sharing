package com.nhannv.p2p.network;

import com.nhannv.p2p.model.SharedFile;
import javafx.concurrent.Task;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Luồng nền (TCP Server) chuyên lắng nghe và nhận tệp từ các Peer khác gửi tới.
 *
 * <h2>Giải thích kiến trúc cho team (Huỳnh Nhân Danh)</h2>
 * <ol>
 *   <li><b>Kế thừa {@link Task} của JavaFX (Thread Binding):</b>
 *       Toàn bộ logic mạng/đĩa trong phương thức {@link #call()} chạy trên <i>Background Daemon Thread</i>,
 *       tuyệt đối không chạm trực tiếp vào UI. Khi gọi {@code updateProgress(workDone, max)} và
 *       {@code updateMessage(text)}, JavaFX tự động đồng bộ an toàn sang <i>JavaFX Application Thread</i>
 *       (thông qua cơ chế hợp nhất sự kiện nội bộ) để cập nhật {@code ProgressBar} và {@code Label}
 *       đã được {@code bind()} trong {@code MainController}.</li>
 *   <li><b>Cơ chế cắt nhỏ tệp (Chunking - 64KB Buffer):</b>
 *       Không bao giờ dùng {@code readAllBytes()} vì tệp vài GB sẽ gây tràn bộ nhớ ({@code OutOfMemoryError}).
 *       Thay vào đó, ta dùng một mảng cố định {@code byte[65536]} (64KB): đọc từng khối từ {@code Socket}
 *       rồi xả (flush) ngay xuống {@code FileOutputStream}. Nhờ vậy gửi tệp 10GB cũng chỉ tốn ~64KB RAM.</li>
 * </ol>
 */
public class FileReceiverTask extends Task<Void> {

    private static final Logger LOG = Logger.getLogger(FileReceiverTask.class.getName());

    /** Cổng TCP mặc định để truyền tệp (tách biệt với cổng UDP 8888 của Discovery). */
    public static final int DEFAULT_PORT = 8889;

    /**
     * Kích thước mỗi khối dữ liệu (Chunk size) = 64 KB.
     * 64KB là điểm cân bằng tối ưu cho mạng LAN Gigabit: đủ lớn để giảm số lần gọi hệ thống (syscall)
     * đọc/ghi đĩa và mạng, nhưng đủ nhỏ để thanh ProgressBar cập nhật mượt mà và không tốn RAM.
     */
    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;

    /** Thư mục mặc định lưu tệp nhận được (nằm ngay tại thư mục gốc của dự án). */
    public static final String DOWNLOADS_DIR_NAME = "Downloads";

    private final int port;
    private final Path downloadDirectory;
    private final Consumer<SharedFile> onFileReceivedCallback;
    private volatile ServerSocket serverSocket;

    public FileReceiverTask() {
        this(DEFAULT_PORT, null);
    }

    public FileReceiverTask(int port, Consumer<SharedFile> onFileReceivedCallback) {
        this.port = port;
        this.downloadDirectory = Path.of(DOWNLOADS_DIR_NAME).toAbsolutePath();
        this.onFileReceivedCallback = onFileReceivedCallback;
    }

    @Override
    protected Void call() {
        try {
            // Đảm bảo thư mục Downloads luôn tồn tại trước khi nhận tệp.
            Files.createDirectories(downloadDirectory);

            ServerSocket ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(port));
            this.serverSocket = ss;

            updateProgress(0.0, 1.0);
            updateMessage("Trạng thái: Sẵn sàng (Đang lắng nghe tệp đến trên TCP port " + port + ")");
            LOG.info(() -> "FileReceiverTask đang lắng nghe trên TCP port " + port);

            // Vòng lặp vô tận chờ các Peer kết nối tới để gửi tệp.
            while (!isCancelled() && !ss.isClosed()) {
                try (Socket clientSocket = ss.accept()) {
                    receiveSingleFile(clientSocket);
                } catch (SocketException e) {
                    // Khi ứng dụng đóng, stopServer() sẽ đóng ServerSocket làm accept() ném SocketException.
                    if (isCancelled() || ss.isClosed()) {
                        break;
                    }
                    LOG.log(Level.WARNING, "Lỗi kết nối Socket khi đang chờ nhận tệp", e);
                    updateMessage("Trạng thái: Lỗi kết nối khi nhận tệp - " + e.getMessage());
                } catch (EOFException e) {
                    LOG.log(Level.WARNING, "Peer ngắt kết nối đột ngột trước khi gửi xong", e);
                    updateProgress(0.0, 1.0);
                    updateMessage("Trạng thái: Lỗi - Peer ngắt kết nối đột ngột khi đang truyền tệp!");
                } catch (IOException e) {
                    LOG.log(Level.WARNING, "Lỗi I/O trong quá trình nhận tệp", e);
                    updateProgress(0.0, 1.0);
                    updateMessage("Trạng thái: Lỗi nhận tệp - " + e.getMessage());
                }
            }
        } catch (BindException e) {
            LOG.log(Level.SEVERE, "Cổng TCP " + port + " đã bị chiếm dụng", e);
            updateMessage("Trạng thái: Lỗi - Cổng TCP " + port + " đang bị sử dụng bởi tiến trình khác!");
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Không thể khởi tạo TCP Server nhận tệp", e);
            updateMessage("Trạng thái: Lỗi khởi tạo bộ nhận tệp - " + e.getMessage());
        } finally {
            closeServerSocket();
        }
        return null;
    }

    /**
     * Xử lý trọn vẹn một phiên nhận tệp từ một kết nối {@link Socket}.
     * Giao thức tầng ứng dụng (Application Framing):
     * <pre>[UTF-8 FileName (2-byte length + bytes)] + [8-byte long FileSize] + [Raw File Bytes]</pre>
     */
    private void receiveSingleFile(Socket clientSocket) throws IOException {
        String senderIp = clientSocket.getInetAddress().getHostAddress();
        // Đặt timeout đọc 15 giây để không bị treo vĩnh viễn nếu bên gửi rút dây mạng giữa chừng.
        clientSocket.setSoTimeout(15_000);

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(clientSocket.getInputStream(), CHUNK_BUFFER_SIZE))) {

            // Bước 1: Đọc metadata gồm tên tệp và kích thước tệp.
            String rawFileName = dis.readUTF();
            long totalBytes = dis.readLong();

            if (totalBytes < 0) {
                throw new IOException("Kích thước tệp không hợp lệ: " + totalBytes);
            }

            // Chống lỗ hổng Path Traversal (vd: tên tệp chứa "../../"): chỉ lấy phần tên tệp đơn thuần.
            String safeFileName = Path.of(rawFileName).getFileName().toString();
            File outputFile = resolveOutputFile(downloadDirectory, safeFileName);

            updateProgress(0, Math.max(totalBytes, 1));
            updateMessage("Trạng thái: Đang nhận \"" + outputFile.getName() + "\" từ " + senderIp + "...");

            // Bước 2 & 3: CHUNKING - Đọc từng khối 64KB từ mạng và ghi ngay xuống ổ cứng.
            byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
            long totalBytesRead = 0;
            long startTimeNano = System.nanoTime();

            try (BufferedOutputStream bos = new BufferedOutputStream(
                    new FileOutputStream(outputFile), CHUNK_BUFFER_SIZE)) {

                long remaining = totalBytes;
                while (remaining > 0 && !isCancelled()) {
                    int maxToRead = (int) Math.min(buffer.length, remaining);
                    int bytesRead = dis.read(buffer, 0, maxToRead);
                    if (bytesRead == -1) {
                        // Luồng kết thúc sớm hơn kích thước đã khai báo -> đứt kết nối giữa chừng!
                        break;
                    }
                    bos.write(buffer, 0, bytesRead);
                    totalBytesRead += bytesRead;
                    remaining -= bytesRead;

                    // Bước 4: Cập nhật tiến độ và tốc độ truyền về UI qua cơ chế Thread Binding.
                    updateProgress(totalBytesRead, totalBytes);
                    double speedMbps = calculateSpeedMBps(totalBytesRead, startTimeNano);
                    int percent = totalBytes > 0 ? (int) ((totalBytesRead * 100) / totalBytes) : 100;
                    updateMessage(String.format(Locale.US,
                            "Trạng thái: Đang nhận \"%s\" (%d%% - %.2f MB/s)",
                            outputFile.getName(), percent, speedMbps));
                }
                bos.flush();
            }

            // Kiểm tra tính toàn vẹn kích thước sau khi kết thúc vòng lặp.
            if (totalBytesRead != totalBytes) {
                Files.deleteIfExists(outputFile.toPath());
                throw new EOFException("Tệp nhận chưa hoàn chỉnh (" + totalBytesRead + "/" + totalBytes + " bytes)");
            }

            updateProgress(1.0, 1.0);
            double avgSpeed = calculateSpeedMBps(totalBytesRead, startTimeNano);
            updateMessage(String.format(Locale.US,
                    "Trạng thái: Đã nhận thành công \"%s\" (%.2f MB/s) -> thư mục %s",
                    outputFile.getName(), avgSpeed, DOWNLOADS_DIR_NAME));

            if (onFileReceivedCallback != null) {
                onFileReceivedCallback.accept(SharedFile.from(outputFile));
            }
        }
    }

    /**
     * Tránh ghi đè tệp cũ nếu trùng tên trong thư mục Downloads
     * (vd: {@code report.pdf} -> {@code report (1).pdf}).
     */
    private static File resolveOutputFile(Path directory, String fileName) {
        Path candidate = directory.resolve(fileName);
        if (!Files.exists(candidate)) {
            return candidate.toFile();
        }
        int dotIndex = fileName.lastIndexOf('.');
        String baseName = (dotIndex > 0) ? fileName.substring(0, dotIndex) : fileName;
        String extension = (dotIndex > 0) ? fileName.substring(dotIndex) : "";
        int counter = 1;
        while (Files.exists(candidate)) {
            candidate = directory.resolve(baseName + " (" + counter + ")" + extension);
            counter++;
        }
        return candidate.toFile();
    }

    private static double calculateSpeedMBps(long bytesTransferred, long startTimeNano) {
        double elapsedSeconds = (System.nanoTime() - startTimeNano) / 1_000_000_000.0;
        if (elapsedSeconds <= 0.0001) {
            return 0.0;
        }
        return (bytesTransferred / (1024.0 * 1024.0)) / elapsedSeconds;
    }

    /** Đóng ServerSocket an toàn khi người dùng tắt ứng dụng. */
    public void stopServer() {
        cancel(true);
        closeServerSocket();
    }

    private void closeServerSocket() {
        ServerSocket ss = this.serverSocket;
        if (ss != null && !ss.isClosed()) {
            try {
                ss.close();
            } catch (IOException ignored) {
                // Bỏ qua lỗi khi đóng.
            }
        }
    }
}
