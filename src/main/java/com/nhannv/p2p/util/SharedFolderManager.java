package com.nhannv.p2p.util;

import com.nhannv.p2p.model.SharedFile;
import javafx.concurrent.Task;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Quản lý thư mục chia sẻ ("Shared Folder") của máy cục bộ.
 *
 * <h2>Nhiệm vụ chính</h2>
 * <ul>
 *   <li>Mặc định khởi tạo thư mục {@code Shared/} ngay tại gốc dự án nếu người dùng chưa chọn.</li>
 *   <li>Cho phép đổi sang bất kỳ thư mục nào người dùng chọn qua {@code DirectoryChooser}.</li>
 *   <li>Quét toàn bộ tệp trong thư mục và tính mã băm SHA-256 trên <b>Background Thread</b>
 *       ({@link #createScanTask()}) để không bao giờ làm đơ giao diện JavaFX.</li>
 *   <li>Lưu danh sách {@link SharedFile} trong bộ nhớ an toàn luồng ({@link CopyOnWriteArrayList})
 *       để {@code P2PServer} trả lời lệnh {@code LIST_FILES} và {@code DOWNLOAD_FILE} tức thì.</li>
 * </ul>
 */
public class SharedFolderManager {

    private static final Logger LOG = Logger.getLogger(SharedFolderManager.class.getName());

    /** Tên thư mục chia sẻ mặc định trong thư mục gốc dự án. */
    public static final String DEFAULT_SHARED_DIR_NAME = "Shared";

    private volatile Path sharedFolderPath;
    private final String localHostName;
    private final List<SharedFile> cachedSharedFiles = new CopyOnWriteArrayList<>();

    public SharedFolderManager() {
        this.sharedFolderPath = Path.of(DEFAULT_SHARED_DIR_NAME).toAbsolutePath();
        this.localHostName = resolveLocalHostName();
        ensureDirectoryExists(this.sharedFolderPath);
    }

    /** Trả về đường dẫn thư mục chia sẻ hiện tại. */
    public Path getSharedFolderPath() {
        return sharedFolderPath;
    }

    /** Đổi thư mục chia sẻ sang thư mục mới do người dùng chọn. */
    public void setSharedFolder(File directory) {
        Objects.requireNonNull(directory, "directory");
        Path path = directory.toPath().toAbsolutePath();
        ensureDirectoryExists(path);
        this.sharedFolderPath = path;
    }

    /** Lấy danh sách tệp chia sẻ đã được băm SHA-256 gần nhất (an toàn đa luồng). */
    public List<SharedFile> getSharedFiles() {
        return Collections.unmodifiableList(new ArrayList<>(cachedSharedFiles));
    }

    /**
     * Tìm tệp thực tế trên đĩa trong thư mục chia sẻ theo tên tệp để phục vụ lệnh {@code DOWNLOAD_FILE}.
     * Chỉ cho phép truy cập tệp nằm trực tiếp bên trong {@code sharedFolderPath} (chống Path Traversal).
     */
    public File resolveSharedFileOnDisk(String requestedFileName) {
        if (requestedFileName == null || requestedFileName.isBlank()) {
            return null;
        }
        String safeName = Path.of(requestedFileName).getFileName().toString();
        Path resolved = sharedFolderPath.resolve(safeName).normalize();
        if (!resolved.startsWith(sharedFolderPath)) {
            return null;
        }
        File file = resolved.toFile();
        return (file.exists() && file.isFile()) ? file : null;
    }

    /** Tìm metadata {@link SharedFile} đã băm sẵn theo tên tệp. */
    public SharedFile findMetadataByName(String fileName) {
        if (fileName == null) {
            return null;
        }
        for (SharedFile sf : cachedSharedFiles) {
            if (sf.fileName().equals(fileName)) {
                return sf;
            }
        }
        return null;
    }

    /**
     * Quét đồng bộ thư mục chia sẻ và tính SHA-256 cho từng tệp.
     * <b>Chỉ gọi hàm này từ luồng nền</b> (hoặc dùng {@link #createScanTask()}).
     */
    public List<SharedFile> scanAndHashSync() {
        Path folder = this.sharedFolderPath;
        ensureDirectoryExists(folder);

        File[] files = folder.toFile().listFiles(File::isFile);
        if (files == null || files.length == 0) {
            cachedSharedFiles.clear();
            return List.of();
        }

        List<SharedFile> result = new ArrayList<>(files.length);
        for (File file : files) {
            try {
                String sha256 = HashUtil.calculateSHA256(file);
                result.add(new SharedFile(
                        file.getName(),
                        file.length(),
                        sha256,
                        "LOCAL",
                        localHostName
                ));
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Không thể băm SHA-256 cho tệp: " + file.getName(), e);
            }
        }

        cachedSharedFiles.clear();
        cachedSharedFiles.addAll(result);
        return Collections.unmodifiableList(result);
    }

    /**
     * Tạo một JavaFX {@link Task} quét thư mục chia sẻ và tính SHA-256 trên luồng nền,
     * đồng thời cập nhật tiến trình băm từng tệp về UI.
     */
    public Task<List<SharedFile>> createScanTask() {
        return new Task<>() {
            @Override
            protected List<SharedFile> call() {
                Path folder = sharedFolderPath;
                ensureDirectoryExists(folder);

                File[] files = folder.toFile().listFiles(File::isFile);
                if (files == null || files.length == 0) {
                    cachedSharedFiles.clear();
                    updateProgress(1.0, 1.0);
                    updateMessage("Trạng thái: Thư mục chia sẻ (" + folder.getFileName() + ") đang trống");
                    return List.of();
                }

                List<SharedFile> scanned = new ArrayList<>(files.length);
                int total = files.length;
                for (int i = 0; i < total; i++) {
                    if (isCancelled()) {
                        break;
                    }
                    File f = files[i];
                    updateProgress(i, total);
                    updateMessage("Trạng thái: Đang tính SHA-256 (" + (i + 1) + "/" + total + "): " + f.getName());
                    try {
                        String sha256 = HashUtil.calculateSHA256(f);
                        scanned.add(new SharedFile(
                                f.getName(),
                                f.length(),
                                sha256,
                                "LOCAL",
                                localHostName
                        ));
                    } catch (IOException e) {
                        LOG.log(Level.WARNING, "Bỏ qua tệp lỗi khi tính SHA-256: " + f.getName(), e);
                    }
                }

                cachedSharedFiles.clear();
                cachedSharedFiles.addAll(scanned);
                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Đã quét xong " + scanned.size()
                        + " tệp trong thư mục chia sẻ (" + folder.getFileName() + ")");
                return Collections.unmodifiableList(scanned);
            }
        };
    }

    private static void ensureDirectoryExists(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Không thể tạo thư mục chia sẻ: " + dir, e);
        }
    }

    private static String resolveLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            String env = System.getenv("COMPUTERNAME");
            return (env != null && !env.isBlank()) ? env : "My-PC";
        }
    }
}
