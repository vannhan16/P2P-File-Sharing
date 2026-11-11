package com.nhannv.p2p.ui;

import com.nhannv.p2p.MainApplication;
import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.network.DiscoveryService;
import com.nhannv.p2p.network.DownloadManager;
import com.nhannv.p2p.network.FileSenderTask;
import com.nhannv.p2p.network.P2PServer;
import com.nhannv.p2p.network.PeerDiscoveryListener;
import com.nhannv.p2p.util.SharedFolderManager;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Window;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Controller trung tâm của {@code MainView.fxml} (Milestone 4 - Final Technical Milestone).
 *
 * <h2>Các tính năng nổi bật (Thuyết trình & Vấn đáp đồ án)</h2>
 * <ul>
 *   <li><b>Gộp nguồn Swarm theo SHA-256:</b>
 *       Các tệp có cùng mã {@code sha256Hash} từ nhiều Peer được gộp thông minh trên {@code TableView}
 *       và hiển thị số lượng nguồn ở cột <b>"Số nguồn (Peers)"</b> (vd: {@code "2 Peers (Swarm)"}).</li>
 *   <li><b>Điều khiển Pause / Resume / Cancel:</b>
 *       Kết nối trực tiếp với {@link DownloadManager} để tạm dừng, tiếp tục tải từ chính xác byte offset
 *       của tệp {@code .part}, hoặc hủy và dọn dẹp tệp tạm.</li>
 *   <li><b>Nhật ký sự kiện mạng thời gian thực ({@code networkLogArea}):</b>
 *       Ghi lại chi tiết từng mốc thời gian {@code [HH:mm:ss]} của mọi sự kiện UDP Discovery,
 *       Offset Resume, phân đoạn Swarm và xác thực SHA-256.</li>
 * </ul>
 */
public class MainController implements PeerDiscoveryListener {

    private static final String STATUS_PREFIX = "Trạng thái: ";
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @FXML
    private Label sharedFolderLabel;

    @FXML
    private Button themeToggleButton;

    @FXML
    private ListView<Peer> peerListView;

    @FXML
    private Label peerCountLabel;

    @FXML
    private TextField searchTextField;

    @FXML
    private Button chooseSharedFolderButton;

    @FXML
    private Button refreshFilesButton;

    @FXML
    private TableView<SharedFile> sharedFileTable;

    @FXML
    private TableColumn<SharedFile, String> fileNameColumn;

    @FXML
    private TableColumn<SharedFile, String> fileSizeColumn;

    @FXML
    private TableColumn<SharedFile, String> fileSourcesColumn;

    @FXML
    private TableColumn<SharedFile, String> fileOwnerColumn;

    @FXML
    private TableColumn<SharedFile, String> fileHashColumn;

    @FXML
    private TextArea networkLogArea;

    @FXML
    private Button downloadSelectedButton;

    @FXML
    private Button pauseButton;

    @FXML
    private Button resumeButton;

    @FXML
    private Button cancelButton;

    @FXML
    private Button chooseFileButton;

    @FXML
    private Label statusLabel;

    @FXML
    private ProgressBar transferProgressBar;

    /** Danh sách Peer đang online trong LAN. */
    private final ObservableList<Peer> peerList = FXCollections.observableArrayList();

    /** Danh sách tệp thô (chưa gộp) từ máy cục bộ và các Peer. */
    private final List<SharedFile> rawFileEntries = new ArrayList<>();

    /** Danh sách tệp đã gộp theo mã SHA-256 để hiển thị lên TableView. */
    private final ObservableList<SharedFile> masterFileList = FXCollections.observableArrayList();

    /** Danh sách bọc quanh masterFileList để lọc theo ô tìm kiếm tức thời. */
    private FilteredList<SharedFile> filteredFileList;

    private final SharedFolderManager sharedFolderManager = new SharedFolderManager();
    private final DownloadManager downloadManager = new DownloadManager(this::appendNetworkLog);
    private DiscoveryService discoveryService;
    private P2PServer p2pServer;

    @FXML
    private void initialize() {
        // --- 1. Cột trái: Danh sách Peer đang online ---
        peerListView.setItems(peerList);
        peerListView.setPlaceholder(new Label("Đang tìm Peer trong mạng LAN..."));
        peerCountLabel.textProperty().bind(Bindings.size(peerList).asString("(%d)"));

        peerListView.getSelectionModel().selectedItemProperty().addListener((obs, oldPeer, selectedPeer) -> {
            if (selectedPeer != null) {
                fetchFilesFromPeersAsync(List.of(selectedPeer));
            }
        });

        // --- 2. Vùng giữa: Bảng 5 cột (Tên tệp, Dung lượng, Số nguồn, Máy sở hữu, Mã SHA-256) ---
        fileNameColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().fileName()));
        fileSizeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSize()));
        fileSourcesColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().peerCountDisplay()));
        fileOwnerColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().ownerDisplay()));
        fileHashColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().sha256Hash()));
        sharedFileTable.setPlaceholder(new Label("Chưa có tệp nào trong thư mục chia sẻ hoặc từ các Peer"));

        filteredFileList = new FilteredList<>(masterFileList, file -> true);
        sharedFileTable.setItems(filteredFileList);

        searchTextField.textProperty().addListener((obs, oldQuery, newQuery) -> {
            String keyword = (newQuery == null) ? "" : newQuery.trim().toLowerCase(Locale.ROOT);
            filteredFileList.setPredicate(file -> matchesSearchKeyword(file, keyword));
        });

        updateSharedFolderLabel();
        appendNetworkLog("Khởi tạo hệ thống P2P File Sharing (AtlantaFX UI + Swarm + Pause/Resume)...");

        // --- 3. Khởi động UDP Discovery & P2P Multi-Command TCP Server ---
        startDiscovery();
        startP2PServer();

        // --- 4. Quét thư mục Shared mặc định lần đầu trên Background Thread ---
        scanLocalSharedFolderAsync();
    }

    private static boolean matchesSearchKeyword(SharedFile file, String keyword) {
        if (keyword.isEmpty()) {
            return true;
        }
        return file.fileName().toLowerCase(Locale.ROOT).contains(keyword)
                || file.ownerName().toLowerCase(Locale.ROOT).contains(keyword)
                || file.ownerIp().toLowerCase(Locale.ROOT).contains(keyword)
                || file.sha256Hash().toLowerCase(Locale.ROOT).contains(keyword);
    }

    /** Khởi động dịch vụ UDP Discovery trên cổng 8888. */
    private void startDiscovery() {
        discoveryService = new DiscoveryService(this);
        try {
            discoveryService.start();
            appendNetworkLog("UDP Discovery đã mở trên cổng " + discoveryService.getPort() + " (Broadcast 3s/lần).");
        } catch (BindException e) {
            String msg = "Lỗi: UDP port " + discoveryService.getPort() + " đang bị chiếm!";
            setStatus(msg);
            appendNetworkLog(msg);
        } catch (IOException e) {
            String msg = "Lỗi: không khởi động được Discovery - " + e.getMessage();
            setStatus(msg);
            appendNetworkLog(msg);
        }
    }

    /** Khởi động {@link P2PServer} đa lệnh trên cổng TCP 8889 và bind vào UI. */
    private void startP2PServer() {
        p2pServer = new P2PServer(
                P2PServer.DEFAULT_PORT,
                sharedFolderManager,
                receivedFile -> runOnFxThread(() -> addOrUpdateFileEntries(List.of(receivedFile), false)),
                this::appendNetworkLog
        );

        bindUiToTask(p2pServer);
        p2pServer.messageProperty().addListener((obs, oldMsg, newMsg) -> {
            if (!downloadManager.isDownloading()) {
                bindUiToTask(p2pServer);
            }
        });

        Thread serverThread = new Thread(p2pServer, "p2p-multi-command-server");
        serverThread.setDaemon(true);
        serverThread.start();
        appendNetworkLog("TCP Multi-Command Server lắng nghe cổng " + P2PServer.DEFAULT_PORT
                + " (LIST_FILES, DOWNLOAD_FILE, DOWNLOAD_CHUNK, PUSH_FILE).");
    }

    /** Giải phóng tài nguyên mạng khi người dùng bấm X tắt ứng dụng. */
    public void shutdown() {
        if (discoveryService != null) {
            discoveryService.stop();
        }
        if (p2pServer != null) {
            p2pServer.stopServer();
        }
    }

    // ------------------------------------------------------------------------------------------
    // PeerDiscoveryListener
    // ------------------------------------------------------------------------------------------

    @Override
    public void onPeerDiscovered(Peer peer) {
        runOnFxThread(() -> {
            int index = indexOfPeer(peer);
            if (index >= 0) {
                peerList.set(index, peer);
            } else {
                peerList.add(peer);
                appendNetworkLog("Phát hiện Peer mới trong mạng LAN: " + peer);
            }
            fetchFilesFromPeersAsync(List.of(peer));
        });
    }

    @Override
    public void onPeerLost(Peer peer) {
        runOnFxThread(() -> {
            peerList.removeIf(p -> p.isSamePeer(peer));
            rawFileEntries.removeIf(f -> f.ownerIp().equals(peer.ipAddress()));
            rebuildMergedFileTable();
            appendNetworkLog("Peer đã offline: " + peer);
        });
    }

    @Override
    public void onDiscoveryError(String message, Throwable cause) {
        runOnFxThread(() -> {
            setStatus("Lỗi mạng: " + message);
            appendNetworkLog("Cảnh báo mạng: " + message);
        });
    }

    // ------------------------------------------------------------------------------------------
    // Sự kiện giao diện: Theme, Shared Folder, LIST_FILES, Tải (Swarm/Resume), Pause, Resume, Cancel
    // ------------------------------------------------------------------------------------------

    /** Nút chuyển đổi giao diện Sáng / Tối (Primer Light <-> Nord Dark) của AtlantaFX. */
    @FXML
    private void handleToggleTheme() {
        boolean isDark = MainApplication.toggleTheme();
        if (themeToggleButton != null) {
            themeToggleButton.setText(isDark ? "Primer Light" : "Nord Dark");
        }
        appendNetworkLog("Đã chuyển giao diện sang theme AtlantaFX: " + (isDark ? "Nord Dark" : "Primer Light"));
    }

    /** Nút "Chọn thư mục chia sẻ": đổi Shared Folder và băm SHA-256 trên luồng nền. */
    @FXML
    private void handleChooseSharedFolder() {
        DirectoryChooser chooser = new DirectoryChooser();
        chooser.setTitle("Chọn thư mục chia sẻ (Shared Folder)");
        File currentDir = sharedFolderManager.getSharedFolderPath().toFile();
        if (currentDir.exists() && currentDir.isDirectory()) {
            chooser.setInitialDirectory(currentDir);
        }

        Window owner = chooseSharedFolderButton.getScene().getWindow();
        File selectedDir = chooser.showDialog(owner);
        if (selectedDir == null) {
            return;
        }

        sharedFolderManager.setSharedFolder(selectedDir);
        updateSharedFolderLabel();
        appendNetworkLog("Đã đổi thư mục chia sẻ sang: " + selectedDir.getAbsolutePath());
        scanLocalSharedFolderAsync();
    }

    /** Nút "Làm mới danh sách tệp": quét lại máy cục bộ và gửi LIST_FILES tới mọi Peer. */
    @FXML
    private void handleRefreshFiles() {
        List<Peer> snapshotPeers = new ArrayList<>(peerList);
        appendNetworkLog("Đang làm mới danh sách tệp từ thư mục chia sẻ và " + snapshotPeers.size() + " Peer...");

        Task<List<SharedFile>> refreshTask = new Task<>() {
            @Override
            protected List<SharedFile> call() {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang quét thư mục chia sẻ và truy vấn "
                        + snapshotPeers.size() + " Peer trong mạng LAN...");

                List<SharedFile> combined = new ArrayList<>(sharedFolderManager.scanAndHashSync());
                int queried = 0;
                for (Peer peer : snapshotPeers) {
                    if (isCancelled()) {
                        break;
                    }
                    queried++;
                    updateMessage("Trạng thái: Đang gửi LIST_FILES tới " + peer + " ("
                            + queried + "/" + snapshotPeers.size() + ")...");
                    try {
                        combined.addAll(P2PServer.requestFileListFromPeer(peer));
                    } catch (IOException ignored) {
                        // Peer tạm thời bận -> tiếp tục hỏi các Peer còn lại.
                    }
                }

                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Đã làm mới danh sách (" + combined.size()
                        + " mục từ máy bạn và " + snapshotPeers.size() + " Peer)");
                return combined;
            }
        };

        refreshTask.setOnSucceeded(event -> {
            List<SharedFile> files = refreshTask.getValue();
            if (files != null) {
                addOrUpdateFileEntries(files, true);
                appendNetworkLog("Làm mới hoàn tất: tổng hợp được " + masterFileList.size() + " tệp duy nhất.");
            }
        });

        bindUiToTask(refreshTask);
        Thread thread = new Thread(refreshTask, "p2p-refresh-files");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Nút "Tải tệp đã chọn":
     * Giao cho {@link DownloadManager} tự động chọn tải Đa nguồn ({@code SwarmDownloadTask})
     * nếu có nhiều Peer cùng giữ mã SHA-256, hoặc tải Đơn nguồn có hỗ trợ Pause/Resume
     * ({@code ResumableDownloadTask}).
     */
    @FXML
    private void handleDownloadSelectedFile() {
        SharedFile selectedFile = sharedFileTable.getSelectionModel().getSelectedItem();
        if (selectedFile == null) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa chọn tệp để tải");
            alert.setHeaderText("Vui lòng chọn một tệp trong bảng danh sách ở giữa!");
            alert.setContentText("Hãy bấm chọn một tệp từ danh sách trước khi bấm \"Tải tệp đã chọn\".");
            alert.showAndWait();
            return;
        }

        Task<Boolean> downloadTask = downloadManager.startDownload(selectedFile);
        bindUiToTask(downloadTask);
    }

    /** Nút "Tạm dừng (Pause)": tạm dừng phiên tải đang chạy, giữ nguyên tệp {@code .part}. */
    @FXML
    private void handlePauseDownload() {
        boolean paused = downloadManager.pauseCurrentDownload();
        if (!paused) {
            setStatus("Không có tác vụ tải nào đang chạy để tạm dừng.");
        }
    }

    /** Nút "Tiếp tục (Resume)": tải nối tiếp từ byte offset của tệp {@code .part}. */
    @FXML
    private void handleResumeDownload() {
        Task<Boolean> resumedTask = downloadManager.resumeCurrentDownload();
        if (resumedTask != null) {
            bindUiToTask(resumedTask);
        } else {
            setStatus("Không có phiên tải gián đoạn nào (.part) để tiếp tục.");
        }
    }

    /** Nút "Hủy (Cancel)": dừng phiên tải và xóa sạch tệp tạm {@code .part}. */
    @FXML
    private void handleCancelDownload() {
        downloadManager.cancelCurrentDownload();
        transferProgressBar.progressProperty().unbind();
        transferProgressBar.setProgress(0.0);
        setStatus("Đã hủy tải và xóa tệp tạm (.part).");
    }

    /** Nút "Chọn file (Push)": chủ động đẩy một tệp sang Peer đang chọn bên trái. */
    @FXML
    private void handleChooseFile() {
        Peer selectedPeer = peerListView.getSelectionModel().getSelectedItem();
        if (selectedPeer == null) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa chọn máy nhận (Peer)");
            alert.setHeaderText("Vui lòng chọn một Peer đang online ở danh sách bên trái!");
            alert.setContentText("Bạn cần chọn máy đích trong mạng LAN trước khi chọn tệp để gửi.");
            alert.showAndWait();
            return;
        }

        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Chọn tệp để gửi tới " + selectedPeer);
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Tất cả tệp", "*.*"));

        Window owner = chooseFileButton.getScene().getWindow();
        File selectedFile = fileChooser.showOpenDialog(owner);
        if (selectedFile == null) {
            return;
        }

        appendNetworkLog("Bắt đầu Push tệp \"" + selectedFile.getName() + "\" tới " + selectedPeer + "...");
        FileSenderTask senderTask = new FileSenderTask(selectedFile, selectedPeer.ipAddress());
        bindUiToTask(senderTask);

        Thread senderThread = new Thread(senderTask, "p2p-tcp-file-sender");
        senderThread.setDaemon(true);
        senderThread.start();
    }

    @FXML
    private void onChooseFileClicked() {
        handleChooseFile();
    }

    // ------------------------------------------------------------------------------------------
    // Gộp tệp trùng SHA-256 (Swarm Grouping) & Nhật ký sự kiện mạng
    // ------------------------------------------------------------------------------------------

    private void scanLocalSharedFolderAsync() {
        Task<List<SharedFile>> scanTask = sharedFolderManager.createScanTask();
        scanTask.setOnSucceeded(e -> {
            List<SharedFile> localFiles = scanTask.getValue();
            rawFileEntries.removeIf(SharedFile::isLocal);
            if (localFiles != null) {
                rawFileEntries.addAll(0, localFiles);
                appendNetworkLog("Đã băm SHA-256 xong " + localFiles.size()
                        + " tệp trong thư mục chia sẻ cục bộ.");
            }
            rebuildMergedFileTable();
        });
        bindUiToTask(scanTask);

        Thread scanThread = new Thread(scanTask, "p2p-shared-folder-scanner");
        scanThread.setDaemon(true);
        scanThread.start();
    }

    private void fetchFilesFromPeersAsync(List<Peer> peersToQuery) {
        if (peersToQuery == null || peersToQuery.isEmpty()) {
            return;
        }
        Task<List<SharedFile>> listTask = new Task<>() {
            @Override
            protected List<SharedFile> call() {
                List<SharedFile> fetched = new ArrayList<>();
                for (Peer peer : peersToQuery) {
                    try {
                        fetched.addAll(P2PServer.requestFileListFromPeer(peer));
                    } catch (IOException ignored) {
                        // Peer chưa mở cổng TCP hoặc đang bận.
                    }
                }
                return fetched;
            }
        };

        listTask.setOnSucceeded(e -> {
            List<SharedFile> remoteFiles = listTask.getValue();
            if (remoteFiles != null && !remoteFiles.isEmpty()) {
                for (Peer p : peersToQuery) {
                    rawFileEntries.removeIf(f -> f.ownerIp().equals(p.ipAddress()));
                }
                addOrUpdateFileEntries(remoteFiles, false);
                appendNetworkLog("Nhận danh sách " + remoteFiles.size()
                        + " tệp từ Peer " + peersToQuery.get(0));
            }
        });

        Thread t = new Thread(listTask, "p2p-list-files-client");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Cập nhật danh sách tệp thô và tự động gộp các tệp có cùng mã {@code sha256Hash}
     * từ nhiều Peer thành 1 dòng duy nhất trên {@code TableView} với danh sách {@code peerIps} đầy đủ.
     */
    private void addOrUpdateFileEntries(List<SharedFile> incomingFiles, boolean replaceAll) {
        if (replaceAll) {
            rawFileEntries.clear();
        }
        for (SharedFile incoming : incomingFiles) {
            rawFileEntries.removeIf(existing ->
                    existing.ownerIp().equals(incoming.ownerIp())
                            && existing.fileName().equals(incoming.fileName()));
            rawFileEntries.add(incoming);
        }
        rebuildMergedFileTable();
    }

    private void rebuildMergedFileTable() {
        Map<String, SharedFile> mergedBySha256 = new LinkedHashMap<>();
        for (SharedFile entry : rawFileEntries) {
            String key = entry.sha256Hash().toLowerCase(Locale.ROOT);
            SharedFile existing = mergedBySha256.get(key);
            if (existing == null) {
                mergedBySha256.put(key, entry);
            } else {
                mergedBySha256.put(key, existing.mergeWith(entry));
            }
        }
        masterFileList.setAll(mergedBySha256.values());
    }

    /**
     * Ghi một dòng sự kiện kèm mốc thời gian {@code [HH:mm:ss]} vào ô Nhật ký sự kiện mạng.
     * Tự động chuyển về JavaFX Application Thread nếu được gọi từ luồng mạng.
     */
    public void appendNetworkLog(String eventMessage) {
        String timestamp = LocalTime.now().format(TIME_FORMATTER);
        String line = "[" + timestamp + "] " + eventMessage + "\n";
        runOnFxThread(() -> {
            if (networkLogArea != null) {
                networkLogArea.appendText(line);
            }
        });
    }

    private void updateSharedFolderLabel() {
        if (sharedFolderLabel != null) {
            sharedFolderLabel.setText("Thư mục chia sẻ: " + sharedFolderManager.getSharedFolderPath());
        }
    }

    private void bindUiToTask(Task<?> task) {
        transferProgressBar.progressProperty().unbind();
        statusLabel.textProperty().unbind();

        transferProgressBar.progressProperty().bind(task.progressProperty());
        statusLabel.textProperty().bind(task.messageProperty());
    }

    private int indexOfPeer(Peer peer) {
        for (int i = 0; i < peerList.size(); i++) {
            if (peerList.get(i).isSamePeer(peer)) {
                return i;
            }
        }
        return -1;
    }

    private void setStatus(String message) {
        statusLabel.textProperty().unbind();
        statusLabel.setText(STATUS_PREFIX + message);
    }

    private static void runOnFxThread(Runnable action) {
        if (Platform.isFxApplicationThread()) {
            action.run();
            return;
        }
        try {
            Platform.runLater(action);
        } catch (IllegalStateException ignored) {
            // JavaFX toolkit đã tắt.
        }
    }
}
