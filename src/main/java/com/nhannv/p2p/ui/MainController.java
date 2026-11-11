package com.nhannv.p2p.ui;

import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.network.DiscoveryService;
import com.nhannv.p2p.network.FileDownloadTask;
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
import javafx.scene.control.TextField;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Window;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Controller trung tâm của {@code MainView.fxml} (Milestone 3).
 *
 * <h2>Kiến trúc & Luồng hoạt động cho team (Huỳnh Nhân Danh)</h2>
 * <ul>
 *   <li><b>Quản lý Shared Folder ({@link SharedFolderManager}):</b>
 *       Quét các tệp trong thư mục chia sẻ và tính SHA-256 trên luồng nền.</li>
 *   <li><b>Giao thức TCP Đa Lệnh ({@link P2PServer}):</b>
 *       Lắng nghe đồng thời 3 lệnh {@code LIST_FILES}, {@code DOWNLOAD_FILE}, {@code PUSH_FILE}
 *       trên cổng TCP 8889 bằng {@code ExecutorService} (Thread Pool).</li>
 *   <li><b>Tìm kiếm & Gom danh sách tệp ({@code LIST_FILES}):</b>
 *       Khi người dùng bấm vào một Peer ở cột trái hoặc bấm "Làm mới danh sách tệp", ứng dụng
 *       gửi lệnh {@code LIST_FILES} tới các Peer trên luồng nền, gom toàn bộ {@link SharedFile}
 *       về {@code masterFileList} và lọc tức thời qua {@link FilteredList} khi gõ ô tìm kiếm.</li>
 *   <li><b>Tải tệp & Xác thực On-the-fly SHA-256 ({@link FileDownloadTask}):</b>
 *       Khi chọn 1 tệp trên bảng và bấm "Tải tệp đã chọn", {@code FileDownloadTask} tải từng chunk 64KB,
 *       đo tốc độ MB/s thời gian thực, vừa ghi đĩa vừa băm SHA-256 ngay trên luồng byte và đối chiếu
 *       với mã SHA-256 gốc khi đạt 100%.</li>
 * </ul>
 */
public class MainController implements PeerDiscoveryListener {

    private static final String STATUS_PREFIX = "Trạng thái: ";

    @FXML
    private Label sharedFolderLabel;

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
    private TableColumn<SharedFile, String> fileOwnerColumn;

    @FXML
    private TableColumn<SharedFile, String> fileHashColumn;

    @FXML
    private Button downloadSelectedButton;

    @FXML
    private Button chooseFileButton;

    @FXML
    private Label statusLabel;

    @FXML
    private ProgressBar transferProgressBar;

    /** Danh sách Peer đang online trong LAN. */
    private final ObservableList<Peer> peerList = FXCollections.observableArrayList();

    /** Danh sách gốc chứa toàn bộ tệp chia sẻ (của máy mình + của các Peer trong LAN). */
    private final ObservableList<SharedFile> masterFileList = FXCollections.observableArrayList();

    /** Danh sách bọc quanh masterFileList để lọc theo ô tìm kiếm tức thời. */
    private FilteredList<SharedFile> filteredFileList;

    private final SharedFolderManager sharedFolderManager = new SharedFolderManager();
    private DiscoveryService discoveryService;
    private P2PServer p2pServer;

    @FXML
    private void initialize() {
        // --- 1. Cột trái: Danh sách Peer đang online ---
        peerListView.setItems(peerList);
        peerListView.setPlaceholder(new Label("Đang tìm Peer trong mạng LAN..."));
        peerCountLabel.textProperty().bind(Bindings.size(peerList).asString("(%d)"));

        // Khi người dùng click chọn một Peer bên trái -> tự động gửi LIST_FILES lấy danh sách tệp.
        peerListView.getSelectionModel().selectedItemProperty().addListener((obs, oldPeer, selectedPeer) -> {
            if (selectedPeer != null) {
                fetchFilesFromPeersAsync(List.of(selectedPeer));
            }
        });

        // --- 2. Vùng giữa: Bảng 4 cột (Tên tệp, Dung lượng, Máy sở hữu, Mã SHA-256) + Tìm kiếm ---
        fileNameColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().fileName()));
        fileSizeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSize()));
        fileOwnerColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().ownerDisplay()));
        fileHashColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().sha256Hash()));
        sharedFileTable.setPlaceholder(new Label("Chưa có tệp nào trong thư mục chia sẻ hoặc từ các Peer"));

        filteredFileList = new FilteredList<>(masterFileList, file -> true);
        sharedFileTable.setItems(filteredFileList);

        // Lọc danh sách tệp ngay khi người dùng gõ vào ô tìm kiếm.
        searchTextField.textProperty().addListener((obs, oldQuery, newQuery) -> {
            String keyword = (newQuery == null) ? "" : newQuery.trim().toLowerCase(Locale.ROOT);
            filteredFileList.setPredicate(file -> matchesSearchKeyword(file, keyword));
        });

        updateSharedFolderLabel();

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
        } catch (BindException e) {
            setStatus("Lỗi: UDP port " + discoveryService.getPort()
                    + " đang bị chiếm (có thể ứng dụng đang chạy ở cửa sổ khác).");
        } catch (IOException e) {
            setStatus("Lỗi: không khởi động được Discovery - " + e.getMessage());
        }
    }

    /** Khởi động {@link P2PServer} đa lệnh trên cổng TCP 8889 và bind vào UI. */
    private void startP2PServer() {
        p2pServer = new P2PServer(
                P2PServer.DEFAULT_PORT,
                sharedFolderManager,
                receivedFile -> runOnFxThread(() -> mergeFilesIntoTable(List.of(receivedFile), false))
        );

        bindUiToTask(p2pServer);
        p2pServer.messageProperty().addListener((obs, oldMsg, newMsg) -> bindUiToTask(p2pServer));

        Thread serverThread = new Thread(p2pServer, "p2p-multi-command-server");
        serverThread.setDaemon(true);
        serverThread.start();
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
    // PeerDiscoveryListener (chạy trên luồng mạng UDP -> chuyển về JavaFX Thread)
    // ------------------------------------------------------------------------------------------

    @Override
    public void onPeerDiscovered(Peer peer) {
        runOnFxThread(() -> {
            int index = indexOfPeer(peer);
            if (index >= 0) {
                peerList.set(index, peer);
            } else {
                peerList.add(peer);
            }
            // Tự động hỏi danh sách tệp chia sẻ của Peer vừa phát hiện.
            fetchFilesFromPeersAsync(List.of(peer));
        });
    }

    @Override
    public void onPeerLost(Peer peer) {
        runOnFxThread(() -> {
            peerList.removeIf(p -> p.isSamePeer(peer));
            masterFileList.removeIf(f -> f.ownerIp().equals(peer.ipAddress()));
        });
    }

    @Override
    public void onDiscoveryError(String message, Throwable cause) {
        runOnFxThread(() -> setStatus("Lỗi mạng: " + message));
    }

    // ------------------------------------------------------------------------------------------
    // Sự kiện giao diện: Chọn Shared Folder, Làm mới LIST_FILES, Tải tệp (Pull), Gửi tệp (Push)
    // ------------------------------------------------------------------------------------------

    /**
     * Nút "Chọn thư mục chia sẻ": mở {@link DirectoryChooser}, đổi thư mục chia sẻ
     * và quét + băm SHA-256 toàn bộ tệp trong thư mục đó trên luồng nền.
     */
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
        scanLocalSharedFolderAsync();
    }

    /**
     * Nút "Làm mới danh sách tệp": quét lại thư mục chia sẻ cục bộ và gửi lệnh {@code LIST_FILES}
     * tới toàn bộ các Peer đang online trong mạng LAN để gom danh sách tệp mới nhất.
     */
    @FXML
    private void handleRefreshFiles() {
        List<Peer> snapshotPeers = new ArrayList<>(peerList);
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
                        // Peer tạm thời bận hoặc đóng kết nối -> tiếp tục hỏi các Peer còn lại.
                    }
                }

                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Đã làm mới danh sách (" + combined.size()
                        + " tệp từ máy bạn và " + snapshotPeers.size() + " Peer)");
                return combined;
            }
        };

        refreshTask.setOnSucceeded(event -> {
            List<SharedFile> files = refreshTask.getValue();
            if (files != null) {
                mergeFilesIntoTable(files, true);
            }
        });

        bindUiToTask(refreshTask);
        Thread thread = new Thread(refreshTask, "p2p-refresh-files");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Nút "Tải tệp đã chọn" (Pull Protocol):
     * Khởi chạy {@link FileDownloadTask}, bind với {@code ProgressBar} và {@code statusLabel},
     * đo tốc độ MB/s và xác thực SHA-256 On-the-fly.
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

        // Nếu người dùng chọn tệp của chính máy mình, vẫn cho phép tải thử qua loopback (127.0.0.1)
        // để có thể kiểm thử ngay trên 1 máy!
        String targetIp = selectedFile.isLocal() ? "127.0.0.1" : selectedFile.ownerIp();

        FileDownloadTask downloadTask = new FileDownloadTask(
                targetIp,
                P2PServer.DEFAULT_PORT,
                selectedFile.fileName()
        );

        bindUiToTask(downloadTask);

        Thread downloadThread = new Thread(downloadTask, "p2p-file-download");
        downloadThread.setDaemon(true);
        downloadThread.start();
    }

    /**
     * Nút "Chọn file" (Push Protocol):
     * Chọn một Peer ở cột trái rồi chủ động đẩy một tệp bất kỳ trên máy sang Peer đó.
     */
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
    // Tiện ích quét thư mục và truy vấn LIST_FILES bất đồng bộ
    // ------------------------------------------------------------------------------------------

    private void scanLocalSharedFolderAsync() {
        Task<List<SharedFile>> scanTask = sharedFolderManager.createScanTask();
        scanTask.setOnSucceeded(e -> {
            List<SharedFile> localFiles = scanTask.getValue();
            masterFileList.removeIf(SharedFile::isLocal);
            if (localFiles != null) {
                masterFileList.addAll(0, localFiles);
            }
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
                    masterFileList.removeIf(f -> f.ownerIp().equals(p.ipAddress()));
                }
                mergeFilesIntoTable(remoteFiles, false);
                setStatus("Đã tải danh sách " + remoteFiles.size() + " tệp từ " + peersToQuery.get(0));
            }
        });

        Thread t = new Thread(listTask, "p2p-list-files-client");
        t.setDaemon(true);
        t.start();
    }

    /** Hợp nhất danh sách tệp vào bảng và loại bỏ các bản ghi trùng lặp (cùng IP + tên tệp + SHA-256). */
    private void mergeFilesIntoTable(List<SharedFile> incomingFiles, boolean replaceAll) {
        Map<String, SharedFile> uniqueMap = new LinkedHashMap<>();
        if (!replaceAll) {
            for (SharedFile existing : masterFileList) {
                uniqueMap.put(uniqueKey(existing), existing);
            }
        }
        for (SharedFile incoming : incomingFiles) {
            uniqueMap.put(uniqueKey(incoming), incoming);
        }
        masterFileList.setAll(uniqueMap.values());
    }

    private static String uniqueKey(SharedFile file) {
        return file.ownerIp() + "|" + file.fileName() + "|" + file.sha256Hash();
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
