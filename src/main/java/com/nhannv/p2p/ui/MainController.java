package com.nhannv.p2p.ui;

import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.network.DiscoveryService;
import com.nhannv.p2p.network.FileReceiverTask;
import com.nhannv.p2p.network.FileSenderTask;
import com.nhannv.p2p.network.PeerDiscoveryListener;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.stage.FileChooser;
import javafx.stage.Window;

import java.io.File;
import java.io.IOException;
import java.net.BindException;

/**
 * Controller của {@code MainView.fxml}.
 *
 * <h2>Giải thích cơ chế Thread Binding & Đa luồng cho team (Huỳnh Nhân Danh)</h2>
 * <ul>
 *   <li><b>Nguyên tắc bất di bất dịch:</b> Tầng UI (Controller) tuyệt đối KHÔNG thực hiện đọc/ghi
 *       {@code FileInputStream}/{@code FileOutputStream} hay {@code Socket} trên <i>JavaFX Application Thread</i>.</li>
 *   <li><b>Cơ chế Thread Binding với {@link Task}:</b>
 *     <ul>
 *       <li>Khi ứng dụng khởi chạy ({@link #initialize()}), {@link FileReceiverTask} được chạy trên một
 *           <i>Daemon Thread</i> nền và hai thuộc tính {@code progressProperty()} / {@code messageProperty()}
 *           của nó được ràng buộc ({@code bind}) trực tiếp vào {@code transferProgressBar} và {@code statusLabel}.</li>
 *       <li>Khi người dùng bấm "Chọn file" ({@link #handleChooseFile()}), một {@link FileSenderTask} mới
 *           được tạo để gửi tệp tới Peer đang chọn. Ta dùng {@link #bindUiToTask(Task)} để chuyển hướng
 *           {@code ProgressBar} và {@code Label} sang theo dõi tiến trình gửi. Khi gửi xong, UI tự động
 *           nối (re-bind) trở lại {@code FileReceiverTask} để tiếp tục theo dõi luồng nhận.</li>
 *       <li><b>Lưu ý kỹ thuật quan trọng:</b> Trong JavaFX, một thuộc tính đang {@code bind()} mà gọi
 *           {@code setText()} hoặc {@code setProgress()} trực tiếp sẽ ném lỗi
 *           {@code RuntimeException: A bound value cannot be set}. Vì vậy hàm {@link #setStatus(String)}
 *           bên dưới luôn gọi {@code unbind()} an toàn trước khi gán chuỗi thủ công.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class MainController implements PeerDiscoveryListener {

    private static final String STATUS_PREFIX = "Trạng thái: ";

    @FXML
    private ListView<Peer> peerListView;

    @FXML
    private Label peerCountLabel;

    @FXML
    private TableView<SharedFile> sharedFileTable;

    @FXML
    private TableColumn<SharedFile, String> fileNameColumn;

    @FXML
    private TableColumn<SharedFile, String> fileSizeColumn;

    @FXML
    private TableColumn<SharedFile, String> filePathColumn;

    @FXML
    private Button chooseFileButton;

    @FXML
    private Label statusLabel;

    @FXML
    private ProgressBar transferProgressBar;

    /** Nguồn dữ liệu của ListView. CHỈ được sửa trên JavaFX Application Thread. */
    private final ObservableList<Peer> peerList = FXCollections.observableArrayList();

    private DiscoveryService discoveryService;
    private FileReceiverTask fileReceiverTask;

    /** Được FXMLLoader gọi sau khi đã inject toàn bộ các trường @FXML. */
    @FXML
    private void initialize() {
        // --- 1. Danh sách Peer (bên trái) ---
        peerListView.setItems(peerList);
        peerListView.setPlaceholder(new Label("Đang tìm Peer trong mạng LAN..."));
        peerCountLabel.textProperty().bind(Bindings.size(peerList).asString("(%d)"));

        // --- 2. Bảng tệp chia sẻ / đã nhận (ở giữa) ---
        fileNameColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().name()));
        fileSizeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSize()));
        filePathColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().path()));
        sharedFileTable.setPlaceholder(new Label("Chưa có tệp nào được chia sẻ hoặc nhận về"));

        // --- 3. Khởi động UDP Discovery ---
        startDiscovery();

        // --- 4. Khởi động TCP FileReceiverTask trên Daemon Thread và bind vào UI ---
        startFileReceiverServer();
    }

    /** Khởi động dịch vụ UDP Discovery để tự động tìm các máy trong mạng LAN. */
    private void startDiscovery() {
        discoveryService = new DiscoveryService(this);
        try {
            discoveryService.start();
            setStatus("Đang dò tìm Peer trên UDP port " + discoveryService.getPort() + "...");
        } catch (BindException e) {
            setStatus("Lỗi: UDP port " + discoveryService.getPort()
                    + " đang bị chiếm (có thể ứng dụng đang chạy ở cửa sổ khác).");
        } catch (IOException e) {
            setStatus("Lỗi: không khởi động được Discovery - " + e.getMessage());
        }
    }

    /**
     * Khởi tạo {@link FileReceiverTask}, bind {@code progressProperty()} và {@code messageProperty()}
     * vào thanh {@code ProgressBar} + {@code Label} trạng thái, rồi chạy trên một Daemon Thread nền.
     */
    private void startFileReceiverServer() {
        fileReceiverTask = new FileReceiverTask(
                FileReceiverTask.DEFAULT_PORT,
                receivedFile -> runOnFxThread(() -> sharedFileTable.getItems().add(receivedFile))
        );

        // THREAD BINDING: Ràng buộc thanh tiến trình và nhãn trạng thái vào luồng nhận tệp.
        bindUiToTask(fileReceiverTask);

        // Nếu trước đó UI đã chuyển sang theo dõi FileSenderTask (hoặc bị unbind),
        // tự động bind lại về FileReceiverTask ngay khi có một phiên nhận tệp mới bắt đầu.
        fileReceiverTask.messageProperty().addListener((obs, oldMsg, newMsg) -> bindUiToTask(fileReceiverTask));

        Thread receiverThread = new Thread(fileReceiverTask, "p2p-tcp-file-receiver");
        receiverThread.setDaemon(true); // Daemon Thread: tự động tắt theo JVM khi đóng cửa sổ chính.
        receiverThread.start();
    }

    /**
     * Giải phóng toàn bộ tài nguyên mạng (UDP Discovery + TCP ServerSocket).
     * Được {@code MainApplication.stop()} gọi khi người dùng bấm X đóng ứng dụng.
     */
    public void shutdown() {
        if (discoveryService != null) {
            discoveryService.stop();
        }
        if (fileReceiverTask != null) {
            fileReceiverTask.stopServer();
        }
    }

    // ------------------------------------------------------------------------------------------
    // PeerDiscoveryListener: các method dưới đây chạy trên LUỒNG MẠNG UDP, không phải luồng UI.
    // Mọi thay đổi lên ObservableList / Control phải đi qua Platform.runLater().
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
        });
    }

    @Override
    public void onPeerLost(Peer peer) {
        runOnFxThread(() -> peerList.removeIf(p -> p.isSamePeer(peer)));
    }

    @Override
    public void onDiscoveryError(String message, Throwable cause) {
        runOnFxThread(() -> setStatus("Lỗi mạng: " + message));
    }

    // ------------------------------------------------------------------------------------------
    // Sự kiện giao diện: Chọn Peer -> Chọn File -> Gửi qua TCP (Chunking)
    // ------------------------------------------------------------------------------------------

    /**
     * Xử lý sự kiện bấm nút "Chọn file":
     * <ol>
     *   <li>Kiểm tra người dùng đã chọn một Peer trong {@code peerListView} chưa.</li>
     *   <li>Mở hộp thoại {@link FileChooser} để chọn tệp cần gửi.</li>
     *   <li>Khởi tạo {@link FileSenderTask}, bind {@code ProgressBar} + {@code Label} vào Task này,
     *       và kích hoạt trên một Daemon Thread mới.</li>
     * </ol>
     */
    @FXML
    private void handleChooseFile() {
        // Bước 1: Bắt buộc chọn Peer đích từ ListView trước khi chọn tệp.
        Peer selectedPeer = peerListView.getSelectionModel().getSelectedItem();
        if (selectedPeer == null) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa chọn máy nhận (Peer)");
            alert.setHeaderText("Vui lòng chọn một Peer đang online ở danh sách bên trái!");
            alert.setContentText("Bạn cần chọn máy đích trong mạng LAN trước khi chọn tệp để gửi.");
            alert.showAndWait();
            return;
        }

        // Bước 2: Mở hộp thoại FileChooser.
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Chọn tệp để gửi tới " + selectedPeer);
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Tất cả tệp", "*.*"));

        Window owner = chooseFileButton.getScene().getWindow();
        File selectedFile = fileChooser.showOpenDialog(owner);

        if (selectedFile == null) {
            return;
        }

        // Hiển thị tệp vừa chọn lên bảng trung tâm để theo dõi lịch sử chia sẻ.
        sharedFileTable.getItems().add(SharedFile.from(selectedFile));

        // Bước 3: Tạo FileSenderTask, bind thanh ProgressBar & Label trạng thái vào Task gửi,
        // và chạy trên một luồng nền mới để giao diện hoàn toàn mượt mà.
        String targetPeerIp = selectedPeer.ipAddress();
        FileSenderTask senderTask = new FileSenderTask(selectedFile, targetPeerIp);

        bindUiToTask(senderTask);

        Thread senderThread = new Thread(senderTask, "p2p-tcp-file-sender");
        senderThread.setDaemon(true);
        senderThread.start();
    }

    /** Giữ bí danh (alias) tương thích ngược với cấu hình FXML cũ nếu cần. */
    @FXML
    private void onChooseFileClicked() {
        handleChooseFile();
    }

    // ------------------------------------------------------------------------------------------
    // Tiện ích nội bộ & Quản lý Thread Binding
    // ------------------------------------------------------------------------------------------

    /**
     * Ràng buộc (bind) thanh {@code ProgressBar} và {@code Label} trạng thái vào một {@link Task}
     * đang chạy (có thể là {@link FileReceiverTask} hoặc {@link FileSenderTask}).
     */
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

    /**
     * Cập nhật nhãn trạng thái thủ công. Tự động {@code unbind()} trước khi {@code setText()}
     * để không bao giờ xảy ra lỗi {@code A bound value cannot be set}.
     */
    private void setStatus(String message) {
        statusLabel.textProperty().unbind();
        statusLabel.setText(STATUS_PREFIX + message);
    }

    /**
     * Đẩy tác vụ về JavaFX Application Thread.
     * Bắt IllegalStateException cho trường hợp gói tin tới đúng lúc toolkit đang tắt.
     */
    private static void runOnFxThread(Runnable action) {
        if (Platform.isFxApplicationThread()) {
            action.run();
            return;
        }
        try {
            Platform.runLater(action);
        } catch (IllegalStateException ignored) {
            // JavaFX toolkit đã tắt -> không còn UI để cập nhật.
        }
    }
}
