package com.nhannv.p2p.ui;

import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.network.DiscoveryService;
import com.nhannv.p2p.network.PeerDiscoveryListener;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
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
 * <p>
 * Chỉ xử lý phần hiển thị. Logic mạng nằm trong {@link DiscoveryService} và chạy trên luồng nền;
 * Controller nhận kết quả qua {@link PeerDiscoveryListener} rồi chuyển về JavaFX Application Thread.
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

    /** Được FXMLLoader gọi sau khi đã inject toàn bộ các trường @FXML. */
    @FXML
    private void initialize() {
        // --- Danh sách Peer ---
        peerListView.setItems(peerList);
        peerListView.setPlaceholder(new Label("Đang tìm Peer trong mạng LAN..."));
        peerCountLabel.textProperty().bind(Bindings.size(peerList).asString("(%d)"));

        // --- Bảng tệp chia sẻ ---
        fileNameColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().name()));
        fileSizeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSize()));
        filePathColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().path()));
        sharedFileTable.setPlaceholder(new Label("Chưa có tệp nào được chia sẻ"));

        transferProgressBar.setProgress(0.0);
        setStatus("Sẵn sàng");

        startDiscovery();
    }

    /** Khởi động dịch vụ Discovery; mọi lỗi được hiển thị thân thiện trên thanh trạng thái. */
    private void startDiscovery() {
        discoveryService = new DiscoveryService(this);
        try {
            // start() chỉ mở socket rồi trả về ngay, các vòng lặp mạng chạy trên luồng nền riêng.
            discoveryService.start();
            setStatus("Đang dò tìm Peer trên UDP port " + discoveryService.getPort() + "...");
        } catch (BindException e) {
            setStatus("Lỗi: UDP port " + discoveryService.getPort()
                    + " đang bị chiếm (có thể ứng dụng đang chạy ở cửa sổ khác).");
        } catch (IOException e) {
            setStatus("Lỗi: không khởi động được Discovery - " + e.getMessage());
        }
    }

    /** Giải phóng tài nguyên mạng. Được {@code MainApplication.stop()} gọi khi đóng ứng dụng. */
    public void shutdown() {
        if (discoveryService != null) {
            discoveryService.stop();
        }
    }

    // ------------------------------------------------------------------------------------------
    // PeerDiscoveryListener: các method dưới đây chạy trên LUỒNG MẠNG, không phải luồng UI.
    // Mọi thay đổi lên ObservableList / Control phải đi qua Platform.runLater(), nếu không
    // JavaFX sẽ ném IllegalStateException ("Not on FX application thread") hoặc hỏng trạng thái.
    // ------------------------------------------------------------------------------------------

    @Override
    public void onPeerDiscovered(Peer peer) {
        runOnFxThread(() -> {
            int index = indexOfPeer(peer);
            if (index >= 0) {
                peerList.set(index, peer); // Peer đổi tên máy -> thay thế phần tử cũ.
            } else {
                peerList.add(peer);
            }
            setStatus("Phát hiện Peer: " + peer);
        });
    }

    @Override
    public void onPeerLost(Peer peer) {
        runOnFxThread(() -> {
            peerList.removeIf(p -> p.isSamePeer(peer));
            setStatus("Peer đã offline: " + peer);
        });
    }

    @Override
    public void onDiscoveryError(String message, Throwable cause) {
        runOnFxThread(() -> setStatus("Lỗi mạng: " + message));
    }

    // ------------------------------------------------------------------------------------------
    // Sự kiện giao diện
    // ------------------------------------------------------------------------------------------

    /** Xử lý nút "Chọn file". */
    @FXML
    private void onChooseFileClicked() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Chọn tệp để chia sẻ");
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Tất cả tệp", "*.*"));

        Window owner = chooseFileButton.getScene().getWindow();
        File selectedFile = fileChooser.showOpenDialog(owner);

        if (selectedFile == null) {
            setStatus("Đã hủy chọn tệp");
            return;
        }

        // TODO (Milestone sau): chuyển cho SharingService băm SHA-256 trên background Task.
        sharedFileTable.getItems().add(SharedFile.from(selectedFile));
        setStatus("Đã thêm: " + selectedFile.getName());
    }

    // ------------------------------------------------------------------------------------------
    // Tiện ích nội bộ
    // ------------------------------------------------------------------------------------------

    private int indexOfPeer(Peer peer) {
        for (int i = 0; i < peerList.size(); i++) {
            if (peerList.get(i).isSamePeer(peer)) {
                return i;
            }
        }
        return -1;
    }

    private void setStatus(String message) {
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
