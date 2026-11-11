package com.nhannv.p2p.ui;

import com.nhannv.p2p.model.SharedFile;
import javafx.beans.property.ReadOnlyStringWrapper;
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

/**
 * Controller of {@code MainView.fxml}.
 * <p>
 * Only handles UI concerns. Networking and file I/O must live in the
 * {@code network} / {@code util} layers and run on background threads.
 */
public class MainController {

    @FXML
    private ListView<String> peerListView;

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

    /** Called by FXMLLoader after all @FXML fields are injected. */
    @FXML
    private void initialize() {
        fileNameColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().name()));
        fileSizeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSize()));
        filePathColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().path()));

        peerListView.setPlaceholder(new Label("Đang tìm Peer trong mạng LAN..."));
        sharedFileTable.setPlaceholder(new Label("Chưa có tệp nào được chia sẻ"));

        transferProgressBar.setProgress(0);
        statusLabel.setText("Sẵn sàng");
    }

    /** Handles the "Chọn file chia sẻ" button. */
    @FXML
    private void onChooseFileClicked() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Chọn tệp để chia sẻ");
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Tất cả tệp", "*.*"));

        Window owner = chooseFileButton.getScene().getWindow();
        File selectedFile = fileChooser.showOpenDialog(owner);

        if (selectedFile == null) {
            statusLabel.setText("Đã hủy chọn tệp");
            return;
        }

        // TODO: delegate to a sharing service (hashing/indexing on a background Task).
        sharedFileTable.getItems().add(SharedFile.from(selectedFile));
        statusLabel.setText("Đã thêm: " + selectedFile.getName());
    }
}
