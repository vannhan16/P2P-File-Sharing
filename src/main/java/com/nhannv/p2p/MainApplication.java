package com.nhannv.p2p;

import com.nhannv.p2p.ui.MainController;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;
import java.net.URL;

/**
 * Điểm khởi chạy ứng dụng P2P LAN File Sharing.
 */
public class MainApplication extends Application {

    private static final String MAIN_VIEW = "/com/nhannv/p2p/layout/MainView.fxml";
    private static final String APP_TITLE = "P2P LAN File Sharing - VKU";
    private static final double DEFAULT_WIDTH = 800;
    private static final double DEFAULT_HEIGHT = 600;

    /** Giữ tham chiếu Controller để giải phóng tài nguyên mạng khi ứng dụng tắt. */
    private MainController mainController;

    @Override
    public void start(Stage stage) throws IOException {
        URL viewUrl = MainApplication.class.getResource(MAIN_VIEW);
        if (viewUrl == null) {
            throw new IllegalStateException("Không tìm thấy giao diện: " + MAIN_VIEW);
        }

        // Dùng instance FXMLLoader (thay vì FXMLLoader.load tĩnh) để lấy được Controller.
        FXMLLoader loader = new FXMLLoader(viewUrl);
        Parent root = loader.load();
        mainController = loader.getController();

        Scene scene = new Scene(root, DEFAULT_WIDTH, DEFAULT_HEIGHT);
        stage.setTitle(APP_TITLE);
        stage.setScene(scene);
        stage.setMinWidth(640);
        stage.setMinHeight(480);
        stage.show();
    }

    /**
     * JavaFX gọi hàm này khi cửa sổ cuối cùng bị đóng (bấm X) hoặc gọi Platform.exit().
     * Tại đây dừng DiscoveryService: gửi BYE, dừng các luồng nền và đóng socket UDP
     * để không rò rỉ luồng và không kẹt port 8888 cho lần chạy sau.
     */
    @Override
    public void stop() {
        if (mainController != null) {
            mainController.shutdown();
        }
    }

    public static void main(String[] args) {
        launch(args);
    }
}
