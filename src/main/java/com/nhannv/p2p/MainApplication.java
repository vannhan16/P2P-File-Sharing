package com.nhannv.p2p;

import atlantafx.base.theme.NordDark;
import atlantafx.base.theme.PrimerLight;
import com.nhannv.p2p.ui.MainController;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;
import java.net.URL;

/**
 * Điểm khởi chạy ứng dụng P2P LAN File Sharing (Milestone 4 - Final Technical Milestone).
 * <p>
 * Tích hợp thư viện giao diện hiện đại <b>AtlantaFX</b> (hỗ trợ chuyển đổi trực tiếp giữa
 * {@link PrimerLight} và {@link NordDark}).
 */
public class MainApplication extends Application {

    private static final String MAIN_VIEW = "/com/nhannv/p2p/layout/MainView.fxml";
    private static final String APP_TITLE = "P2P LAN File Sharing - VKU (Pause/Resume & Swarm Edition)";
    private static final double DEFAULT_WIDTH = 1080;
    private static final double DEFAULT_HEIGHT = 720;

    private static boolean darkThemeActive = false;

    /** Giữ tham chiếu Controller để giải phóng tài nguyên mạng khi ứng dụng tắt. */
    private MainController mainController;

    @Override
    public void start(Stage stage) throws IOException {
        // Khoác giao diện hiện đại AtlantaFX (mặc định Primer Light, có nút đổi sang Nord Dark trên UI).
        Application.setUserAgentStylesheet(new PrimerLight().getUserAgentStylesheet());

        URL viewUrl = MainApplication.class.getResource(MAIN_VIEW);
        if (viewUrl == null) {
            throw new IllegalStateException("Không tìm thấy giao diện: " + MAIN_VIEW);
        }

        FXMLLoader loader = new FXMLLoader(viewUrl);
        Parent root = loader.load();
        mainController = loader.getController();

        Scene scene = new Scene(root, DEFAULT_WIDTH, DEFAULT_HEIGHT);
        stage.setTitle(APP_TITLE);
        stage.setScene(scene);
        stage.setMinWidth(860);
        stage.setMinHeight(600);
        stage.show();
    }

    /** Chuyển đổi qua lại giữa giao diện Primer Light và Nord Dark của AtlantaFX. */
    public static boolean toggleTheme() {
        darkThemeActive = !darkThemeActive;
        if (darkThemeActive) {
            Application.setUserAgentStylesheet(new NordDark().getUserAgentStylesheet());
        } else {
            Application.setUserAgentStylesheet(new PrimerLight().getUserAgentStylesheet());
        }
        return darkThemeActive;
    }

    /**
     * Đóng toàn bộ Sockets/Threads an toàn khi người dùng bấm dấu X tắt ứng dụng.
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
