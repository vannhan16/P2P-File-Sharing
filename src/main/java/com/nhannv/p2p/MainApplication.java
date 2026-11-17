package com.nhannv.p2p;

import atlantafx.base.theme.PrimerDark;
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
 * Điểm khởi chạy ứng dụng P2P LAN &amp; Internet File Sharing (Crafted Desktop UI Edition).
 * <p>
 * Kết hợp hệ thống Design System 3-Tier Surface ({@code app-theme.css}) cùng nền tảng
 * <b>AtlantaFX</b> ({@link PrimerDark} / {@link PrimerLight}) và bộ Vector Icon <b>Ikonli Feather</b>.
 */
public class MainApplication extends Application {

    private static final String MAIN_VIEW = "/com/nhannv/p2p/layout/MainView.fxml";
    private static final String APP_CSS = "/com/nhannv/p2p/css/app-theme.css";
    private static final String APP_TITLE = "P2P LAN & Internet File Sharing — VKU Enterprise Edition";
    private static final double DEFAULT_WIDTH = 1320;
    private static final double DEFAULT_HEIGHT = 820;

    private static boolean darkThemeActive = true;
    private static Scene activeScene;

    /** Giữ tham chiếu Controller để giải phóng tài nguyên mạng khi ứng dụng tắt. */
    private MainController mainController;

    @Override
    public void start(Stage stage) throws IOException {
        // Mặc định khởi chạy ở chế độ PrimerDark chuẩn kỹ thuật (Surface 0 = #0D1117, Surface 1 = #161B22)
        Application.setUserAgentStylesheet(new PrimerDark().getUserAgentStylesheet());

        URL viewUrl = MainApplication.class.getResource(MAIN_VIEW);
        if (viewUrl == null) {
            throw new IllegalStateException("Không tìm thấy giao diện: " + MAIN_VIEW);
        }

        FXMLLoader loader = new FXMLLoader(viewUrl);
        Parent root = loader.load();
        mainController = loader.getController();

        Scene scene = new Scene(root, DEFAULT_WIDTH, DEFAULT_HEIGHT);
        URL cssUrl = MainApplication.class.getResource(APP_CSS);
        if (cssUrl != null) {
            scene.getStylesheets().add(cssUrl.toExternalForm());
        }
        activeScene = scene;

        stage.setTitle(APP_TITLE);
        stage.setScene(scene);
        stage.setMinWidth(980);
        stage.setMinHeight(660);
        stage.show();
    }

    /** Chuyển đổi qua lại giữa giao diện PrimerDark và PrimerLight kèm cập nhật lớp CSS. */
    public static boolean toggleTheme() {
        darkThemeActive = !darkThemeActive;
        if (darkThemeActive) {
            Application.setUserAgentStylesheet(new PrimerDark().getUserAgentStylesheet());
            if (activeScene != null && activeScene.getRoot() != null) {
                activeScene.getRoot().getStyleClass().remove("theme-light");
            }
        } else {
            Application.setUserAgentStylesheet(new PrimerLight().getUserAgentStylesheet());
            if (activeScene != null && activeScene.getRoot() != null
                    && !activeScene.getRoot().getStyleClass().contains("theme-light")) {
                activeScene.getRoot().getStyleClass().add("theme-light");
            }
        }
        return darkThemeActive;
    }

    public static boolean isDarkThemeActive() {
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
