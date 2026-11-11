package com.nhannv.p2p;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;
import java.net.URL;

/**
 * Entry point of the P2P LAN File Sharing desktop application.
 */
public class MainApplication extends Application {

    private static final String MAIN_VIEW = "/com/nhannv/p2p/layout/MainView.fxml";
    private static final String APP_TITLE = "P2P LAN File Sharing - VKU";
    private static final double DEFAULT_WIDTH = 800;
    private static final double DEFAULT_HEIGHT = 600;

    @Override
    public void start(Stage stage) throws IOException {
        URL viewUrl = MainApplication.class.getResource(MAIN_VIEW);
        if (viewUrl == null) {
            throw new IllegalStateException("Không tìm thấy giao diện: " + MAIN_VIEW);
        }

        Parent root = FXMLLoader.load(viewUrl);
        Scene scene = new Scene(root, DEFAULT_WIDTH, DEFAULT_HEIGHT);

        stage.setTitle(APP_TITLE);
        stage.setScene(scene);
        stage.setMinWidth(640);
        stage.setMinHeight(480);
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
