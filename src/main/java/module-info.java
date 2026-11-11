module com.p2pfilesharing {
    requires javafx.controls;
    requires javafx.fxml;
    requires java.logging;
    requires atlantafx.base;

    // P2P application packages
    opens com.nhannv.p2p.ui to javafx.fxml;
    exports com.nhannv.p2p;
    exports com.nhannv.p2p.model;
    exports com.nhannv.p2p.network;
    exports com.nhannv.p2p.util;
}
