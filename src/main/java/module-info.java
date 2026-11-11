module com.p2pfilesharing {
    requires javafx.controls;
    requires javafx.fxml;
    requires java.logging;


    opens com.p2pfilesharing to javafx.fxml;
    exports com.p2pfilesharing;

    // P2P application packages
    opens com.nhannv.p2p.ui to javafx.fxml;
    exports com.nhannv.p2p;
    exports com.nhannv.p2p.model;
    exports com.nhannv.p2p.network;
    exports com.nhannv.p2p.util;
}
