module com.p2pfilesharing {
    requires javafx.controls;
    requires javafx.fxml;


    opens com.p2pfilesharing to javafx.fxml;
    exports com.p2pfilesharing;
}