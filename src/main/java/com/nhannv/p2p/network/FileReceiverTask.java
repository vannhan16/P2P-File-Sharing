package com.nhannv.p2p.network;

import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.util.SharedFolderManager;

import java.util.function.Consumer;

/**
 * Lớp giữ tương thích ngược với Milestone 2, kế thừa trực tiếp từ {@link P2PServer}
 * (hỗ trợ đầy đủ 3 lệnh {@code LIST_FILES}, {@code DOWNLOAD_FILE}, {@code PUSH_FILE}).
 */
public class FileReceiverTask extends P2PServer {

    public FileReceiverTask() {
        super(DEFAULT_PORT, new SharedFolderManager(), null);
    }

    public FileReceiverTask(int port, Consumer<SharedFile> onFileReceivedCallback) {
        super(port, new SharedFolderManager(), onFileReceivedCallback);
    }
}
