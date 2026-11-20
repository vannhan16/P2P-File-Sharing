# KẾ HOẠCH KIẾN TRÚC TÍCH HỢP TRÍ TUỆ NHÂN TẠO (AI-AUGMENTED P2P FILE SHARING)

> **Vai trò:** Principal Software Architect & AI Engineer  
> **Đối tượng áp dụng:** Hệ thống chia sẻ tệp phi tập trung P2P (`com.nhannv.p2p`)  
> **Triết lý thiết kế:** **Thực dụng (Pragmatic) — Phi tập trung (Decentralized) — Chịu lỗi Offline (Offline-First Graceful Degradation) — Không làm nghẽn luồng I/O (Non-blocking Async).**  
> Tuyệt đối không tích hợp Chatbot trò chuyện phiếm; mọi mô-đun AI đều phục vụ trực tiếp cho bài toán **Tìm kiếm tệp thông minh**, **Tiết kiệm băng thông mạng LAN/Tunnel** và **Chẩn đoán sự cố truyền tải tầng Socket**.

---

## 1. ĐÁNH GIÁ & ĐỀ XUẤT CÁC TÍNH NĂNG AI ĐÁNG GIÁ NHẤT CHO P2P

Dựa trên kiến trúc hiện tại của dự án (`SharedFolderManager`, `P2PServer`, `DownloadManager`, `SwarmDownloadTask`, `MainController`), dưới đây là phân tích chuyên sâu cho **3 tính năng cốt lõi** và **1 tính năng bảo mật bổ trợ**:

### 1.1. Tính năng 1: Tự động gắn thẻ & Tìm kiếm ngữ nghĩa (Smart Auto-Tagging & Semantic Search)

* **Vấn đề thực tế trong mạng P2P LAN:**
  Trong môi trường học tập/doanh nghiệp, các Peer thường đặt tên tệp thiếu quy chuẩn (ví dụ: `BaiTap1_final_v2.pdf`, `code_nop_thay.zip`, `slide_tuan5.pptx`). Cơ chế lọc hiện tại trong [`MainController.matchesSearchKeyword(...)`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/java/com/nhannv/p2p/ui/MainController.java#L1011-L1019) chỉ dùng `String.contains(keyword)` thuần túy nên nếu sinh viên gõ *"lập trình socket đa luồng"* sẽ không thể tìm thấy tệp `Lab03_Network.java` hoặc `Chuong4_TCP_UDP.pdf`.
* **Giải pháp Kỹ thuật & Tích hợp vào Luồng Code:**
  1. **Điểm kích hoạt (Trigger Point):** Tích hợp trực tiếp vào luồng nền của [`SharedFolderManager.java`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/java/com/nhannv/p2p/util/SharedFolderManager.java) ngay sau khi `WatchService` (hoặc `scanAndHashSync`) tính xong mã băm `sha256Hash` của tệp mới.
  2. **Trích xuất ngữ cảnh siêu nhẹ (`FileContentExtractor`):**
     - Chỉ đọc tối đa **4 KB đầu tiên** (không bao giờ nạp toàn bộ tệp vào RAM):
       + Với tệp văn bản/mã nguồn (`.txt`, `.md`, `.java`, `.py`, `.c`, `.cpp`, `.json`, `.xml`, `.sql`, `.csv`, `.log`): Đọc 4 KB đầu bằng `BufferedReader` chuẩn UTF-8.
       + Với tệp `.docx` / `.pptx` (bản chất là gói ZIP chứa XML): Dùng `ZipInputStream` đọc trực tiếp entry `word/document.xml` hoặc `ppt/slides/slide1.xml`, loại bỏ thẻ XML lấy 1.500 ký tự đầu mà **không cần thêm thư viện Apache POI nặng hàng chục MB**.
       + Với tệp `.zip`: Đọc danh sách tên các entry bên trong gói ZIP (`ZipEntry.getName()`) làm ngữ cảnh cấu trúc dự án.
       + Với tệp nhị phân/media/PDF phức tạp: Kết hợp tên tệp, phần mở rộng, kích thước và các chuỗi ASCII đọc được ở phần đầu.
  3. **Sinh Metadata ngữ nghĩa:** AI trả về cấu trúc JSON gọn nhẹ gồm:
     - `aiTags`: Danh sách 3–5 từ khóa ngữ nghĩa không dấu & có dấu (ví dụ: `["lap-trinh-mang", "tcp-socket", "da-luong", "java-nio"]`).
     - `aiSummary`: Mô tả tóm tắt 1–2 câu (tối đa 160 ký tự) về nội dung tệp.
  4. **Tìm kiếm ngữ nghĩa hai tầng (Two-Tier Semantic Search):**
     - **Tầng 1 (Instant Local Filter - 0ms):** Mở rộng `matchesSearchKeyword(SharedFile file, String keyword)` để so khớp đồng thời trên `fileName`, `ownerName`, `sha256Hash`, **`aiTags`** và **`aiSummary`** (kèm chuẩn hóa bỏ dấu tiếng Việt).
     - **Tầng 2 (AI Semantic Ranking khi bấm Enter hoặc nút `AI Search`):** Khi người dùng nhập câu truy vấn tự nhiên (ví dụ: *"báo cáo thực nghiệm tốc độ tải đa nguồn tuần trước"*), `AIService` đối chiếu câu truy vấn với danh mục siêu dữ liệu (`fileName + aiTags + aiSummary`) của toàn bộ các tệp trên mạng LAN và xếp hạng các tệp phù hợp nhất lên đầu bảng `sharedFileTable`.

---

### 1.2. Tính năng 2: Tóm tắt tệp trước khi tải theo yêu cầu (Pre-download AI Preview / On-Demand Inspection)

* **Vấn đề thực tế:**
  Khi kết nối xuyên Internet qua TCP Tunnel (`Ngrok` / `Playit.gg`) hoặc mạng LAN ảo (`Radmin VPN`), băng thông có thể bị giới hạn. Người dùng thấy một tệp `SourceCode_DoAn.zip` (250 MB) hoặc `TaiLieuOnThi.docx` (15 MB) trên máy bạn cùng nhóm nhưng không chắc có đúng phiên bản mình cần hay không.
* **Giải pháp Kỹ thuật & Tích hợp vào Luồng Code:**
  1. **Tận dụng sẵn lệnh `DOWNLOAD_CHUNK` hoặc lệnh mới `PREVIEW_SNIPPET` trong [`P2PServer.java`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/java/com/nhannv/p2p/network/P2PServer.java):**
     - Nếu Peer sở hữu tệp đã có sẵn `aiSummary` từ bước Auto-Tagging, thông tin tóm tắt đã có mặt ngay trong `SharedFile` mà không tốn thêm kết nối.
     - Nếu người dùng muốn xem **Tóm tắt chuyên sâu trước khi tải (Deep Pre-download Preview)**, ứng dụng chỉ gửi lệnh `DOWNLOAD_CHUNK` với `startOffset = 0` và `length = 8192` (chỉ tải đúng **8 KB đầu tiên** của tệp — hoặc đọc danh mục file nếu là `.zip`) từ Peer đích, sau đó đưa qua `AIService` của máy người nhận để phân tích chi tiết:
       + *Nội dung chính của tệp là gì?*
       + *Cấu trúc / các hàm chính (nếu là code) hoặc các mục chính (nếu là tài liệu)?*
       + *Khuyến nghị có nên tải toàn bộ tệp hay không?*
  2. **Giá trị mạng cốt lõi:** Thay vì tốn 250 MB băng thông để tải thử, người dùng chỉ tốn **8 KB (tiết kiệm 99,99% băng thông)** để biết chính xác nội dung bên trong tệp của Peer đối tác.

---

### 1.3. Tính năng 3: Chẩn đoán lỗi mạng & Tối ưu băng thông thông minh (AI Network & Telemetry Diagnostics)

* **Vấn đề thực tế:**
  Trong quá trình vận hành P2P thực tế giữa nhiều máy Windows (LAN + Radmin VPN + Firewall + TCP Tunnel), người dùng thường gặp các lỗi kỹ thuật phức tạp trong `networkLogArea`:
  - `Connection timed out: no further information` trên dải `26.x.x.x:8889` (do Windows xếp card Radmin VPN vào Public Firewall Profile).
  - `BindException: Address already in use` trên cổng `8888` hoặc `8889`.
  - `AUTH_FAILED (-401)` hoặc `EOFException` khi đang tải Swarm đa nguồn mà 1 Peer bị đứt kết nối.
  - Biểu đồ tốc độ `AreaChart` bị dao động mạnh hoặc tụt xuống `< 0.5 MB/s` do nghẽn luồng hoặc giới hạn QoS.
* **Giải pháp Kỹ thuật & Tích hợp vào Luồng Code:**
  1. **Thu thập ngữ cảnh hệ thống thực tế (`NetworkDiagnosticSnapshot`):**
     Khi người dùng bấm nút **"Chẩn đoán Mạng (AI)"** (hoặc khi có lỗi đỏ xuất hiện), hệ thống tự động gom một gói dữ liệu chẩn đoán thực tế gồm:
     - 25 dòng log gần nhất từ `networkLogArea`.
     - Danh sách card mạng vật lý & Radmin VPN từ `DiscoveryService.collectPhysicalLanSubnets()`.
     - Trạng thái Windows Firewall từ `FirewallUtil.isFirewallRuleConfigured()`.
     - Số lượng Peers hiện tại, độ trễ Ping RTT (`latencyMs`) của từng Peer, cấu hình QoS `BandwidthLimiter` hiện tại và thống kê từ `historyList`.
  2. **Phân tích & Đề xuất Hành động cụ thể:**
     AI kết hợp với bộ quy tắc chuyên gia (Rule-based + LLM) để trả về 3 mục rõ ràng bằng tiếng Việt:
     - **Nguyên nhân gốc rễ (Root Cause):** Chỉ đích danh lỗi (ví dụ: *"Card Radmin VPN 26.45.12.88 đang mở nhưng kết nối TCP 8889 tới 26.45.19.102 bị timeout 4000ms -> Máy đối tác chưa mở Inbound Rule cho Public Profile"*).
     - **Cách khắc phục từng bước (Actionable Fix):** Hướng dẫn thao tác ngay trên giao diện (ví dụ: *"Bấm nút 'Firewall (1-Click)' ở thanh Header trên máy đối tác"* hoặc *"Tắt giới hạn băng thông QoS đang để ở mức 512 KB/s"*).
     - **Khuyến nghị tối ưu hóa Swarm:** Gợi ý kích thước chunk hoặc loại bỏ Peer có RTT `> 250 ms` khỏi nhóm Swarm.

---

### 1.4. Tính năng Đề xuất Bổ sung (Bonus): Kiểm duyệt An toàn Tệp Chia sẻ (AI Security & Sensitive Data Guard)

* **Giá trị thực tế:** Trước khi một tệp mới trong thư mục `Shared/` được quảng bá ra toàn mạng LAN qua lệnh `LIST_FILES`, bộ trích xuất 4 KB đầu sẽ kiểm tra nhanh xem tệp có vô tình chứa **Khóa bí mật, Mật khẩu, API Key, Private Key (`-----BEGIN RSA PRIVATE KEY-----`), hoặc chuỗi kết nối Database** hay không. Nếu phát hiện, hệ thống gắn nhãn cảnh báo màu cam `.pill-warning` (`Sensitive Content Warning`) để chủ máy biết và tránh lộ mật khẩu cho các máy khác trong phòng thực hành.

---

## 2. KIẾN TRÚC KỸ THUẬT & XỬ LÝ BÀI TOÁN OFFLINE (STRATEGY PATTERN + SHA-256 CACHE)

Để tuân thủ nghiêm ngặt quy tắc trong [`.agents/rules/p2p-project-context.md`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/.agents/rules/p2p-project-context.md) (*Phân tách MVC chặt chẽ, không block UI, xử lý triệt để ngoại lệ khi mất mạng*), toàn bộ phân hệ AI được đóng gói độc lập trong package `com.nhannv.p2p.ai`.

```mermaid
classDiagram
    class AIService {
        <<interface>>
        +isAvailable() boolean
        +providerName() String
        +analyzeFileMetadata(File file, String sha256) CompletableFuture~AIMetadata~
        +semanticRankFiles(String query, List~SharedFile~ candidates) CompletableFuture~List~SharedFile~~
        +generatePreDownloadPreview(String fileName, String snippetText) CompletableFuture~String~
        +diagnoseNetworkIssue(NetworkDiagnosticContext ctx) CompletableFuture~String~
    }

    class GeminiAIService {
        -HttpClient httpClient
        -String apiKey
        -String modelName
        +analyzeFileMetadata(...)
    }

    class OllamaAIService {
        -HttpClient httpClient
        -String localEndpoint
        -String localModel
        +analyzeFileMetadata(...)
    }

    class HeuristicOfflineAIService {
        +analyzeFileMetadata(...)
        +diagnoseNetworkIssue(...)
    }

    class AIManager {
        -AIService primaryService
        -AIService ollamaService
        -AIService offlineFallback
        -AICacheStore cacheStore
        -ExecutorService aiWorkerPool
        +resolveActiveService() AIService
    }

    class AICacheStore {
        -Path cacheFilePath
        -ConcurrentHashMap~String, AIMetadata~ memoryCache
        +getBySha256(String sha256) Optional~AIMetadata~
        +putBySha256(String sha256, AIMetadata meta) void
    }

    AIService <|.. GeminiAIService
    AIService <|.. OllamaAIService
    AIService <|.. HeuristicOfflineAIService
    AIManager --> AIService
    AIManager --> AICacheStore
```

### 2.1. Đa chiến lược thực thi (`Strategy Pattern` + `Chain of Responsibility`)

1. **`GeminiAIService` (Cloud REST API — Java 17+ `java.net.http.HttpClient` thuần):**
   - Sử dụng `HttpClient` chuẩn có sẵn trong JDK (`java.net.http`), không cần kéo thêm SDK nặng nề.
   - Đọc API Key từ biến môi trường `GEMINI_API_KEY`, tệp cấu hình cục bộ `.p2p-ai-config.properties`, hoặc hộp thoại cài đặt nhanh trên UI.
   - Gọi endpoint `gemini-2.5-flash` (hoặc `gemini-2.0-flash`) với `connectTimeout = 3s`, `requestTimeout = 6s`.
2. **`OllamaAIService` (100% Local Offline LAN AI — `http://localhost:11434/api/generate`):**
   - Khi chạy trong phòng máy thực hành hoàn toàn không có Internet, nếu máy có bật Ollama cục bộ (với các model nhẹ như `qwen2.5:1.5b`, `llama3.2:1b` hoặc `gemma2:2b`), hệ thống tự động kết nối `localhost:11434` để xử lý 100% nội bộ, đảm bảo quyền riêng tư tuyệt đối.
3. **`HeuristicOfflineAIService` (Zero-Dependency Local Engine — Luôn sẵn sàng 100%):**
   - **Đảm bảo Graceful Degradation tuyệt đối:** Ngay cả khi máy **không có Internet**, **không có Gemini API Key** và **không cài Ollama**, ứng dụng vẫn không bao giờ báo lỗi hay vô hiệu hóa trống rỗng!
   - `HeuristicOfflineAIService` sử dụng bộ phân tích từ khóa cục bộ (TF-IDF rút gọn, phân tích cú pháp AST/header của file `.java`/`.py`/`.sql`/`.docx`/`.zip`, và bộ cây quyết định chẩn đoán lỗi Socket/Firewall) để vẫn sinh được `aiTags`, trích xuất tóm tắt cấu trúc file và chẩn đoán chính xác các lỗi `BindException`, `ConnectException`, `AUTH_FAILED` trong 1ms!
   - Trên thanh Header / AI Pill sẽ hiển thị rõ chế độ đang chạy: `AI: Gemini Cloud`, `AI: Ollama Local`, hoặc `AI: Local Heuristic (Offline)`.

### 2.2. Cơ chế Caching theo Mã băm Nội dung (`SHA-256 Content-Addressed Cache`)

* **Nguyên lý:** Trong hệ thống P2P của chúng ta, mỗi tệp trong thư mục `Shared/` đều đã được [`HashUtil.calculateSHA256(File)`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/java/com/nhannv/p2p/util/HashUtil.java) tính mã băm 64 ký tự hex duy nhất.
* **Thiết kế `AICacheStore` (`.p2p-ai-cache.json` / `.properties`):**
  - Khóa chính (Primary Key) của Cache chính là **`sha256Hash`** của tệp (không phụ thuộc vào tên tệp hay đường dẫn).
  - Nếu một tệp được đổi tên hoặc sao chép sang máy khác nhưng nội dung không đổi (`sha256Hash` giữ nguyên), `AICacheStore.getBySha256(sha256)` trả về ngay `AIMetadata(aiTags, aiSummary, sensitiveWarning)` trong **0.1 ms** với **0 token API**!
  - Luồng xử lý của `WatchService`:
    1. Phát hiện file mới -> Tính `SHA-256` -> Đưa file lên bảng `sharedFileTable` **ngay lập tức** (0ms độ trễ cho tính năng P2P cốt lõi).
    2. Kiểm tra `AICacheStore`: Nếu đã có `sha256Hash` -> Gắn luôn `aiTags` và `aiSummary`.
    3. Nếu chưa có trong Cache -> Đẩy tác vụ vào một `ExecutorService` riêng biệt (`p2p-ai-background-worker`, độ ưu tiên thấp, `CompletableFuture`) để gọi `AIService`. Khi có kết quả, lưu vào `.p2p-ai-cache.json` và cập nhật dòng tương ứng trên `sharedFileTable` qua `Platform.runLater()`.

---

## 3. CẬP NHẬT GIAO THỨC TCP & GIAO DIỆN (UI/UX)

### 3.1. Mở rộng Model [`SharedFile.java`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/java/com/nhannv/p2p/model/SharedFile.java) & Giao thức `LIST_FILES` Tương thích ngược 100%

Để truyền kèm `aiTags` và `aiSummary` qua lệnh TCP `LIST_FILES` mà không làm nặng gói tin và không gây lỗi khi giao tiếp với các phiên bản Peer cũ:

1. **Mở rộng `record SharedFile`:**
   ```java
   public record SharedFile(
           String fileName,
           long fileSize,
           String sha256Hash,
           String ownerIp,
           String ownerName,
           List<String> peerIps,
           String aiTags,      // Ví dụ: "java-nio, socket, bao-cao" (tối đa 80 ký tự)
           String aiSummary    // Ví dụ: "Báo cáo đồ án lập trình mạng P2P" (tối đa 160 ký tự)
   )
   ```
   *(Giữ nguyên toàn bộ các constructor 5 tham số và 6 tham số hiện có để không phá vỡ bất kỳ dòng code nào của Milestone 1–6).*
2. **Định dạng Wire Protocol (`toWireLine` / `fromWireLine`):**
   - Hiện tại: `fileName|fileSize|sha256Hash|ownerIp|ownerName` (5 trường).
   - Nâng cấp: `fileName|fileSize|sha256Hash|ownerIp|ownerName|aiTags|aiSummary` (7 trường, loại bỏ ký tự `|` và xuống dòng, giới hạn tổng độ dài thêm `< 240 bytes/file`).
   - Trong `fromWireLine(String line, String fallbackIp)`: Dùng `line.split("\\|", 7)`. Nếu `parts.length >= 5` vẫn đọc bình thường; nếu `parts.length == 7` thì lấy thêm `parts[5]` (`aiTags`) và `parts[6]` (`aiSummary`). Khi gộp Swarm (`mergeWith`), nếu một Peer có `aiTags`/`aiSummary` đầy đủ hơn thì ưu tiên giữ lại bản mô tả giàu thông tin nhất.

### 3.2. Thiết kế Giao diện Tinh tế theo Chuẩn [`.agents/skills/javafx-pro-ui/SKILL.md`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/.agents/skills/javafx-pro-ui/SKILL.md)

Tuyệt đối không nhồi nhét thêm các khung văn bản to thô kệch làm vỡ bố cục 4 vùng hiện tại. Các thành phần AI được đặt tinh tế như sau:

1. **Trên Thanh Top Header Bar (`52px`):**
   - Thêm 1 **AI Status Pill** nhỏ gọn cạnh `PIN: OFF`: `AI: Ready` (class `.network-pill, .pill-sync`, icon `Feather.CPU`). Khi bấm vào Pill này sẽ mở hộp thoại cấu hình nhanh (nhập Gemini API Key, chọn Ollama `localhost:11434`, hoặc chế độ Offline Heuristic).
2. **Trên Thanh Tìm kiếm & Bảng `sharedFileTable` (Tab 1):**
   - Ô tìm kiếm `searchTextField` tự động hỗ trợ tìm theo cả **Tên tệp + Thẻ ngữ nghĩa (`aiTags`) + Nội dung tóm tắt (`aiSummary`)**.
   - Trong **Cột 1 (`Tên tệp / Thư mục nén`)**:
     + Hiển thị Tên tệp kèm các **Tag Pills siêu gọn** ngay bên cạnh hoặc trong Tooltip, và khi di chuột vào dòng tệp sẽ hiện `aiSummary` rõ ràng.
   - Bổ sung nút phụ `Tóm tắt AI` (class `button-outlined`, icon `Feather.ZAP`) trên thanh Toolbar của Tab 1: Khi người dùng chọn 1 tệp của Peer khác và bấm `Tóm tắt AI`, hệ thống lấy nhanh 8 KB đầu qua `DOWNLOAD_CHUNK` (hoặc đọc `aiSummary` có sẵn) và hiển thị bản tóm tắt trực quan ngay trong bảng thông tin chi tiết mà không cần tải cả file nặng.
3. **Trên Khu vực Nhật ký Sự kiện Mạng (`networkLogArea`):**
   - Ngay góc phải phía trên khung `NHẬT KÝ SỰ KIỆN MẠNG THỜI GIAN THỰC`, thêm một nút nhỏ gọn `Chẩn đoán Lỗi Mạng (AI)` (class `flat`, icon `Feather.ACTIVITY`). Khi bấm vào, AI đọc log thực tế + trạng thái Firewall/RTT và in bản phân tích nguyên nhân + hướng dẫn khắc phục từng bước ngay trong khu vực chẩn đoán.

---

## 4. LỘ TRÌNH TRIỂN KHAI THEO TỪNG PHASE (IMPLEMENTATION ROADMAP)

### Phase 1: Core AI Engine, Strategy Pattern & SHA-256 Content Cache
* **Mục tiêu:** Xây dựng nền tảng AI bất đồng bộ, cơ chế tự động chuyển đổi `Gemini Cloud -> Ollama Local -> Heuristic Offline` và bộ nhớ đệm theo mã băm `SHA-256`.
* **Các tệp tạo mới:**
  1. `src/main/java/com/nhannv/p2p/ai/AIMetadata.java`: Record lưu `sha256Hash`, `tags`, `summary`, `sensitiveWarning`, `provider`.
  2. `src/main/java/com/nhannv/p2p/ai/AIService.java`: Interface chuẩn cho mọi nhà cung cấp AI.
  3. `src/main/java/com/nhannv/p2p/ai/GeminiAIService.java`: Kết nối Gemini REST API qua `java.net.http.HttpClient`.
  4. `src/main/java/com/nhannv/p2p/ai/OllamaAIService.java`: Kết nối Local LLM qua `http://localhost:11434`.
  5. `src/main/java/com/nhannv/p2p/ai/HeuristicOfflineAIService.java`: Bộ phân tích ngữ nghĩa & chẩn đoán mạng Offline 100% không cần Internet.
  6. `src/main/java/com/nhannv/p2p/ai/AICacheStore.java`: Quản lý lưu trữ và truy xuất cache theo `sha256Hash` tại `.p2p-ai-cache.properties`.
  7. `src/main/java/com/nhannv/p2p/ai/FileContentExtractor.java`: Trích xuất an toàn 4 KB đầu của tệp (`.txt`, `.java`, `.md`, `.json`, `.docx`, `.pptx`, `.zip`) và quét cảnh báo lộ khóa nhạy cảm.
  8. `src/main/java/com/nhannv/p2p/ai/AIManager.java`: Bộ điều phối trung tâm quản lý Thread Pool riêng cho AI và cơ chế Fallback.
* **Các tệp cập nhật:**
  - `src/main/java/module-info.java`: Bổ sung `requires java.net.http;` và `exports com.nhannv.p2p.ai;`.

### Phase 2: Smart Auto-Tagging, Wire Protocol Upgrade & Semantic Search
* **Mục tiêu:** Tự động gắn thẻ khi `WatchService` phát hiện tệp mới, truyền `aiTags` & `aiSummary` qua lệnh `LIST_FILES` cho các Peer khác trong LAN, và nâng cấp thanh tìm kiếm ngữ nghĩa.
* **Các tệp cập nhật:**
  1. [`src/main/java/com/nhannv/p2p/model/SharedFile.java`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/java/com/nhannv/p2p/model/SharedFile.java): Bổ sung `aiTags`, `aiSummary` và nâng cấp `toWireLine()` / `fromWireLine()` tương thích ngược.
  2. [`src/main/java/com/nhannv/p2p/util/SharedFolderManager.java`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/java/com/nhannv/p2p/util/SharedFolderManager.java): Kết nối `AIManager` để gắn thẻ ngầm sau khi băm SHA-256 và tra cứu `AICacheStore`.
  3. [`src/main/java/com/nhannv/p2p/ui/MainController.java`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/java/com/nhannv/p2p/ui/MainController.java): Nâng cấp `matchesSearchKeyword` (hỗ trợ tìm kiếm ngữ nghĩa không dấu/có dấu trên `aiTags` và `aiSummary`) và hiển thị Tag Badge trên bảng `sharedFileTable`.

### Phase 3: Pre-download 8KB AI Preview & Live Network Diagnostics trên Crafted UI
* **Mục tiêu:** Hoàn thiện tính năng xem trước nội dung tệp từ xa bằng 8 KB chunk đầu tiên (`DOWNLOAD_CHUNK`) và nút Chẩn đoán lỗi mạng thông minh từ log thực tế.
* **Các tệp cập nhật:**
  1. [`src/main/resources/com/nhannv/p2p/layout/MainView.fxml`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/resources/com/nhannv/p2p/layout/MainView.fxml) & [`src/main/resources/com/nhannv/p2p/css/app-theme.css`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/resources/com/nhannv/p2p/css/app-theme.css): Thêm `AI Status Pill`, nút `Xem trước / Tóm tắt AI` và nút `Chẩn đoán Mạng (AI)` theo đúng chuẩn Crafted Desktop UI.
  2. [`src/main/java/com/nhannv/p2p/ui/MainController.java`](file:///d:/NetworkProgramming/Netwwork/P2P-File-Sharing/src/main/java/com/nhannv/p2P/ui/MainController.java): Cài đặt luồng lấy 8 KB snippet từ Peer đối tác để tóm tắt trước khi tải và luồng thu thập `NetworkDiagnosticSnapshot` để chẩn đoán lỗi mạng.
