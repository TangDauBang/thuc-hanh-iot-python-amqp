# BÁO CÁO THỰC HÀNH: LẬP TRÌNH PYTHON VỚI GIAO THỨC AMQP (RABBITMQ)

Dự án mô phỏng các bài toán truyền nhận thông điệp trong hệ thống IoT sử dụng giao thức **AMQP (Advanced Message Queuing Protocol)** và thư viện **Pika** trên nền tảng **RabbitMQ**.

---

## 1. Broker Sử Dụng

* **Nền tảng Broker:** CloudAMQP (RabbitMQ as a Service - Hosted trên Cloud).
* **Giao thức kết nối:** AMQPS (AMQP bảo mật qua TLS/SSL - Port 5671).
* **Cấu hình kết nối:** Được lưu trữ trong file `env` và `.env` (hoặc biến môi trường `AMQP_URL`).
  ```env
  AMQP_URL=amqps://yaydosas:YwVneW6ksfDWnEFN3IZDcW1Rdj4J0Gb_@fuji.lmq.cloudamqp.com/yaydosas
  ```
  *(Các chương trình đều có cơ chế tự động đọc từ file `env`/`.env` hoặc sử dụng URL mặc định, không cần cấu hình phức tạp).*

---

## 2. Cài Đặt Môi Trường

Yêu cầu máy tính đã cài đặt Python 3.8+ (khuyên dùng Python 3.10 trở lên).

Cài đặt thư viện AMQP cần thiết:
```bash
pip install pika python-dotenv
```

---

## 3. Cấu Trúc Dự Án

```
thuc-hanh-iot-python-amqp/
│
├── env                       # File chứa cấu hình AMQP_URL
├── .env                      # File môi trường chuẩn
├── producer_bai1.py          # Bài 1: Producer gửi thông điệp cơ bản
├── consumer_bai1.py          # Bài 1: Consumer nhận và in thời gian
│
├── sensor_producer_bai2.py   # Bài 2: Mô phỏng cảm biến IoT gửi JSON mỗi 3s
├── monitor_consumer_bai2.py  # Bài 2: Consumer phân tích ngưỡng và cảnh báo
│
├── alert_producer_bai3.py    # Bài 3: Điều phối cảnh báo qua Direct Exchange
├── warning_consumer_bai3.py  # Bài 3: Consumer chỉ nhận mức WARNING
├── critical_consumer_bai3.py # Bài 3: Consumer chỉ nhận mức CRITICAL
│
├── README.md                 # Tài liệu hướng dẫn và báo cáo kết quả
└── Thực hành python amqp.docx # Đề bài thực hành
```

---

## 4. Hướng Dẫn Chạy Từng Bài & Kết Quả Đạt Được

### BÀI 1: Gửi và nhận message cơ bản qua Queue (`iot_lab_queue`)

#### Mục tiêu:
Làm quen với mô hình cơ bản: `Producer -> Queue -> Consumer`.

#### Cách chạy:
1. Mở một cửa sổ Terminal thứ nhất và chạy Consumer để lắng nghe:
   ```bash
   python consumer_bai1.py
   ```
2. Mở một cửa sổ Terminal thứ hai và chạy Producer để gửi tin nhắn:
   ```bash
   python producer_bai1.py
   ```
   *Chọn `1` để gửi thông điệp mặc định, `2` để tự nhập thông điệp, hoặc `3` để gửi liên tiếp.*

#### Kết quả đạt được:
* **Producer:**
  ```text
  === BÀI 1: PRODUCER (Gửi message qua queue) ===
  Đang kết nối tới RabbitMQ Broker...
  [x] Đã gửi thành công vào queue 'iot_lab_queue':
      -> Xin chao tu ung dung Python AMQP - B21DCCN001 - Nguyen Van A
  [OK] Đã hoàn thành và đóng kết nối.
  ```
* **Consumer:**
  ```text
  === BÀI 1: CONSUMER (Lắng nghe message từ queue) ===
  [*] Đang lắng nghe trên queue 'iot_lab_queue'...
  Da nhan message: Xin chao tu ung dung Python AMQP - B21DCCN001 - Nguyen Van A
  Thoi gian nhan: 00:50:20
  --------------------------------------------------
  ```

---

### BÀI 2: Mô phỏng cảm biến IoT gửi dữ liệu môi trường (`sensor_data_queue`)

#### Mục tiêu:
Mô phỏng thiết bị IoT gửi telemetry định dạng JSON tuần hoàn mỗi 3 giây, Consumer tự động phân tích và kích hoạt cảnh báo khi vượt ngưỡng:
* Nhiệt độ > 35°C $\rightarrow$ `"CANH BAO: Nhiet do cao"`
* Độ ẩm < 40% $\rightarrow$ `"CANH BAO: Do am thap"`

#### Cách chạy:
1. Mở Terminal 1 chạy Monitoring Consumer:
   ```bash
   python monitor_consumer_bai2.py
   ```
2. Mở Terminal 2 chạy Sensor Producer:
   ```bash
   python sensor_producer_bai2.py
   ```

#### Kết quả đạt được:
* **Sensor Producer:**
  ```text
  [*] Bắt đầu gửi dữ liệu cảm biến vào queue 'sensor_data_queue' mỗi 3 giây...
  [#1 Lúc 2026-10-15 00:51:00] Gửi thành công: {"device_id": "sensor01", "temperature": 36.4, "humidity": 38.9, "timestamp": "2026-10-15 00:51:00"}
  [#2 Lúc 2026-10-15 00:51:03] Gửi thành công: {"device_id": "sensor02", "temperature": 28.2, "humidity": 65.4, "timestamp": "2026-10-15 00:51:03"}
  ```
* **Monitoring Consumer:**
  ```text
  Device: sensor01
  Temperature: 36.4
  Humidity: 38.9
  Time: 2026-10-15 00:51:00
  CANH BAO: Nhiet do cao
  CANH BAO: Do am thap
  ----------------------------------------
  Device: sensor02
  Temperature: 28.2
  Humidity: 65.4
  Time: 2026-10-15 00:51:03
  Trang thai: Binh thuong
  ----------------------------------------
  ```

---

### BÀI 3: Điều phối cảnh báo IoT với Direct Exchange (`iot_alert_exchange`)

#### Mục tiêu:
Sử dụng **Direct Exchange** và **Routing Key** (`info`, `warning`, `critical`) để định tuyến thông điệp chính xác tới các queue mong muốn:
* `warning_queue`: Chỉ nhận thông điệp có routing key `warning`.
* `critical_queue`: Chỉ nhận thông điệp có routing key `critical`.
* Message mức `info`: Bị bỏ qua vì không có queue nào bind với routing key `info`.

#### Cách chạy:
1. Mở Terminal 1 chạy Warning Consumer:
   ```bash
   python warning_consumer_bai3.py
   ```
2. Mở Terminal 2 chạy Critical Consumer:
   ```bash
   python critical_consumer_bai3.py
   ```
3. Mở Terminal 3 chạy Alert Producer:
   ```bash
   python alert_producer_bai3.py
   ```
   *Bấm Enter để gửi cùng lúc 3 mức cảnh báo (info, warning, critical).*

#### Kết quả đạt được:
* **Alert Producer:**
  ```text
  [*] Đang gửi 3 thông điệp mẫu...
  [x] Đã gửi [Routing Key: 'info'] -> 'He thong giam sat IoT khoi dong thanh cong'
  [x] Đã gửi [Routing Key: 'warning'] -> 'Nhiet do phong may vuot nguong warning'
  [x] Đã gửi [Routing Key: 'critical'] -> 'Cam bien kho lanh mat ket noi critical'
  ```
* **Warning Consumer (`warning_queue`):**
  ```text
  [warning_queue] Da nhan: Nhiet do phong may vuot nguong warning
                  (Thoi gian: 00:50:55, Routing Key: warning)
  --------------------------------------------------
  ```
* **Critical Consumer (`critical_queue`):**
  ```text
  [critical_queue] Da nhan: Cam bien kho lanh mat ket noi critical
                   (Thoi gian: 00:50:55, Routing Key: critical)
  --------------------------------------------------
  ```
* **Đánh giá định tuyến:**
  Thông điệp `info` không xuất hiện ở cả 2 consumer trên, chứng minh cơ chế Direct Exchange định tuyến hoàn toàn chính xác theo routing key.

---

## 5. Tổng Kết

* Hệ thống hoàn thành 100% các yêu cầu từ file tài liệu thực hành.
* Các chương trình hoạt động ổn định, có xử lý ngoại lệ ngắt phím (`Ctrl+C`), xử lý lỗi font encoding tiếng Việt trên Windows terminal.
* Sử dụng CloudAMQP giúp triển khai nhanh chóng, bảo mật qua AMQPS, không phụ thuộc vào Docker hay cài đặt phần mềm cục bộ.