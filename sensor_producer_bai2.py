import json
import os
import random
import sys
import time
from datetime import datetime
import pika

# Đảm bảo in tiếng Việt không bị lỗi encoding trên Windows console
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

def get_connection():
    """Lấy kết nối RabbitMQ Broker từ file env hoặc URL mặc định."""
    amqp_url = os.environ.get("AMQP_URL")
    if not amqp_url:
        for env_file in [".env", "env"]:
            if os.path.exists(env_file):
                with open(env_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("AMQP_URL="):
                            amqp_url = line.strip().split("=", 1)[1].strip("'\"")
                            break
            if amqp_url:
                break
    if not amqp_url:
        amqp_url = "amqps://yaydosas:YwVneW6ksfDWnEFN3IZDcW1Rdj4J0Gb_@fuji.lmq.cloudamqp.com/yaydosas"
    parameters = pika.URLParameters(amqp_url)
    return pika.BlockingConnection(parameters)

def main():
    queue_name = "sensor_data_queue"

    print("=== BÀI 2: SENSOR PRODUCER (Mô phỏng cảm biến IoT) ===")
    print("Đang kết nối tới RabbitMQ Broker...")
    connection = get_connection()
    channel = connection.channel()

    # Khai báo queue
    channel.queue_declare(queue=queue_name, durable=True)

    # Danh sách các thiết bị cảm biến mô phỏng (gợi ý mở rộng)
    device_list = ["sensor01", "sensor02", "sensor03"]

    print(f"[*] Bắt đầu gửi dữ liệu cảm biến vào queue '{queue_name}' mỗi 3 giây...")
    print("[*] Nhấn Ctrl+C để dừng mô phỏng.\n")

    counter = 1
    try:
        while True:
            # Chọn ngẫu nhiên hoặc luân phiên thiết bị
            device_id = random.choice(device_list)

            # Sinh ngẫu nhiên nhiệt độ (22.0 - 40.0) và độ ẩm (30.0 - 75.0)
            # để thỉnh thoảng kích hoạt ngưỡng cảnh báo (>35 độ hoặc <40%)
            temperature = round(random.uniform(22.0, 40.0), 1)
            humidity = round(random.uniform(30.0, 75.0), 1)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            payload = {
                "device_id": device_id,
                "temperature": temperature,
                "humidity": humidity,
                "timestamp": timestamp
            }

            message_json = json.dumps(payload, ensure_ascii=False)

            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=message_json.encode("utf-8"),
                properties=pika.BasicProperties(delivery_mode=2)
            )

            print(f"[#{counter} Lúc {timestamp}] Gửi thành công: {message_json}")
            counter += 1

            time.sleep(3)

    except KeyboardInterrupt:
        print("\n[!] Dừng gửi dữ liệu cảm biến.")
        connection.close()
        print("[OK] Đã đóng kết nối an toàn.")
        sys.exit(0)

if __name__ == "__main__":
    main()
