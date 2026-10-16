import json
import os
import sys
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

def on_sensor_message(ch, method, properties, body):
    """Callback xử lý dữ liệu cảm biến và phát cảnh báo nếu vượt ngưỡng."""
    try:
        data = json.loads(body.decode("utf-8"))
        device_id = data.get("device_id", "Unknown")
        temperature = data.get("temperature", 0.0)
        humidity = data.get("humidity", 0.0)
        timestamp = data.get("timestamp", "")

        # In thông tin cảm biến theo yêu cầu bài thực hành
        print(f"Device: {device_id}")
        print(f"Temperature: {temperature}")
        print(f"Humidity: {humidity}")
        if timestamp:
            print(f"Time: {timestamp}")

        # Kiểm tra ngưỡng cảnh báo
        has_warning = False
        if temperature > 35:
            print("CANH BAO: Nhiet do cao")
            has_warning = True
        if humidity < 40:
            print("CANH BAO: Do am thap")
            has_warning = True

        if not has_warning:
            print("Trang thai: Binh thuong")

        print("-" * 40)

        # Xác nhận đã xử lý xong
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        print(f"[!] Loi parse JSON cho message: {body.decode('utf-8')}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    queue_name = "sensor_data_queue"

    print("=== BÀI 2: MONITORING CONSUMER (Giám sát dữ liệu cảm biến) ===")
    print("Đang kết nối tới RabbitMQ Broker...")
    connection = get_connection()
    channel = connection.channel()

    # Khai báo queue
    channel.queue_declare(queue=queue_name, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=on_sensor_message)

    print(f"[*] Đang lắng nghe dữ liệu từ queue '{queue_name}'...")
    print("[*] Nhấn Ctrl+C để dừng chương trình.\n")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[!] Đang dừng monitoring consumer...")
        channel.stop_consuming()
        connection.close()
        print("[OK] Đã đóng kết nối an toàn.")
        sys.exit(0)

if __name__ == "__main__":
    main()
