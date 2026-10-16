import os
import sys
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

def on_warning_received(ch, method, properties, body):
    """Callback khi nhận message mức warning."""
    message = body.decode("utf-8")
    time_str = datetime.now().strftime("%H:%M:%S")

    print(f"[warning_queue] Da nhan: {message}")
    print(f"                (Thoi gian: {time_str}, Routing Key: {method.routing_key})")
    print("-" * 50)

    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    exchange_name = "iot_alert_exchange"
    queue_name = "warning_queue"
    routing_key = "warning"

    print("=== BÀI 3: WARNING CONSUMER (Chỉ nhận cảnh báo mức WARNING) ===")
    print("Đang kết nối tới RabbitMQ Broker...")
    connection = get_connection()
    channel = connection.channel()

    # Đảm bảo exchange tồn tại
    channel.exchange_declare(exchange=exchange_name, exchange_type="direct", durable=True)

    # Khai báo queue riêng cho warning
    channel.queue_declare(queue=queue_name, durable=True)

    # Bind queue với exchange thông qua routing_key='warning'
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=on_warning_received)

    print(f"[*] Queue '{queue_name}' đã bind với exchange '{exchange_name}' [Routing Key: '{routing_key}']")
    print("[*] Đang chờ cảnh báo mức WARNING... Nhấn Ctrl+C để dừng.\n")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[!] Đang dừng Warning Consumer...")
        channel.stop_consuming()
        connection.close()
        print("[OK] Đã đóng kết nối an toàn.")
        sys.exit(0)

if __name__ == "__main__":
    main()
