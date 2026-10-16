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

def on_message_received(ch, method, properties, body):
    """Callback xử lý khi nhận được message từ queue."""
    message = body.decode("utf-8")
    receive_time = datetime.now().strftime("%H:%M:%S")

    print(f"Da nhan message: {message}")
    print(f"Thoi gian nhan: {receive_time}")
    print("-" * 50)

    # Xác nhận đã xử lý message thành công
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    queue_name = "iot_lab_queue"

    print("=== BÀI 1: CONSUMER (Lắng nghe message từ queue) ===")
    print("Đang kết nối tới RabbitMQ Broker...")
    connection = get_connection()
    channel = connection.channel()

    # Khai báo queue (đảm bảo queue tồn tại)
    channel.queue_declare(queue=queue_name, durable=True)

    # Đảm bảo mỗi lần chỉ nhận 1 message để xử lý xong mới nhận tiếp
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=on_message_received)

    print(f"[*] Đang lắng nghe trên queue '{queue_name}'...")
    print("[*] Nhấn Ctrl+C để dừng chương trình.\n")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[!] Đang dừng consumer...")
        channel.stop_consuming()
        connection.close()
        print("[OK] Đã đóng kết nối an toàn.")
        sys.exit(0)

if __name__ == "__main__":
    main()
