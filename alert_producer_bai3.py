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

def main():
    exchange_name = "iot_alert_exchange"

    print("=== BÀI 3: ALERT PRODUCER (Điều phối cảnh báo với Direct Exchange) ===")
    print("Đang kết nối tới RabbitMQ Broker...")
    connection = get_connection()
    channel = connection.channel()

    # Khai báo direct exchange
    channel.exchange_declare(exchange=exchange_name, exchange_type="direct", durable=True)

    # Khai báo sẵn các queue và liên kết (binding) để đảm bảo message không bị thất lạc
    # dù consumer được bật trước hay sau
    channel.queue_declare(queue="warning_queue", durable=True)
    channel.queue_bind(exchange=exchange_name, queue="warning_queue", routing_key="warning")

    channel.queue_declare(queue="critical_queue", durable=True)
    channel.queue_bind(exchange=exchange_name, queue="critical_queue", routing_key="critical")

    # Danh sách cảnh báo mẫu theo yêu cầu bài thực hành
    sample_alerts = [
        ("info", "He thong giam sat IoT khoi dong thanh cong"),
        ("warning", "Nhiet do phong may vuot nguong warning"),
        ("critical", "Cam bien kho lanh mat ket noi critical")
    ]

    print("\nChọn chức năng gửi cảnh báo:")
    print("1. Gửi đồng thời cả 3 message (info, warning, critical) để kiểm tra định tuyến")
    print("2. Gửi cảnh báo tùy chọn (tự chọn routing key)")
    choice = input("Nhập lựa chọn (1/2, Enter mặc định chọn 1): ").strip()

    if choice == "2":
        print("\nCác mức routing key: info | warning | critical")
        rk = input("Nhập routing key: ").strip().lower()
        if rk not in ["info", "warning", "critical"]:
            print(f"[!] Routing key '{rk}' không thuộc danh sách chuẩn, nhưng vẫn sẽ gửi.")
        msg = input("Nhập nội dung cảnh báo: ").strip()
        if not msg:
            msg = f"Canh bao muc {rk}"

        channel.basic_publish(
            exchange=exchange_name,
            routing_key=rk,
            body=msg.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[x] Đã gửi message tới exchange '{exchange_name}' với routing_key='{rk}':\n    -> {msg}")

    else:
        # Gửi tuần tự 3 message
        print("\n[*] Đang gửi 3 thông điệp mẫu...")
        for rk, msg in sample_alerts:
            channel.basic_publish(
                exchange=exchange_name,
                routing_key=rk,
                body=msg.encode("utf-8"),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"[x] Đã gửi [Routing Key: '{rk}'] -> '{msg}'")

    connection.close()
    print("\n[OK] Đã hoàn thành gửi và đóng kết nối.")

if __name__ == "__main__":
    main()
