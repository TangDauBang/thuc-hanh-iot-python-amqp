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
    queue_name = "iot_lab_queue"

    print("=== BÀI 1: PRODUCER (Gửi message qua queue) ===")
    print("Đang kết nối tới RabbitMQ Broker...")
    connection = get_connection()
    channel = connection.channel()

    # Khai báo queue (đảm bảo queue tồn tại)
    channel.queue_declare(queue=queue_name, durable=True)

    # Nội dung thông điệp theo yêu cầu bài thực hành:
    # Gồm: Họ tên sinh viên, Mã sinh viên, Nội dung chào mừng
    student_name = "Nguyen Van A"
    student_id = "B21DCCN001"
    message_content = f"Xin chao tu ung dung Python AMQP - {student_id} - {student_name}"

    print(f"\n1. Gửi message mặc định: '{message_content}'")
    print("2. Tự nhập message gửi một lần")
    print("3. Gửi liên tiếp nhiều message (mở rộng)")
    choice = input("Chọn chế độ (1/2/3, Enter để chọn 1): ").strip()

    if choice == "2":
        custom_msg = input("Nhập nội dung message muốn gửi: ").strip()
        if custom_msg:
            message_content = custom_msg
        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=message_content.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent message
        )
        print(f"[x] Đã gửi message: {message_content}")

    elif choice == "3":
        try:
            count = int(input("Nhập số lượng message muốn gửi (ví dụ: 5): ").strip())
        except ValueError:
            count = 5
        print(f"Bắt đầu gửi {count} messages...")
        for i in range(1, count + 1):
            msg = f"{message_content} (Message #{i})"
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=msg.encode("utf-8"),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"[x] Đã gửi #{i}: {msg}")
    else:
        # Chế độ mặc định 1
        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=message_content.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[x] Đã gửi thành công vào queue '{queue_name}':")
        print(f"    -> {message_content}")

    connection.close()
    print("\n[OK] Đã hoàn thành và đóng kết nối.")

if __name__ == "__main__":
    main()
