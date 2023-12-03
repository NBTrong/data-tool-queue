# Import thư viện CronTab để tạo và quản lý công việc Cron
from crontab import CronTab

# Định nghĩa lớp Crawler
class Crawler:
    def __init__(self):
        # Tạo một đối tượng CronTab với quyền người dùng 'root'
        cron = CronTab(user='root')

        # Xóa toàn bộ các công việc Cron hiện tại
        cron.remove_all()

        # Các công việc Cron sẽ được lên lịch thông qua hàm self.execute với các tham số command và quantity

        # Thu thập dữ liệu từ luồng cửa hàng A của trang web Shopee, lên lịch mỗi 10 phút 1 lần
        self.execute(
            lambda: cron.new(command="/usr/local/bin/python /crawler/src/job/queue/__main__.py").setall("* * * * *"),
            quantity=1
        )

        # Lưu các công việc đã tạo vào lịch trình Cron thực tế của hệ thống
        cron.write()

    # Hàm thực hiện lên lịch và thêm các công việc vào lớp Crawler
    def execute(self, command, quantity):
        for _ in range(quantity):
            command()

# Chạy lớp Crawler nếu đoạn mã này được thực thi là một tập lệnh chính
if __name__ == "__main__":
    Crawler()
