from crontab import CronTab

class Crawler:
    def __init__(self):
        cron = CronTab(user='root')

        cron.remove_all()


        self.execute(
            lambda: cron.new(command="/usr/local/bin/python /crawler/src/job/queue/__main__.py").setall("* * * * *"),
            quantity=1
        )

        cron.write()

    def execute(self, command, quantity):
        for _ in range(quantity):
            command()

if __name__ == "__main__":
    Crawler()
