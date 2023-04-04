import schedule
import time
def scheduler_fun():
    print("Hello laxman")
schedule.every(3).minutes.do(scheduler_fun)

while True:
    schedule.run_pending()
