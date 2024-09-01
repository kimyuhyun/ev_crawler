from datetime import datetime


now = datetime.now()
str_now = now.strftime("%Y-%m-%d %H:%M:%S")

print(str_now)