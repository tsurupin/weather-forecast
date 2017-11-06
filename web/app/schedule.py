from crontab import CronTab

my_cron = CronTab()

job = my_cron.new(command='python /cron/test.py')
job.minute.every(1)
