import smtplib
from email.mime.text import MIMEText
from email.header import Header




mail_host = "smtp.qq.com"
mail_user = "414804619@qq.com"
mail_password = "grgpsutcsazdbihf"






sender = "414804619@qq.com"

receivers = ["anbang_li@163.com"]

message = MIMEText("python 邮件",'plain',"utf-8")

message["From"] = Header("this workday",'utf-8')

message['To'] = Header("it is a nice day","utf-8")

subject = "使用Python"

message['Subject'] = Header(subject,"utf-8")

try:
    smtpObj = smtplib.SMTP()
    smtpObj.connect(mail_host,25)
    smtpObj.login(mail_user,mail_password)

    smtpObj.sendmail(sender, receivers, message.as_string())
    print("邮件发送成功")

except smtplib.SMTPException as e:
    print(e)
    print("Error: 无法发送邮件")
