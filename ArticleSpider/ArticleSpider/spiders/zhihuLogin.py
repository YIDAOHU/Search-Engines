# -*- coding: utf-8 -*-

from selenium import webdriver
import time
import json

driver = webdriver.Firefox(executable_path='D:/webdriver/geckodriver.exe')
driver.get("https://www.zhihu.com/signup?next=%2F")

driver.find_element_by_xpath("/html/body/div[1]/div/main/div/div/div/div[2]/div[2]/span").click()
# 输入账号密码
driver.find_element_by_name("username").send_keys("账户")
driver.find_element_by_name("password").send_keys("密码")

time.sleep(10)
# 模拟点击登录
driver.find_element_by_xpath("//*[@id='root']/div/main/div/div/div/div[2]/div[1]/form/button").submit()
time.sleep(3)
cookies = driver.get_cookies()
print cookies
json.dump(cookies, open('zh_cookie.json', 'w'), ensure_ascii=False)
driver.close()
