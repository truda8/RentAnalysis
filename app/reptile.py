# -*- coding: UTF-8 -*-
import pandas as pd
import aiohttp
import asyncio
import async_timeout
from pyquery import PyQuery as pq

async def fetch(session, url):
    with async_timeout.timeout(10):
        async with session.get(url) as response:
            return await response.text()

async def main():
    url_base = "http://rent.0772.fccs.com/lease/search/p%d.html"
    async with aiohttp.ClientSession() as session:
        for page in range(page_num):
            print("正在爬取第 %d 页..." % (page + 1))
            url = url_base % (page + 1)
            html = await fetch(session, url=url)
            parsing(html)

def parsing(html):
    html_pq = pq(html)
    ul = html_pq(".fy_list")("ul")("li")
    for item in ul.items():
        address = item.attr("floor")  # 地址
        area = item.attr("areaname")  # 区域
        if area == "柳州市":
            continue
        price = item.attr("pricerent")  # 价格
        buildarea = item.attr("buildarea")  # 面积
        dataset.append([address, area, price, buildarea])

def save_to_csv(data):
    df = pd.DataFrame(data, columns=['address', 'area', 'price', 'buildarea'])
    df.dropna()  # 数据清洗
    df.to_csv(save_path, index=False)

if __name__ == '__main__':
    dataset = []
    save_path = "data/rent.csv"
    page_num = 70
    asyncio.run(main())
    save_to_csv(dataset)
    print("爬取完成! 已保存到：%s" % save_path)