import json
import scrapy
from scrapy.http import FormRequest

class MobikeSpider(scrapy.Spider):

    name = "mobike"
    url = "https://mwx.mobike.com/mobike-api/rent/nearbyBikesInfo.do"
    body = {
        'latitude': "31.218253",
        'longitude': "121.485527",
        'errMsg': "getMapCenterLocation"
    }
    headers = {
        'charset': "utf-8",
        'platform': "4",
        "referer": "https://servicewechat.com/wx40f112341ae33edb/1/",
        'content-type': "application/x-www-form-urlencoded",
        'user-agent': "MicroMessenger/6.5.4.1000 NetType/WIFI Language/zh_CN",
        'host': "mwx.mobike.com",
        'connection': "Keep-Alive",
        'accept-encoding': "gzip",
        'cache-control': "no-cache"
    }

    def start_requests(self):
        return [FormRequest(url=self.url, formdata=self.body, headers=self.headers, callback=self.parse)]

    def parse(self, response):
        result = json.loads(response.body)
        print(result)
        print(result['object'])


