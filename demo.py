# -*- coding: utf-8 -*-
# @File  : ST_CLIENT.py
# @Author: wglink
# @Time: 2024/8/10 18:39


import requests as req

class StockToday:
    def __init__(self):
        self.url = 'https://tushare.citydata.club'
        self.TOKEN = '4985365681928082342'

    def stock_basic(self,exchange,list_status,fields):
        url = self.url+'/stock_basic'
        infos = {'TOKEN':self.TOKEN,'exchange':exchange, 'list_status':list_status, 'fields':fields,}
        data = req.post(url,data=infos).json()
        return data


if __name__ == '__main__':
    ST = StockToday()
    print(ST.stock_basic(exchange='',list_status='L',fields='ts_code,symbol,name,area,industry,list_date'))
