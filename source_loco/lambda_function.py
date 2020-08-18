# --- coding: utf-8 ---
# 検索ネコ(ロコガイド)
# WebからAPI Gateway経由で呼ばれるLambda
# ロコガイドAPIだけを利用する。

import sys
import json
import os
import re
import requests
from urllib.parse import urljoin

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
_consoleHandler = logging.StreamHandler(sys.stdout)
_consoleHandler.setLevel(logging.INFO)
_simpleFormatter = logging.Formatter(
    fmt='%(levelname)-5s %(funcName)-20s %(lineno)4s: %(message)s'
)
_consoleHandler.setFormatter(_simpleFormatter)
logger.addHandler(_consoleHandler)

from retry import retry

LOCOGUIDE_API_ADDRESS       = ""    if("LOCOGUIDE_API_ADDRESS" not in os.environ)   else os.environ["LOCOGUIDE_API_ADDRESS"]
LOCOGUIDE_API_TOKEN         = ""    if("LOCOGUIDE_API_TOKEN" not in os.environ)     else os.environ["LOCOGUIDE_API_TOKEN"]

def lambda_handler(event, context):
    try:
        latlon = event["queryStringParameters"]["latlon"]
        logger.info("=== START ===")
        logger.info("latlon={0}".format(latlon))

        #ロコリストで混雑レベルを取得する
        resultList = []
        has_clowd = getCrowdLvFromLoco(resultList, latlon)

        result = {}
        result["has_clowd"] = has_clowd
        result["list"] = resultList
        
        return {
            "headers": {
                "Access-Control-Allow-Origin" : "*",
                "Access-Control-Allow-Credentials": "true"
            },
            'statusCode': 200,
            "body": json.dumps(result, ensure_ascii=False, indent=2)
        }

    except Exception as e:
        logger.exception(e)
        return {
            "statusCode": 500,
            "body": "error"
        }

def getCrowdLvFromLoco(resultList, latlon):
    has_clowd = False
    try:
        latlonList = latlon.split(",")
        # ロコガイドAPIをコールする
        url = LOCOGUIDE_API_ADDRESS + "?latitude=" + latlonList[0] + "&longitude=" + latlonList[1] + "&distance=50"
        has_clowd = getDataFromLocoLink(resultList, url, 1, 0, 0)

    except Exception as e:
        logger.exception(e)

    return has_clowd

def getDataFromLocoLink(resultList, url, page, lastLat, lastLng):
    if page > 3:
        return False
    logger.info("------loco address------page=" + str(page))
    logger.info(url)

    headers ={}
    headers["Authorization"] = "Bearer " + LOCOGUIDE_API_TOKEN
    response = request(url, headers)
    response.encoding = response.apparent_encoding
    content = response.content.decode("utf-8")
    jsn = json.loads(content)

    logger.info("list size = " + str(len(jsn)))
    #logger.info(jsn)

    has_clowd = False
    for item in jsn:
        data, flg = convert(item)
        if data["position"]["lat"] == lastLat and data["position"]["lng"] == lastLng:
            insertListWithCrowd(resultList[-1]["list"], data["list"][0])
        else:
            resultList.append(data)
        lastLat = data["position"]["lat"]
        lastLng = data["position"]["lng"]
        if flg:
            has_clowd = True

    if "Link" in response.headers:
        nextUrl = response.headers["Link"]
        nextUrl = nextUrl[1:nextUrl.find(">")]
        page = page + 1
        flg = getDataFromLocoLink(resultList, nextUrl, page, lastLat, lastLng)
        if flg:
            has_clowd = True

    return has_clowd

def insertListWithCrowd(list, data):
    isInserted = False
    if "crowd_lv" in data and len(list) > 0:
        for i in range(len(list)):
            if "crowd_lv" not in list[i] or data["crowd_lv"] > list[i]["crowd_lv"]:
                list.insert(i, data)
                isInserted = True
                break
    if isInserted == False:
        list.append(data)

@retry(tries=3, delay=1)
def request(url, headers):
    return requests.get(url, headers=headers)
    
def convert(item):
    has_clowd = False
    data = {}
    data["position"] = { "lat": float(item["latitude"]), "lng": float(item["longitude"]) }
    data["address"] = item["address"]
    
    child = {}
    child["title"] = item["name"]
    child["tel"] = item["phone_number"]
    child["homepage"] = {
        "address": item["url"],
        "has_xframe_options": "0"
    }
    child["media1"] = {
        "address": "https://locoguide.jp/places/" + str(item["id"]),
        "has_xframe_options": "1"
    }
    if "crowd_lamp" in item and item["crowd_lamp"]:
        has_clowd = True
        color = item["crowd_lamp"]["color"]
        lv = 0
        if color == "red":
            lv = 3
        elif color == "yellow":
            lv = 2
        elif color == "green" or color == "blue":
            lv = 1
        child["crowd_lv"] = lv
        
    data["list"] = []
    data["list"].append(child)
    
    return data, has_clowd
