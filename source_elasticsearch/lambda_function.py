# --- coding: utf-8 ---
# 検索ネコ
# WebからAPI Gateway経由で呼ばれるLambda
# ElasticSearch に対して距離順検索をして得られた結果を返す。

import sys
import json
import os
import re
import requests
from urllib.parse import urljoin

from elasticsearch import Elasticsearch, RequestsHttpConnection

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

ENDPOINT_ES                 = ""    if("ENDPOINT_ES" not in os.environ)             else os.environ["ENDPOINT_ES"]
LOCOGUIDE_API_ADDRESS       = ""    if("LOCOGUIDE_API_ADDRESS" not in os.environ)   else os.environ["LOCOGUIDE_API_ADDRESS"]
LOCOGUIDE_API_TOKEN         = ""    if("LOCOGUIDE_API_TOKEN" not in os.environ)     else os.environ["LOCOGUIDE_API_TOKEN"]
RESULT_COUNT_MAX            = 100

def lambda_handler(event, context):
    try:
        logger.info("=== START ===")
        types = event["queryStringParameters"]["type"]
        latlon = event["queryStringParameters"]["latlon"]
        logger.info("type={0}, latlon={1}".format(types, latlon))

        searchRes = search(types, latlon)
        hits = searchRes["hits"]["hits"]

        result = {}
        has_clowd = False
        resultList = []
        locolist = []
        lastLat = 0
        lastLng = 0
        index = 0
        for item in hits:
            data = convert(types, item)
            if item["_source"]["locoguide_id"]:
                data["list"][0]["locoguide_id"] = item["_source"]["locoguide_id"]
                data["list"][0]["crowd_lv"] = 0
                locolist.append(data["list"][0])
            
            # ★TTEST
            # 同じ場所に複数ポイントのテスト
            #if index == 1 or index == 2:
            #    data["position"]["lat"] = lastLat
            #    data["position"]["lng"] = lastLng

            if data["position"]["lat"] == lastLat and data["position"]["lng"] == lastLng:
                resultList[-1]["list"].append(data["list"][0])
            else:
                resultList.append(data)
            lastLat = data["position"]["lat"]
            lastLng = data["position"]["lng"]
            index = index + 1

        #ロコリストで混雑レベルを取得する
        if len(locolist) > 0:
            has_clowd = getCrowdLvFromLoco(resultList, locolist)

        result["list"] = resultList
        result["has_clowd"] = has_clowd
        
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

@retry(tries=3, delay=1)
def search(types, latlon):
    try:
        es = Elasticsearch(
            hosts=[{
                'host': ENDPOINT_ES,
                'port': 443
            }],
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=1500
        )
        data = {
            "query": {
                "match": { "type" : types } 
            },
            "sort": {
              "_geo_distance": {
                "latlon": latlon,
                "order": "asc",
                "unit": "km",
                "distance_type": "plane"
              }
            },
            "size": RESULT_COUNT_MAX
        }
        result = es.search(index="articles", body=data)

        return result
    except Exception as e:
        logger.exception(e)
        raise e

def getCrowdLvFromLoco(resultlist, locolist):
    has_clowd = False
    try:
        ids = ""
        for item in locolist:
            ids += "{0},".format(item["locoguide_id"])
        ids.rstrip(",")
            
        url = LOCOGUIDE_API_ADDRESS + "?id=" + ids
        idList = []
        lvList = []
        has_clowd = requestLoco(url, 1, idList, lvList)

        for i in range(len(idList)):
            id = idList[i]
            lv = lvList[i]
            for tmp in locolist:
                if tmp["locoguide_id"] == id:
                    tmp["crowd_lv"] = lv
                    break

    except Exception as e:
        logger.exception(e)

    return has_clowd

def requestLoco(url, page, idList, lvList):
    logger.info("------loco address------page=" + str(page))
    logger.info(url)
    headers ={}
    headers["Authorization"] = "Bearer " + LOCOGUIDE_API_TOKEN
    response = request(url, headers)
    response.encoding = response.apparent_encoding
    content = response.content.decode("utf-8")
    jsn = json.loads(content)
    
    logger.info("list size = " + str(len(jsn)))

    has_clowd = False
    for tmp in jsn:
        if "crowd_lamp" not in tmp or not tmp["crowd_lamp"]:
            continue
        color = tmp["crowd_lamp"]["color"]
        
        # ★TEST
        # ロコIDがNULLでもblueにするテストコード(↑これはコメントする)
        #if "crowd_lamp" not in tmp:
        #    continue
        #color = "blue"
        
        if tmp["crowd_lamp"] != None:
            color = tmp["crowd_lamp"]["color"]
        lv = 0
        if color == "red":
            lv = 3
        elif color == "yellow":
            lv = 2
        elif color == "green" or color == "blue":
            lv = 1
        lvList.append(lv)
        idList.append(str(tmp["id"]))
        has_clowd = True

    if "Link" in response.headers:
        nextUrl = response.headers["Link"]
        nextUrl = nextUrl[1:nextUrl.find(">")]
        page = page + 1
        flg = requestLoco(nextUrl, page, idList, lvList)
        if flg:
            has_clowd = True

    return has_clowd

@retry(tries=3, delay=1)
def request(url, headers):
    return requests.get(url, headers=headers)

def convert(types, item):
    data = {}
    data["type"] = types
    latlon = item["_source"]["latlon"].split(",")
    data["position"] = { "lat": float(latlon[0]), "lng": float(latlon[1]) }
    
    child = {}
    child["guid"] = item["_source"]["guid"]
    child["title"] = item["_source"]["title"]
    child["tel"] = item["_source"]["tel"]
    child["address"] = item["_source"]["address"]

    if item["_source"]["image"]:
        child["image"] = urljoin("https://near-near-map.s3-ap-northeast-1.amazonaws.com/", item["_source"]["image"])
    else:
        child["image"] = ""

    child["facebook"] = item["_source"]["facebook"]
    child["twitter"] = item["_source"]["twitter"]
    child["instagram"] = item["_source"]["instagram"]
    
    has_xframe_options = [0, 0, 0, 0, 0, 0]
    has_xframe_options = item["_source"]["has_xframe_options"].split(",")
    
    child["homepage"] = {
        "address": item["_source"]["homepage"],
        "has_xframe_options": has_xframe_options[0]
    }
    child["media1"] = {
        "address": item["_source"]["media1"],
        "has_xframe_options": has_xframe_options[1]
    }
    child["media2"] = {
        "address": item["_source"]["media2"],
        "has_xframe_options": has_xframe_options[2]
    }
    child["media3"] = {
        "address": item["_source"]["media3"],
        "has_xframe_options": has_xframe_options[3]
    }
    child["media4"] = {
        "address": item["_source"]["media4"],
        "has_xframe_options": has_xframe_options[4]
    }
    child["media5"] = {
        "address": item["_source"]["media5"],
        "has_xframe_options": has_xframe_options[5]
    }
    
    if "star" in item["_source"]:
        child["star"] = item["_source"]["star"]
    else:
        child["star"] = 0
    
    data["list"] = []
    data["list"].append(child)
    
    return data