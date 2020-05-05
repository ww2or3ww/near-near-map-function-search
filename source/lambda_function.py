import json
import os
import requests
from urllib.parse import urljoin
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

API_ADDRESS_CLOUDSEARCH        = ""    if("API_ADDRESS_CLOUDSEARCH" not in os.environ)    else os.environ["API_ADDRESS_CLOUDSEARCH"]

def lambda_handler(event, context):
    try:  
        logger.info("-----")
        types = event["queryStringParameters"]["type"]
        latlon = event["queryStringParameters"]["latlon"]
        logger.info("type={0}, latlon={1}".format(types, latlon))

        url = "{0}/2013-01-01/search?".format(API_ADDRESS_CLOUDSEARCH)
        #urlPrm = "q={0}".format(types) + "&expr.distance=haversin({0},latlon.latitude,latlon.longitude)&return=distance,".format(latlon)
        urlPrm = "q={0}&q.options={{fields:['type']}}".format(types) + "&expr.distance=haversin({0},latlon.latitude,latlon.longitude)&return=distance,".format(latlon)
        urlField = "type,title,tel,address,latlon,image,candelivery,needreservation,candrivethru,cantakeout,facebook,twitter,instagram,homepage"
        urlEtc = "&sort=distance%20asc&size=10"
        url = url + urlPrm + urlField + urlEtc
        logger.info(url)
        
        response = requests.get(url)
        response.encoding = response.apparent_encoding
        content = response.content.decode("utf-8")
        jsn = json.loads(content)
        hits = jsn["hits"]["hit"]

        result = []
        for mark in hits:
            tmp = {}
            latlon = mark["fields"]["latlon"].split(",")
            tmp["position"] = { "lat": float(latlon[0]), "lng": float(latlon[1]) }
            tmp["title"] = mark["fields"]["title"]
            tmp["tel"] = mark["fields"]["tel"]
            tmp["address"] = mark["fields"]["address"]
            if "image" in mark["fields"]:
                tmp["image"] = urljoin("https://near-near-map.s3-ap-northeast-1.amazonaws.com/", mark["fields"]["image"])
            if "facebook" in mark["fields"]:
                tmp["facebook"] = mark["fields"]["facebook"]
            if "twitter" in mark["fields"]:
                tmp["twitter"] = mark["fields"]["twitter"]
            if "instagram" in mark["fields"]:
                tmp["instagram"] = mark["fields"]["instagram"]
            if "homepage" in mark["fields"]:
                tmp["homepage"] = mark["fields"]["homepage"]
            if "media" in mark["fields"]:
                tmp["media"] = mark["fields"]["media"]
            if "needreservation" in mark["fields"]:
                tmp["needReservation"] = True if int(mark["fields"]["needreservation"]) == 1 else False
            if "candelivery" in mark["fields"]:
                tmp["canDelivery"] = True if int(mark["fields"]["candelivery"]) == 1 else False
            if "cantakeout" in mark["fields"]:
                tmp["canTakeout"] = True if int(mark["fields"]["cantakeout"]) == 1 else False
            if "candrivethru" in mark["fields"]:
                tmp["canDriveThru"] = True if int(mark["fields"]["candrivethru"]) == 1 else False
            
            result.append(tmp)

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
