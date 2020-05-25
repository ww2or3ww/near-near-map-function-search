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
        urlField = "type,title,tel,address,latlon,image,candelivery,reservation,candrivethru,cantakeout,facebook,twitter,instagram,homepage,media,media1,media2,media3,media4,media5"
        urlEtc = "&sort=distance asc &size=15"
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
            tmp["type"] = types
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
            if "reservation" in mark["fields"]:
                tmp["reservation"] = int(mark["fields"]["reservation"])
                
            if "candelivery" in mark["fields"]:
                tmp["canDelivery"] = True if int(mark["fields"]["candelivery"]) == 1 else False
            else:
                tmp["canDelivery"] = False
            if "cantakeout" in mark["fields"]:
                tmp["canTakeout"] = True if int(mark["fields"]["cantakeout"]) == 1 else False
            else:
                tmp["canTakeout"] = False
            if "candrivethru" in mark["fields"]:
                tmp["canDriveThru"] = True if int(mark["fields"]["candrivethru"]) == 1 else False
            else:
                tmp["canDriveThru"] = False
                
            if "media" in mark["fields"]:
                tmp["media"] = mark["fields"]["media"]
            if "media1" in mark["fields"]:
                tmp["media1"] = mark["fields"]["media1"]
                tmp["media"] = mark["fields"]["media1"]
            if "media2" in mark["fields"]:
                tmp["media2"] = mark["fields"]["media2"]
            if "media3" in mark["fields"]:
                tmp["media3"] = mark["fields"]["media3"]
            if "media4" in mark["fields"]:
                tmp["media4"] = mark["fields"]["media4"]
            if "media5" in mark["fields"]:
                tmp["media5"] = mark["fields"]["media5"]
            
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
