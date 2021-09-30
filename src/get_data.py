from time import sleep
import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import schedule

# Constants
PRICE_MULT = 5
URL = 'https://leroymerlin.ru/catalogue/dekorativnye-oboi/'
TOPIC_TGT = 'leroy-db-final'
HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }

def publish_message(producer_instance, topic_name, key, value):
    
    # Successful result returns assigned partition and offset

    # def on_send_success(record_metadata):
    #     print(record_metadata.topic)
    #     print(record_metadata.partition)
    #     print(record_metadata.offset)

    # handle exception
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')

        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)#.add_callback(on_send_success) 
        producer_instance.flush()
        
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'],\
                                    api_version=(2,8,1))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def fetch_raw(recipe_url):
    html = None
    print('Processing..{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url, headers=HEADERS)
        if r.status_code == 200:
            html = r.text
    except Exception as ex:
        print('Exception while accessing raw html')
        print(str(ex))
    finally:
        return html.strip()


def get_recipes():
    recipies = []
    url = URL
    print('Accessing list')
    
    try:
        r = requests.get(url, headers=HEADERS)
        if r.status_code == 200:
            
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            links = soup.select('.c155f0re_plp a')#('.fixed-recipe-card__h3 a')#
            idx = 0
            for link in links:
                sleep(2)
                recipe = fetch_raw('https://leroymerlin.ru/' + link['href'])
                recipies.append(recipe)
                idx += 1
                if len(recipies)>10:
                   break
    except Exception as ex:
        print('Exception in get_recipes')
        print(str(ex))
    finally:
        return recipies


def parse(markup):
    article = 0
    price = 0.0
    pricem2 = 0.0
    product = ''

    try:

        soup = BeautifulSoup(markup, 'lxml')
        # article
        article_section = soup.find('span', {"slot": "article"}, {"itemprop": "sku"})['content']
        # price
        price_section = soup.find('span', {"slot": "price"})
        # price-m2
        pricem2_section = soup.select('.second-price')
        # product
        product_section = soup.select('.header-2')

        if product_section:
            product = product_section[0].text
            #print(product)

        if article_section:
            article = int(article_section)
        if price_section:
            price = float(price_section.getText().replace(" ",""))
        if pricem2_section:
            try:
                pricem2 = PRICE_MULT * float('.'.join(pricem2_section[0].text.replace("\n",".").split(".")[1:3]))
            except:
                pricem2 = PRICE_MULT * float('.'.join(pricem2_section[0].text.replace("\n",".").split(".")[1:2]))
            #print(pricem2)

        rec = {'article': article, 'product': product, 'price': price, 'pricem2': pricem2}

    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
    finally:
        return json.dumps(rec)


def job():   
    parse_time = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    print(parse_time)
    all_recipes = get_recipes()

    if len(all_recipes) > 0:
        kafka_producer = connect_kafka_producer()
        for recipe in all_recipes:
            result = parse(recipe)
            
            publish_message(kafka_producer, TOPIC_TGT, parse_time, result)
        if kafka_producer is not None:
            kafka_producer.close()

schedule.every(3).minutes.do(job) # run every X mins
job() # start right now

while True:
    schedule.run_pending()
    sleep(1)