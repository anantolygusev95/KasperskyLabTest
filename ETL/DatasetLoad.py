import requests as rq
import json
import pandas as pd
import sqlalchemy as sa
from psycopg2.extras import Json
from datetime import datetime

import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - [%(levelname)s] %(name)s [%(module)s.%(funcName)s:%('
                              'lineno)d]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
ch.setFormatter(formatter)
logger.addHandler(ch)

query_ins = '''insert into moscow_household_services (global_id, 
                                       name, 
                                       is_net_object,
                                       operating_company, 
                                       type_service, 
                                       adm_area,
                                       district, 
                                       address, 
                                       public_phone,
                                       working_hours, 
                                       clarification_of_working_hours, 
                                       geo_data, 
                                       publication_date,
                                       version_number) 
                 values (%(global_id)s, %(Name)s, %(IsNetObject)s, %(OperatingCompany)s, %(TypeService)s,
                 %(AdmArea)s, %(District)s, %(Address)s, %(public_phone)s, %(working_hours)s,
                 %(ClarificationOfWorkingHours)s, %(geo_data)s, %(publication_date)s, %(version_number)s)'''

token = '7de91817f3819512cde69a237c2580b3'
dataset_id = 1904

conn_string = 'localhost://8080'

sa_postgres_conn = sa.create_engine(conn_string)

def check_db_version_number(engine):
    conn = engine.connect().connection
    cur = conn.cursor()
    cur.execute('''select version_number
                    from moscow_household_services
                    limit 1''')
    version = [x[0] for x in cur.fetchall()]
    if len(version) == 0:
        return None
    else:
        return version[0]

def load_api_dataset(dataset_id, engine, cnt_rows):
    logger.warning('Очищаем таблицу')
    conn = engine.connect().connection
    cur = conn.cursor()
    cur.execute('''truncate moscow_household_services''')
    conn.commit()
    conn.close()
    cur.close()
    logger.warning('Очистили')
    flag = True
    skip_rows = 0
    while flag:

        if (cnt_rows - skip_rows) >= 500:
            top_rows = 500
        else:
            top_rows = cnt_rows - skip_rows

        logger.warning(f'Начинаю забирать из api {top_rows} записей')
        load_dataset_api = f'''
        https://apidata.mos.ru/v1/datasets/{dataset_id}/rows?api_key={token}&$top={top_rows}&$skip={skip_rows}&$orderby="global_id%20desc" '''
        print(load_dataset_api)
        load_req = rq.get(url=load_dataset_api)

        if load_req.status_code == 200:
            load_res = json.loads(load_req.text)

        final_res = []

        for i in range(0, len(load_res)):
            final_res.append(load_res[i]['Cells']) 
            final_res[i].update([('publication_date', publication_date)])
            final_res[i].update([('version_number', version_number)])

        for item in final_res:
            item['public_phone'] = Json(item['PublicPhone'])
            item['working_hours'] = Json(item['WorkingHours'], )
            item['geo_data'] = Json(item['geoData'])
        logger.warning(f'Забрал {len(final_res)} записей')    
        logger.warning('Начал запись в бд')
        conn = sa_postgres_conn.connect().connection
        cur = conn.cursor()
        cur.executemany(query_ins, final_res)
        conn.commit()
        cur.close()
        conn.close()
        logger.warning('Закончил запись')
        skip_rows += 500
        if skip_rows > cnt_rows:
            flag = False
    logger.warning('Закончил')


def run_load(dataset_id=dataset_id, token=token, engine=sa_postgres_conn):
    db_version = check_db_version_number(engine)
    logger.warning('Проверяю дату публикации и текущую версию датасета')
    
    info_dataset_api = f'https://apidata.mos.ru/v1/datasets/{dataset_id}?api_key={token}'
    
    info_req = rq.get(url=info_dataset_api)
    
    if info_req.status_code == 200:
        info_res = json.loads(info_req.text)
        publication_date = info_res['VersionDate']
        version_number = info_res['VersionNumber']
        cnt_rows = info_res['ItemsCount']
    logger.warning('Проверил')
    
    publication_date = datetime.strptime(publication_date, '%d.%m.%Y')
    publication_date = publication_date.date()
    
    if db_version == version_number:
        logger.warning('В бд актуальная версия, обновлять не требуется')
    
    else:
        load_api_dataset(dataset_id=dataset_id, engine=engine, cnt_rows=cnt_rows)