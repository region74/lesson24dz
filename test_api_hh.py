import pprint

import requests
from sqlalchemy import Column, Integer, String, create_engine, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

engine = create_engine('sqlite:///base.sqlite', echo=False)
Base = declarative_base()


def parser(text):
    url = 'https://api.hh.ru/vacancies'
    gorod = '1384'
    params = {
        'text': text,
        'area': gorod
    }

    result = requests.get(url, params=params).json()
    pprint.pprint(result)
    vacancy_list = []
    for item in result['items']:
        city = item['area']['name']
        firma = item['employer']['name']
        vacancy = item['name']
        link = item['alternate_url']
        try:
            zarplata = item['salary']['from']
            if zarplata == None:
                zarplata = 'Не указана'
        except Exception:
            zarplata = 'Не указана'
        data = (vacancy, firma, city, zarplata, link)
        vacancy_list.append(data)
    return vacancy_list


class Itogi(Base):
    __tablename__ = 'itogi'
    id = Column(Integer, primary_key=True)
    vac_id = Column(Integer, ForeignKey('vacancy.id'))
    region_id = Column(Integer, ForeignKey('region.id'))
    firma_id = Column(Integer, ForeignKey('firma.id'))
    zarplata_id = Column(Integer, ForeignKey('zarplata.id'))
    link_id = Column(Integer, ForeignKey('link.id'))

    def __init__(self, vac, region, firma, zarplata, link):
        self.vac_id = vac
        self.region_id = region
        self.firma_id = firma
        self.zarplata_id = zarplata
        self.link_id = link

    def __str__(self):
        return f'{self.id} {self.vac_id} {self.region_id} {self.firma_id} {self.zarplata_id} {self.link_id}'


class Vacancy(Base):
    __tablename__ = 'vacancy'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f'{self.name}'


class Region(Base):
    __tablename__ = 'region'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f'{self.name}'


class Zarplata(Base):
    __tablename__ = 'zarplata'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f'{self.name}'


class Firma(Base):
    __tablename__ = 'firma'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    # name = Column(String)

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f'{self.name}'


class Link(Base):
    __tablename__ = 'link'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f'{self.name}'


Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


def load_tobase(text):
    items = parser(text)
    vacancy_list = []
    for item in items:
        # Фирма
        if session.query(Firma).filter(Firma.name == f'{item[1]}').one_or_none():
            pass
        else:
            session.add(Firma(item[1]))
        firma_id = session.query(Firma).filter(Firma.name == f'{item[1]}').one_or_none().id

        # Зарплата
        if session.query(Zarplata).filter(Zarplata.name == f'{item[3]}').one_or_none():
            pass
        else:
            session.add(Zarplata(item[3]))
        zarplata_id = session.query(Zarplata).filter(Zarplata.name == f'{item[3]}').first().id

        # Город
        if session.query(Region).filter(Region.name == f'{item[2]}').one_or_none():
            pass
        else:
            session.add(Region(item[2]))
        region_id = session.query(Region).filter(Region.name == f'{item[2]}').first().id

        # Ссылка
        if session.query(Link).filter(Link.name == f'{item[4]}').one_or_none():
            pass
        else:
            session.add(Link(item[4]))
        link_id = session.query(Link).filter(Link.name == f'{item[4]}').first().id

        # Вакансия
        session.add(Vacancy(item[0]))
        vac_id = session.query(Vacancy).filter(Vacancy.name == f'{item[0]}').first().id
        session.add(Itogi(vac_id, region_id, firma_id, zarplata_id, link_id))
        # session.query(Itogi).order_by(Itogi.id)[-1]
        # session.commit()
        tmp = session.query(Itogi).order_by(Itogi.id)[-1].id
        vacancy_list.append(tmp)
    session.commit()
    return get_data(vacancy_list)


def get_data(vacancy_list):
    data_set = []
    for vac in vacancy_list:
        tmp = select([Itogi.id, Vacancy.name, Region.name, Firma.name, Zarplata.name, Link.name]).where(
            Itogi.vac_id == Vacancy.id,
            Itogi.link_id == Link.id, Itogi.zarplata_id == Zarplata.id, Itogi.vac_id == Vacancy.id,
            Itogi.firma_id == Firma.id, Itogi.region_id == Region.id, Itogi.id == vac)
        result = engine.execute(tmp)
        for res in result:
            print(res)
            data_set.append(res)
    return data_set
    # print(res)


text = 'Хирург'
load_tobase(text)
# get_data()

# association_table.insert(self=['vac_id', 'region_id', 'firma_id', 'zarplata_id', 'link_id'], values=[1, 2, 3, 4, 5])
# association_table.insert(values=[1, 2, 3, 4, 5])
# association_table.insert([1, 2, 3, 4, 5])
# session.commit()
# for i in item:
#     print(i)

# final = session.query(Link).filter(Link.name==items[0][4]).all()
# for f in final:
#     print(f)

#
# def get_info(text):
#     DOMAIN = 'https://api.hh.ru/'
#     url = f'{DOMAIN}vacancies'
#     found_vac = text
#     gorod = '1384'
#     params = {
#         'text': found_vac,
#         # 'experience': 'noExperience',
#         'area': gorod
#     }
#
#     conn = sqlite3.connect('C:/Users/Кирилл/Desktop/PYTHON/BD/api.db', check_same_thread=False)  # соединяем с бд
#     cursor = conn.cursor()
#
#     result = requests.get(url, params=params).json()
#     pprint.pprint(result)
#     final_result = []
#     vacancy_list = []
#
#     for item in result['items']:
#         city = item['area']['name']
#         employer = item['employer']['name']
#         position = item['name']
#         link = item['alternate_url']
#         try:
#             salary = item['salary']['from']
#             if salary == None:
#                 salary = 'Не указана'
#         except Exception:
#             salary = 'Не указана'
#
#         cursor.execute('insert or ignore into  city (name) values (?)', [city])
#         cursor.execute('insert or ignore into link (name) values (?)', [link])
#         cursor.execute('insert or ignore into firma (name) values (?)', [employer])
#         cursor.execute('insert or ignore into zarplata (name) values(?)', [salary])
#         conn.commit()
#         city_id = cursor.execute(f'select id from city where city.name="{city}"').fetchone()[0]
#         firma_id = cursor.execute(f'select id from firma where firma.name="{employer}"').fetchone()[0]
#         link_id = cursor.execute(f'select id from link where link.name="{link}"').fetchone()[0]
#         zp_id = cursor.execute(f'select id from zarplata where zarplata.name="{salary}"').fetchone()[0]
#         cursor.execute('insert or ignore into vacancy (name,firma_id,city_id,zp_id,link_id) values(?,?,?,?,?)',
#                        [position, firma_id, city_id, zp_id, link_id])
#         vacancy_list.append(cursor.execute(f'select id from vacancy where vacancy.link_id ="{link_id}"').fetchone()[0])
#         conn.commit()
#
#     load_info(final_result, vacancy_list)
#     conn.close()
#     return final_result,
#
#
# def load_info(final_result, vacancy_list):
#     conn = sqlite3.connect('C:/Users/Кирилл/Desktop/PYTHON/BD/api.db', check_same_thread=False)  # соединяем с бд
#     cursor = conn.cursor()
#     for vac in vacancy_list:
#         query = f'select v.name, c.name, f.name, z.name, l.name ' \
#                 'from vacancy v, city c, firma f, zarplata z, link l ' \
#                 f'where v.city_id=c.id and v.firma_id=f.id and v.link_id=l.id and v.zp_id=z.id and v.id="{vac}"'
#         cursor.execute(query)
#         result = cursor.fetchall()
#         for res in result:
#             line = ''
#             for i in res:
#                 line += f'{i} '
#             final_result.append(line)
#
#     return final_result
