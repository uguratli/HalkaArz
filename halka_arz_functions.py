import yfinance as yf
from pandas_datareader import data as pr
from bs4 import BeautifulSoup as bs
from bs4.element import Comment
import pandas as pd
import numpy as np
import string
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime as dt
from dateutil.relativedelta import relativedelta
import time
import pickle

yf.pdr_override()


def get_hyped_days(dataframe):
    """Hype days.
    Finds hype days for given new born stock."""
    if isinstance(dataframe, pd.DataFrame):
        df = dataframe.copy()
        df = df[df["Volume"] != 0]
        df['Next_day_open'] = df['Open'].shift(-1, axis = 0)
        hype_check = [df['Close'] >= df['Next_day_open']][0]
        day = 0
        while not hype_check[day]:
            day += 1
            if day == df.shape[0]:
                return np.NAN 
                break
        return day
    else:
        print('Variable is not a dataframe')
        return 0
    

def value_fix(value, replace = False):
    value = value.replace(' ','')
    value = value.split('-')[0]
    letters_pattern = str.maketrans('', '', string.ascii_letters)
    value = value.translate(letters_pattern).strip()
    if replace:
        return float(value.replace(',', '.'))
    else:
        return value.replace(',', '')
    
def value_fix_dot(value):
    value = value.replace(' ','')
    value = value.split('-')[0]
    letters_pattern = str.maketrans('', '', string.ascii_letters)
    value = value.translate(letters_pattern).strip()
    return float(value.replace('.', ''))


def months(month):
        Months = {
                'Ocak': '1',	
                'Şubat': '2',
                'Mart': '3',	
                'Nisan': '4',	
                'Mayıs': '5',	
                'Haziran': '6',	
                'Temmuz': '7',	
                'Ağustos': '8',	
                'Eylül': '9',	
                'Ekim': '10',	
                'Kasım': '11',	
                'Aralık': '12'
                }       
        if month in Months:
                return Months[month]
        else:
                return month
def fix_time_it(time):
        str_time = ''
        for i in time.split(' '):
                str_time += months(i) + '-'
        #return str_time[:-1]
        return dt.datetime.strptime(str(str_time[:-1]), '%d-%m-%Y').date().strftime('%Y-%m-%d')

def g_stocks(stock, start, end):
    """Google finance.
    Returns historical data for given stocks for given time interval using Google Finance via using google sheets.
    Function has 10 sec time sleep, it may change, due to api limits.
    """
    try:
        scope =  ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('cred.json', scope)
        client = gspread.authorize(creds)
        sheet = client.open('stocks').sheet1
        sheet.update_cell(1, 7, stock)
        sheet.update_cell(1, 9, str(start))
        sheet.update_cell(1, 10, str(end))
        if not sheet.get_all_records() == []:
            df = pd.DataFrame(sheet.get_all_records())[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.drop(df[df['Close'].astype(str).str.len() == 0].index, inplace = True)
            type_map = {'Open': float,
                        'Close': float,
                        'High': float,
                        'Low': float,
                        'Volume':int}   
            df = df.astype(type_map)
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            print(f'{stock} DONE BY GOOGLE FINANCE')
            return df.set_index('Date')
        else:
            print(f'{stock} DONE BY GOOGLE FINANCE')
            return pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume']).set_index('Date')
    except gspread.exceptions.APIError:
        print('Error 429: Too Many Requests')
        print('10sec sleep')
        time.sleep(10)
        return get_hist_data(stock = stock, start_date=start, end_date=end)
    
def page_urls(url):
    page_counter = 1
    pages = [url]
    soup = make_soup(url)
    while soup.find(class_ = 'rightNav'):
        page_counter += 1
        pages.append(url+f'page/{page_counter}/')
        soup = make_soup(pages[-1])
    return pages


def make_request(url):
    """Request maker.
    Makes request for given url."""
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"}
    return requests.get(url=url, headers=headers)  

def make_soup(url):
    """Soup maker.
    Returns soup for given url."""
    r = make_request(url)
    if r.status_code == 200:
        return bs(r.content, 'html5lib')
    else:
        return False
    

def get_halka_arz_info(url, soup = False):
    cols = []
    vals = []
    if not soup:
        soup = make_soup(url)
    info_table = soup.find('table', {'class': 'sp-table'})
    rows = info_table.find_all('tr')
    for row in rows:
        row_data = row.find_all('td')
        cols.append(row_data[0].text)
        vals.append(row_data[1].text)
    return pd.DataFrame(data = [vals], columns = cols)


def get_halka_arz_result(url, soup = False):
    if not soup:
        soup = make_soup(url)
    vals = []
    cols = []
    #cols = ['Yurtiçi_Bireysel_Kisi', 'Yurtiçi_Bireysel_Lot',
    #    'Yurtiçi_Kurumsal_Kisi', 'Yurtiçi_Kurumsal_Lot',
    #    'Yurtdışı_Kurumsal_Kisi', 'Yurtdışı_Kurumsal_Lot',
    #    'Şirket_Çalışanları_Kisi', 'Şirket_Çalışanları_Lot']
    table = soup.find('table', {'class': 'as-table'})
    if not table == None:
        rows = table.find_all('tr')
        for i in range(2, len(rows)-2):
            row_data = rows[i].find_all('td')
            for ex in ['_Kisi', '_Lot']:
                cols.append(row_data[0].text.strip()+ex)
            for j in range(1,3):
                vals.append(row_data[j].text)
        return pd.DataFrame(data = [vals], columns = cols)
    else:
        return pd.DataFrame()
    

def get_hist_data(stock, start_date, end_date):
    """"Historical Data Function.
    Returns historical data for given symbol for given time interval. Function uses Yahoo finance, 
    but in case of missing data, function uses Google finance.
    """
    df = pr.DataReader(stock + '.IS', start=start_date, end=end_date)
    df.drop(columns=['Adj Close'], inplace=True)
    if df.shape[0] <= 1:
        df = g_stocks(stock, start_date, end_date)
    return df


def Delta_Time(years = 0, months = 0, weeks = 0, days = 0, start = dt.datetime.today()):
    """ Delta time function.
    Creates start and end dates. Years, months, weeks, days are set by default to 0, and start date is set to today.
    """

    start_date = start - relativedelta(years = years, months = months, weeks = weeks, days = days)
    end_date = dt.datetime.today()
    return start_date.date(), end_date.date()