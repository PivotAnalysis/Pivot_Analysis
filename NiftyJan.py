from datetime import date
import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as db
from nsepy import get_history
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pretty_html_table import build_table


param_dic = {
    "host"      : "ec2-54-162-119-125.compute-1.amazonaws.com",
    "database"  : "d2doea77iug85",
    "user"      : "znttynzprdtcvq",
    "password"  : "5c113f83faecec6c72daed5b666a3e30f025914e1042cd7964582097fcfac528"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    return conn


data = get_history (symbol = "NIFTY", start = date (2020, 12, 1), end = date.today (), index = True, futures = True,
                    expiry_date = date (2021, 2, 25))
data = data.sort_index(ascending = False)
df = data
H3 = round (df.Close + (df.High - df.Low) * 1.1 / 4, 2)
H4 = round (df.Close + (df.High - df.Low) * 1.1 / 2, 2)
H5 = round (df.Close * (df.High / df.Low), 2)
L3 = round (df.Close - (df.High - df.Low) * 1.1 / 4, 2)
L4 = round (df.Close - (df.High - df.Low) * 1.1 / 2, 2)
df['H3'] = H3
df['H4'] = H4
df['H5'] = H5
L5 = round (df.Close - (df.H5 - df.Close), 2)
df['L3'] = L3
df['L4'] = L4
df['L5'] = L5
P = round((df.High + df.Low + df.Close)/3 ,2)
BC = round((df.High + df.Low)/2,2)
TC = round((P-BC)+P,2)
df['Central Pivot'] = P
df['TC'] = TC
df['BC'] = BC

"""df['Top Central'] = df.apply(lambda df:df['TC'] if df['TC'] > df['BC'] else df['BC'],axis = 1)
df['Bottom Central'] = df.apply(lambda df:df['BC'] if df['TC'] > df['BC'] else df['TC'],axis = 1)"""


if df['H3'].iloc[0] == df['H3'].iloc[1] and df['L3'].iloc[0] == df['L3'].iloc[1]:
    df['2 Day Relationship'] = "Unchanged Value"
elif df['H3'].iloc[0] > df['H3'].iloc[1] and df['L3'].iloc[0] < df['L3'].iloc[1]:
    df['2 Day Relationship'] = "Outside Value"
elif df['H3'].iloc[0] < df['H3'].iloc[1] and df['L3'].iloc[0] > df['L3'].iloc[1]:
    df['2 Day Relationship'] = "Inside Value"
elif df['L3'].iloc[0] > df['H3'].iloc[1]:
    df['2 Day Relationship'] = "Higher Value"
elif df['H3'].iloc[0] > df['H3'].iloc[1] and df['L3'].iloc[0] > df['L3'].iloc[1]:
    df['2 Day Relationship'] = "Overlapping Higher Value"
elif df['H3'].iloc[0] < df['L3'].iloc[1]:
    df['2 Day Relationship'] = "Lower Value"
elif df['H3'].iloc[0] < df['H3'].iloc[1] and df['L3'].iloc[0] < df['L3'].iloc[1]:
    df['2 Day Relationship'] = "Overlapping Lower Value"
else:
    df['2 Day Relationship'] = "Not Applicable"

def expected_outcome(arg):
    switcher = {
        "Higher Value" : "Bullish",
        "Overlapping Higher Value": "Moderately Bullish",
        "Lower Value": "Bearish",
        "Overlapping Lower Value": "Moderately Bearish",
        "Unchanged Value": "Sideways / Breakout",
        "Outside Value":"Sideways",
        "Inside Value": "Breakout"
    }
    return switcher.get(arg,"Nothing")
arg = df['2 Day Relationship'].iloc[0]
df['Expected Outcome'] = expected_outcome(arg)
df1 = df.filter (['Date', 'Symbol', 'Expiry','High','Low', 'Close', 'H3', 'H4', 'H5', 'L3', 'L4', 'L5','Central Pivot','Top Central','Bottom Central'], axis = 1)
df2 = df.filter (['Date', 'Symbol', 'Expiry','High','Low', 'Close', 'H3','L3','Central Pivot','Top Central','Bottom Central','2 Day Relationship','Expected Outcome'], axis = 1)
df3 = df2[0:1]


connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    param_dic['user'],
    param_dic['password'],
    param_dic['host'],
    param_dic['database']
)

def to_alchemy(df):
    """
    Using a dummy table to test this call library
    """
    engine = create_engine(connect)
    df.to_sql (
        'test_table',
        con = engine,
        index = False,
        if_exists = 'replace'
    )
    print ("to_sql() done (sqlalchemy)")

def read_query():
    engine = create_engine (connect)
    connection = engine.connect()
    metadata = db.MetaData ()
    records = db.Table ('test_table', metadata, autoload = True, autoload_with = engine)
    query = db.select([records])
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()
    return_dataframe = pd.DataFrame(ResultSet)
    print ("reading done from sql() via (sqlalchemy)")
    return return_dataframe

def send_mail(body):

    message = MIMEMultipart()
    message['Subject'] = 'Trading Plan Sheet - Nifty'
    message['From'] = 'pivotscamarilla@gmail.com'
    message['To'] = 'anujsharma4@gmail.com'
    password = 'camarillapivots'

    body_content = body
    message.attach(MIMEText(body_content, "html"))
    msg_body = message.as_string()

    server = SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(message['From'], password)
    server.sendmail(message['From'], message['To'], msg_body)
    server.quit()




def trading_plan(rec):
    plan = rec
    output = build_table(plan, 'blue_light')
    send_mail(output)
    return "Mail sent successfully."


if __name__ == "__main__":
    to_alchemy(df2)
    record = read_query()
    trading_plan(record)
