from datetime import date
import os
import psycopg2

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

def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()

from nsepy import get_history

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

df['Top Central'] = df.apply(lambda df:df['TC'] if df['TC'] > df['BC'] else df['BC'],axis = 1)
df['Bottom Central'] = df.apply(lambda df:df['BC'] if df['TC'] > df['BC'] else df['TC'],axis = 1)


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

from sqlalchemy import create_engine

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
    engine = create_engine (connect)
    df.to_sql (
        'test_table',
        con = engine,
        index = False,
        if_exists = 'replace'
    )
    print ("to_sql() done (sqlalchemy)")



from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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

from pretty_html_table import build_table
print(df2.head())

def trading_plan():
    plan = df2
    output = build_table(plan, 'blue_light')
    send_mail(output)
    return "Mail sent successfully."


if __name__ == "__main__":
    to_alchemy (df2)
    trading_plan()
