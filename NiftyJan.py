from datetime import date
import os
import pandas as pd
import numpy as np
from nsepy import get_history
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pretty_html_table import build_table

def Nifty_data():
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
    P = round((df.High + df.Low + df.Close)/3,2)
    BC = round((df.High + df.Low)/2,2)
    TC = round((P-BC)+P,2)
    df['Central Pivot'] = P
    df['TC'] = TC
    df['BC'] = BC

    df['Top Central'] = df.apply(lambda df:df['TC'] if df['TC'] > df['BC'] else df['BC'],axis = 1)
    df['Bottom Central'] = df.apply(lambda df:df['BC'] if df['TC'] > df['BC'] else df['TC'],axis = 1)


    conditions = [
        ((df['H3']== df['H3'].shift(-1))&(df['L3'] == df['L3'].shift(-1))),
        ((df['H3'] > df['H3'].shift(-1))&(df['L3'] < df['L3'].shift(-1))),
        ((df['H3'] < df['H3'].shift(-1))&(df['L3'] > df['L3'].shift(-1))),
        (df['L3'] > df['H3'].shift(-1)),
        ((df['H3'] > df['H3'].shift(-1))& (df['L3'] > df['L3'].shift(-1))),
        (df['H3'] < df['L3'].shift(-1)),
        ((df['H3'] < df['H3'].shift(-1)) & (df['L3'] < df['L3'].shift(-1)))
    ]

    values = [
        "Unchanged Value",
        "Outside Value",
        "Inside Value",
        "Higher Value",
        "Overlapping Higher Value",
        "Lower Value",
        "Overlapping Lower Value"
    ]
    df["2 Day Relationship"] = np.select(conditions,values)


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

    df['Expected Outcome'] =df.apply(lambda df:expected_outcome(df['2 Day Relationship']),axis=1)


    df1 = df.filter (['Date', 'Symbol', 'Expiry','High','Low', 'Close', 'H3','L3','Central Pivot','Top Central','Bottom Central','2 Day Relationship','Expected Outcome'], axis = 1)

    return (df1)

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
    plan = Nifty_data()
    trading_plan(plan)
   
