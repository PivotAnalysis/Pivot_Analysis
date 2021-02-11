from nsepy import get_history
from datetime import date
import numpy as np

data = get_history (symbol = "NIFTY", start = date (2020, 12, 1), end = date.today (), index = True, futures = True,
                    expiry_date = date (2021, 2, 25))
data = data.sort_index(ascending = False)
import pandas as pd
from pandas import DataFrame
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
df1 = df.filter (['Date', 'Symbol', 'Expiry', 'Close', 'H3', 'H4', 'H5', 'L3', 'L4', 'L5'], axis = 1)
export_csv = df1.to_csv (r'C:\Users\AnujSharma\Google Drive\Nifty.csv', index = True, header = True)
