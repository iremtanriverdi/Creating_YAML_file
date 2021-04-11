# I have dataset whose size is 4GB
# First, I try to read the data with pandas

import pandas as pd

df=pd.read_csv('chess_games.csv', sep=',') #data were not read


# Secondly, I try to read with Modin
import modin.pandas as pd
from distributed import Client
client = Client()
data = pd.read_csv('chess_games.csv', sep=',') #taking long time to read
data

# Thirdly, I try to read the data with Dask

import dask.dataframe as dd

df = dd.read_csv('chess_games.csv', sep=',') #dataset readed easily
df
