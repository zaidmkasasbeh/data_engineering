import pandas as pd


def csv_to_json():
    df = pd.read_csv('data.csv')
    df.to_json('fromNIFI.json', orient='records')
