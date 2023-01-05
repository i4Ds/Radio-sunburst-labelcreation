import pandas as pd

def explode_instruments_long_clean_instruments(data):
    data['instruments'] = data['instruments'].str.split(',')
    data = data.explode('instruments')
    data['instruments'] = data['instruments'].str.strip()
    data['instruments'] = data['instruments'].replace({
        '[': '',
        ']': '',
        '(' : '',
        ')' : '',
    })
    return data.reset_index(drop=True)

def keep_only_type_I_to_VI(data):
    data = data[data.type.isin(['I', 'II', 'III', 'IV', 'V', 'VI'])]
    return data