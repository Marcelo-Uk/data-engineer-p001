#importing libraries
import pandas as pd

#chossing field positions (according to pdf layout file) to form the columns
def read_files(path, name_file, year_date, type_file):
    _file = f'{path}{name_file}{year_date}.{type_file}'

    colspecs = [(2,10),
                (10,12),
                (12,24),
                (27,39),
                (56,69),
                (69,82),
                (82,95),
                (108,121),
                (152,170),
                (170,188)
    ]
    #naming columns
    names = ['data_pregao','codbdi','sigla_acao','nome_acao','preco_abertura','preco_maximo','preco_minimo','preco_fechamento','qtd_negocios','volume_negocios' ]

    #creating dataframe
    df = pd.read_fwf(_file, colspecs = colspecs, names = names, skiprows = 1) #skiprows to avoid header

    return df

#transforming fields -- filters
def filter_stocks(df):
    df = df [df['codbdi'] == 2] #making a filter
    df = df.drop(['codbdi'], axis=1) #deleting the column from the view

    return df

#data field adjustment
def parse_date(df):
    df['data_pregao']= pd.to_datetime(df['data_pregao'], format = '%Y%m%d')
    
    return df

#numeric fields adjustment
def parse_values(df):
    df['preco_abertura'] = (df['preco_abertura'] / 100).astype(float)
    df['preco_maximo'] = (df['preco_maximo'] / 100).astype(float)
    df['preco_minimo'] = (df['preco_minimo'] / 100).astype(float)
    df['preco_fechamento'] = (df['preco_fechamento'] / 100).astype(float)

    return df

#merging files
def concat_files(path, name_file, year_date, type_file, final_file):


    for i, y in enumerate(year_date):
        df = read_files(path, name_file, y, type_file)
        df = filter_stocks(df)
        df = parse_date(df)
        df = parse_values(df)

        if i == 0:
            dt_final = df
        else:
            dt_final = pd.concat([dt_final, df])

    dt_final.to_csv(f'{path}{final_file}', index=False)

#running ETL program

year_date = ['2022', '2023', '2024']
path = f'G://Meu Drive//Profissional//Data-Engineer//projects//data-engineer-p001//'
name_file = 'COTAHIST_A'
type_file = 'txt'
final_file = 'all_bovespa.csv'
concat_files(path, name_file, year_date, type_file, final_file)