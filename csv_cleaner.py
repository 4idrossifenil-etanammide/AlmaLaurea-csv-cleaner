import pandas as pd
import os
import argparse

def split_column(dataframe, delimiter):
    column_name = dataframe.columns.values[0]

    dataframe[column_name] = dataframe[column_name].astype(str).str.strip()
    return dataframe[column_name].str.split(delimiter, expand=True)

def delete_empty_values(dataframe, text_qualifier):
    dataframe = dataframe.replace(text_qualifier + text_qualifier, None)
    dataframe = dataframe.replace(text_qualifier + chr(160) + text_qualifier, None)
    dataframe.fillna("",inplace=True)
    return dataframe

def remove_text_qualifiers(value, text_qualifier):
    return value.strip(text_qualifier)

def clean_text_qualifiers(dataframe, text_qualifier):
    dataframe = dataframe.applymap(lambda x: remove_text_qualifiers(x, text_qualifier) if isinstance(x, str) else x)
    return dataframe

def clean_dataframe(df, delimiter, text_qualifier):
    df = split_column(df, delimiter)
    df = delete_empty_values(df, text_qualifier)
    df = clean_text_qualifiers(df, text_qualifier)
    return df

def aggregate_columns(dataframe):
    dataframe[''] = dataframe.apply(lambda row: ''.join(row), axis=1)
    dataframe.drop(dataframe.columns[:-1], axis=1, inplace=True)
    return dataframe

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--delimiter", help="Specify the delimiter (must be passed a string). Default value ;", default=";", type=str)
parser.add_argument("-t", "--text_qualifier", help="Specify the text qualifier (must be passed a string). Default value \"", default="\"", type=str)
parser.add_argument("-o", "--output", help="Specify the name of the output dir. If not given, it will be created as \"output\" (must be passed a string)", default="./output", type=str)

args = parser.parse_args()
delimiter = args.delimiter
text_qualifier = args.text_qualifier
output_dir = args.output

if not os.path.isdir(output_dir):
    os.mkdir(output_dir)
for file_name in os.listdir("."):
    if file_name.endswith(".csv"):
        print(file_name)
        df = pd.read_csv(file_name, encoding='windows-1252', sep='delimiter', header=None, engine="python")
        if(len(df.columns) != 1):
            df = aggregate_columns(df)
        df = clean_dataframe(df, delimiter, text_qualifier)
        df.to_csv(os.path.join(output_dir, file_name.split(".")[0] + "_out.csv"), encoding='windows-1252', index=False)


