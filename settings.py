import os
import glob
import json
import pathlib
import shutil
import logging

logging.basicConfig(
    filename="logging/settings.log",
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
)


def create_folder(fp):
    if not os.path.exists(fp):
        os.mkdir(fp)


def remove_folder(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
        return True


def wildcard_files(wildcard_directory):
    return [f for f in glob.glob(wildcard_directory)]


def get_data_file_paths(mypath="data") -> dict:
    p = pathlib.Path("data").glob("**/*")
    return {x.name: str(x) for x in p if x.is_file()}


def write_df_to_storage(df, fp, fn, file_type="csv"):
    create_folder(fp)
    directory = f"{fp}/{fn}.{file_type}"
    if file_type == "csv":
        df.to_csv(directory, index=False)
    elif file_type == "json":
        df.to_json(
            path_or_buf=directory, orient="records", lines=True, compression=None
        )
    else:
        raise ("Please specific file type: .csv or .json")
    return directory


def temp_files_to_df(wildcard, fp_temp, fp_final, fn):
    import pandas as pd

    temp_list = []
    for file in wildcard_files(f"{fp_temp}/{wildcard}"):
        try:
            df = pd.read_csv(file)
            temp_list.append(df)
        except Exception as e:
            logging.info(f"{file.split('/')[-1]}: {e}")
    df = pd.concat(temp_list)
    write_df_to_storage(df, fp_final, fn, file_type="csv")
    remove_folder(fp_temp)


def write_json_to_storage(j, fp, fn):
    create_folder(fp)
    directory = f"{fp}/{fn}"
    jsonString = json.dumps(j)
    jsonFile = open(directory, "w")
    jsonFile.write(jsonString)
    jsonFile.close()


def append_df(df, fp):
    if not os.path.isfile(fp):
        df.to_csv(fp, index=False)
    else:  # else it exists so append without writing the header
        df.to_csv(fp, mode="a", header=False, index=False)
