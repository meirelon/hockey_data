import os
import glob
import pathlib
import shutil

dlist = ["data", "stats"]


def get_folder(p=dlist, new_folder=None):
    root = "/".join(p)
    if new_folder:
        directory = os.path.join(root, new_folder)
    else:
        directory = root
    if not os.path.exists(directory):
        os.mkdir(directory)
    return directory


def remove_folder(directory):
    shutil.rmtree(directory)
    return True


def wildcard_files(wildcard_directory):
    return [f for f in glob.glob(wildcard_directory)]


def get_data_file_paths(mypath="data") -> dict:
    p = pathlib.Path('data').glob('**/*')
    return {x.name: str(x) for x in p if x.is_file()}


def write_df_to_csv(df, fp):
    directory = "/".join(fp)
    df.to_csv(directory, index=False)
    return directory


if __name__ == "__main__":
    print(get_data_file_paths())
