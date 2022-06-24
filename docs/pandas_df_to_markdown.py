import pandas as pd


def pandas_df_to_markdown():
    """
    transforms table.json to md for readme
    python3 pandas_df_to_markdown.py
    """
    df = pd.read_json("metadata/table.json")
    print(df.to_markdown())


if __name__ == "__main__":
    pandas_df_to_markdown()
