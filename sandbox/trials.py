import pandas as pd


def add_rows_inline(adf):
    new_data = {'A': 10, 'B': 20, 'C': 30}
    adf.loc[len(df)] = new_data

if __name__ == '__main__':

    # Ursprüngliches DataFrame
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6],
        'C': [7, 8, 9]
    })

    # Funktion aufrufen
    add_rows_inline(df)

    # Anzeigen des resultierenden DataFrames
    print(df)
