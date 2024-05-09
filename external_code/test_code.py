import pandas as pd

def read_excel_file():
    # Path ke file Excel yang akan dibaca
    excel_file_path = 'external_code/file_example_XLS_10.xls'
    
    # Baca file Excel
    df = pd.read_excel(excel_file_path)

    # Tampilkan isi dataframe
    print(df)