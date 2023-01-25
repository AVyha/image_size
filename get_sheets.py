from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd


def get_sheet():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credential = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scopes=scope)
    client = gspread.authorize(credential)

    sheet = client.open("Copy of Parser_ImageSize")

    return client, sheet


def get_data_from_sheet(sheet):
    df = pd.DataFrame(sheet.sheet1.get_all_records())

    return df
