import gspread
import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from pandas import DataFrame


class Forma99030:

    def __init__(self, conexion, name_file, name_sheet):
        self.conn = conexion
        self.name_file = name_file
        self.name_sheet = name_sheet
        # Autenticación y acceso a Google Sheets
        self.scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(
            "key.json", self.scope
        )
        client = gspread.authorize(self.creds)
        self.spreadsheet = client.open(
            title=name_file,
        )

        # Selecciona la hoja de Google Sheets
        self.worksheet = self.spreadsheet.worksheet(name_sheet)

        # Construir el servicio de la API de Google Sheets
        self.sheet_service = build("sheets", "v4", credentials=self.creds)

    def get_hist_forma_99030(self) -> DataFrame:
        worksheet = self.worksheet
        all_values = worksheet.get_all_values()
        headers, rows = all_values[0], all_values[1:]

        df_forma99030 = DataFrame(rows, columns=headers)
        cols_montos = [
            "Ventas Base Imponible",
            "Ventas No Gravadas",
            "Débito Fiscal",
            "Compras Base Imponible",
            "Compras No Gravadas",
            "Crédito Fiscal",
            "Exced_cf_m_Ante",
            "Exced_cf_m_Sig",
            "Ret_Acum",
            "Ret_Desc",
            "Ret_Periodo",
        ]  # Lista de columnas que contienen montos

        # Eliminar separadores de miles y reemplazar coma decimal por punto
        for col in cols_montos:
            df_forma99030[col] = (
                df_forma99030[col]
                .str.replace(".", "", regex=False)  # Remove thousand separator
                .str.replace(",", ".", regex=False)  # Replace decimal comma with dot
            )
        # Convertir a float, forzando errores a NaN
        df_forma99030[cols_montos] = df_forma99030[cols_montos].apply(
            pd.to_numeric, errors="raise"  # Convertir a float err
        )
        return df_forma99030

    def planillas_por_registrar(self) -> dict:
        df = self.get_hist_forma_99030()
        # Filtrar las filas que necesitan ser registradas
        df_registrar = df[df["Contabilizar"].str.strip() == "SI"]
        return df_registrar.to_dict("records")


if __name__ == "__main__":
    oForma99030 = Forma99030(
        conexion=None,
        name_file="Historico declaraciones forma 99030 BANTEL",
        name_sheet="data",
    )
    print(oForma99030.planillas_por_registrar())
