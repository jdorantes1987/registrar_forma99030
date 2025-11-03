import sys
from pandas import to_datetime

from read_forma99030 import Forma99030

sys.path.append("../profit")  # Ajusta la ruta según tu estructura de carpetas
from data.mod.banco import orden_pago  # noqa: E402


class GestionOrdenPago:
    def __init__(self, db, name_file, name_sheet):
        self.db = db
        self.oForma99030 = Forma99030(self.db, name_file, name_sheet)
        self.oOrden = orden_pago.OrdenPago(self.db)

    def procesar_orden_pago(
        self,
        fecha_ultima_orden,
    ):
        self.db.autocommit(False)
        data = self.oForma99030.planillas_por_registrar()
        if not data:
            print("No hay planillas por registrar.")
            return

        last_id_orden = self.oOrden.get_last_id_orden(fecha_ultima_orden)

        for index, row in enumerate(data):
            new_id_orden = self.oOrden.get_next_num_orden(last_id_orden)
            print(f"Nuevo ID de orden: {new_id_orden}")

            # Convertir a formato YYYYMMDD
            row["Fecha_Contabilizar"] = to_datetime(
                row["Fecha_Contabilizar"], format="%d/%m/%Y", errors="coerce"
            ).strftime("%Y%m%d")

            payload_orden = {
                "ord_num": new_id_orden,
                "status": "S",
                "fecha": row["Fecha_Contabilizar"],
                "cod_ben": "G200003030",
                "descrip": f"COMPENSACIÓN IVA - {row['Periodo']} - {row['Fecha']}",
                "forma_pag": "EF",
                "fec_pag": row["Fecha_Contabilizar"],
                "cod_caja": "001",
                "tasa": 1,
                "co_mone": "BS",
                "anulado": 0,
                "sino_reten": 0,
                "pagar": 0,
                "co_us_in": "JACK",
                "co_sucu_in": "01",
                "co_us_mo": "JACK",
                "co_sucu_mo": "01",
            }
            safe1 = self.oOrden.normalize_payload_orden(payload_orden)
            id_orden = self.oOrden.create_orden(safe1)
            print(f"id_orden: {id_orden}")

            item = []
            # Inicializar contador de renglones antes de procesar detalles
            renglon_num = 0
            # Definición de las cuentas y montos
            cuentas = [
                ("Débito Fiscal", "2-4-01-02-0002", 0.0, "monto_h"),
                ("Crédito Fiscal", "1-4-04-01-0001", "monto_d", 0.0),
                ("Exced_cf_m_Ante", "1-4-04-01-0002", "monto_d", 0.0),
                ("Exced_cf_m_Sig", "1-4-04-01-0002", 0.0, "monto_h"),
                ("Ret_Desc", "1-4-04-02-0001", "monto_d", 0.0),
            ]

            det_procesado = True  # Indicador de éxito para los detalles
            # Procesar cada cuenta
            for idx, (campo, cta_ie, monto_d, monto_h) in enumerate(cuentas, start=0):
                valor = abs(row.get(campo, 0))
                if valor > 0:
                    renglon_num += 1
                    item_dict = {
                        "reng_num": renglon_num,
                        "ord_num": new_id_orden,
                        "co_cta_ingr_egr": cta_ie,
                        "monto_d": 0.0,
                        "monto_h": 0.0,
                        "monto_iva": 0.0,
                        "porc_retn": 0.0,
                        "sustraendo": 0.0,
                        "monto_reten": 0.0,
                        "tipo_imp": "7",
                        "co_us_in": "JACK",
                        "co_sucu_in": "01",
                        "co_us_mo": "JACK",
                        "co_sucu_mo": "01",
                    }
                    if monto_d == "monto_d":
                        item_dict["monto_d"] = valor
                    else:
                        item_dict["monto_d"] = monto_d
                    if monto_h == "monto_h":
                        item_dict["monto_h"] = valor
                    else:
                        item_dict["monto_h"] = monto_h
                    item.append(item_dict)

                    safe2 = self.oOrden.normalize_payload_det_orden(item_dict)
                    # print(f"Items a registrar: {item}")
                    detalle_id = self.oOrden.create_det_orden(safe2)
                    if not detalle_id:
                        det_procesado = False
                    print(f"detalle_id: {detalle_id}")

            if id_orden and det_procesado:
                self.db.commit()
            else:
                self.db.rollback()

            self.db.autocommit(True)


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    sys.path.append("../conexiones")

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para SQL Server
    sqlserver_connector = SQLServerConnector(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        database=os.environ["DB_NAME_DERECHA_PROFIT"],
        user=os.environ["DB_USER_PROFIT"],
        password=os.environ["DB_PASSWORD_PROFIT"],
    )
    sqlserver_connector.connect()
    db = DatabaseConnector(sqlserver_connector)
    oGestionOrdenPago = GestionOrdenPago(
        db,
        name_file="Historico declaraciones forma 99030 BANTEL",
        name_sheet="data",
    )
    oGestionOrdenPago.procesar_orden_pago(fecha_ultima_orden="20251031")
    db.close_connection()
