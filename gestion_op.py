import sys
from read_forma99030 import Forma99030

sys.path.append("..\\profit")
from data.mod.banco import orden_pago


class GestionOrdenPago:
    def __init__(self, conexion, name_file, name_sheet):
        self.conn = conexion
        self.oForma99030 = Forma99030(self.conn, name_file, name_sheet)
        self.oOrden = orden_pago.OrdenPago(self.conn)

    def procesar_orden_pago(
        self,
    ):
        data = self.oForma99030.planillas_por_registrar()
        last_id_orden = self.oOrden.get_last_id_orden("20250531")
        # Recorre el diccionario de datos
        for index, row in enumerate(data):
            print(f"Procesando fila {index}: {row['Planilla']}")
            new_id_orden = self.oOrden.get_next_num_orden(last_id_orden)
            print(f"Nuevo ID de orden: {new_id_orden}")


if __name__ == "__main__":
    import os

    sys.path.append("..\\profit")
    from conn.conexion import DatabaseConnector
    from data.mod.banco import orden_pago
    from dotenv import load_dotenv

    load_dotenv()
    # Para SQL Server
    datos_conexion = dict(
        host=os.environ["HOST_PRODUCCION_PROFIT"],
        base_de_datos=os.environ["DB_NAME_DERECHA_PROFIT"],
    )
    oConexion = DatabaseConnector(db_type="sqlserver", **datos_conexion)
    oGestionOrdenPago = GestionOrdenPago(
        oConexion,
        name_file="Historico declaraciones forma 99030 BANTEL",
        name_sheet="data",
    )
    oGestionOrdenPago.procesar_orden_pago()
