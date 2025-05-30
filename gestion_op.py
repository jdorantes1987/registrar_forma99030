import sys
from read_forma99030 import Forma99030

sys.path.append("..\\profit")  # Ajusta la ruta según tu estructura de carpetas
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

        try:
            # Recorre el diccionario de datos
            for index, row in enumerate(data):
                print(f"Procesando fila {index}: {row['Planilla']}")
                new_id_orden = self.oOrden.get_next_num_orden(last_id_orden)
                print(f"Nuevo ID de orden: {new_id_orden}")
                item = [
                    dict(
                        cta_ie="2-4-01-02-0002",
                        monto_d=0.0,
                        monto_h=abs(row["Débito Fiscal"]),
                    ),
                    dict(
                        cta_ie="1-4-04-01-0001",
                        monto_d=abs(row["Crédito Fiscal"]),
                        monto_h=0.0,
                    ),
                    dict(
                        cta_ie="1-4-04-01-0002",
                        monto_d=abs(row["Exced_cf_m_Ante"]),
                        monto_h=0.0,
                    ),
                    dict(
                        cta_ie="1-4-04-01-0002",
                        monto_d=0.0,
                        monto_h=abs(row["Exced_cf_m_Sig"]),
                    ),
                    dict(
                        cta_ie="1-4-04-02-0001",
                        monto_d=abs(row["Ret_Desc"]),
                        monto_h=0.0,
                    ),
                ]
                print(f"Items a registrar: {item}")
                # Registrar la orden de pago
                self.oOrden.registrar_orden_pago(
                    num_orden=new_id_orden,
                    cod_ben="G200003030",
                    fecha_emision="20250531",
                    descripcion="COMPENSACIÓN IVA"
                    + " - "
                    + row["Periodo"]
                    + " - "
                    + row["Fecha"],
                    num_mov_caja="NULL",
                    num_mov_banco="NULL",
                    doc_num="NULL",
                    cod_caja="001",
                    cod_cta="NULL",
                    data_detalle=item,
                )

            self.oOrden.confirmar_insercion_orden_compra()
            print("Ordenes de pago registradas correctamente.")
        except Exception as e:
            print(f"Error al registrar orden de pago: {e}")
            self.oOrden.dehacer_insercion_orden_compra()


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
