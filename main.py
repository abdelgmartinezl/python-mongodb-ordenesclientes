import pymongo

# Conexion a la base de datos y definicion de estructuras
cliente = pymongo.MongoClient("mongodb://localhost/")
db = cliente["pedidos"]
collection_ordenes = db["ordenes"]
collection_clientes = db["clientes"]

# Insercion de documentos en las colecciones
def crear_orden(dato_orden):
    insertado = collection_ordenes.insert_one(dato_orden)
    return insertado.inserted_id

def crear_cliente(dato_cliente):
    insertado = collection_clientes.insert_one(dato_cliente)
    return insertado.inserted_id

# Lectura de documentos en las colecciones
def leer_todas_ordenes():
    return list(collection_ordenes.find())

def leer_ordenes_por_cliente(id_cliente):
    return list(collection_ordenes.find({"id_cliente": id_cliente}))

def leer_ordenes_totales_por_cliente():
    pipeline = [
        {"$group": {"_id": "$id_cliente", "total": {"$sum": 1}}}
    ]
    return list(collection_ordenes.aggregate(pipeline))

def leer_ordenes_con_informacion_cliente():
    pipeline = [
        {"$lookup": {"from": "clientes", "localField": "id_cliente",
                     "foreignField": "_id", "as": "cliente"}
        }
    ]
    return list(collection_ordenes.aggregate(pipeline))

# Actualizacion de datos
def actualizar_orden(id_orden, nuevo_dato):
    actualizado = collection_ordenes.update_one({"_id": id_orden},
                                                {"$set": nuevo_dato})
    return actualizado.modified_count

# Eliminacion de datos
def eliminar_orden(id_orden):
    eliminado = collection_ordenes.delete_one({"_id": id_orden})
    return eliminado.deleted_count

if __name__ == "__main__":
    cliente1_id = crear_cliente({"nombre": "Petra", "correo": "petra@ejemplo.com"})
    cliente2_id = crear_cliente({"nombre": "Toribia", "correo": "toribia@chevere.com"})

    crear_orden({"id_cliente": cliente1_id, "producto": "Corn Flakes", "cantidad": 2})
    crear_orden({"id_cliente": cliente1_id, "producto": "Leche", "cantidad": 1})
    crear_orden({"id_cliente": cliente2_id, "producto": "Pescado", "cantidad": 3})

    print("Todas las ordenes:")
    print(leer_todas_ordenes())
    print("\n\n")

    print("Ordenes por cliente:")
    print(leer_ordenes_por_cliente(cliente1_id))
    print("\n")
    print(leer_ordenes_por_cliente(cliente2_id))
    print("\n\n")

    print("Orden agregado por cliente:")
    print(leer_ordenes_totales_por_cliente())
    print("\n\n")

    print("Orden por infomacion de cliente:")
    print(leer_ordenes_con_informacion_cliente())
    print("\n\n")

    orden_por_actualizar = leer_ordenes_por_cliente(cliente1_id)[0]
    actualizar_orden(orden_por_actualizar["_id"], {"cantidad": 5})
    print("Orden actualizada: ")
    resultado = leer_ordenes_con_informacion_cliente()
    for r in resultado:
        print("\n--REGISTRO DE ORDEN--")
        print(r['cliente'][0]['nombre'] + " compro " + str(r['cantidad']) + " " + r['producto'])
    print("\n\n")

    orden_por_eliminar = leer_todas_ordenes()[0]
    eliminar_orden(orden_por_eliminar["_id"])
    print("Luego de eliminacion: ")
    print(leer_todas_ordenes())
    print("\n\n")