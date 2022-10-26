# -*- coding: utf-8 -*-
from datetime import datetime
import ede.ede.validation_functions.validation_functions_facade as facade
from time import sleep
import sys
import os
import ctypes
from ede.ede._logger import logger

from multiprocessing import Process, Manager, current_process
from threading import Thread
import re
from sqlalchemy import create_engine

from sqlalchemy import event
from sqlalchemy.engine import Engine
from typing import Union

from ede.ede.validation_functions.check_utils import validateJSON


@event.listens_for(Engine, "connect")
def sqlite_engine_connect(dbapi_connection: Engine, connection_record: str):
    dbapi_connection.create_function("regexp", 2, sqlite_regexp)
    dbapi_connection.create_function(
        "REGEXP_REPLACE", 3, sqlite_regexp_replace)
    dbapi_connection.create_function("json_valid", 1, validateJSON)


def sqlite_regexp(expr: str, item: str) -> Union[re.Match, bool]:
    if(not item):
        return False
    #logger.info(f"expr: {type(expr)} => {expr}, \nitem: {type(item)} => {item}")
    #reg = re.compile(expr, re.IGNORECASE)
    #reg = re.compile(expr)
    # logger.info(f"reg:{reg}")
    try:
        # logger.info(f"search:{re.search(fr'{expr}',str(item))}")
        return re.search(expr, str(item)) is not None
    except Exception as e:
        print(f'ERROR: {e}')
        return False


def sqlite_regexp_replace(item: str, find: str, repl: str):
    reg = re.compile(find, re.IGNORECASE)
    return reg.sub(repl, item)

# ----------------------------------------------------------------------------
# PASO N° 20 - Transformar el archivo JSON en archivos CSV's. Uno por tabla.
# ----------------------------------------------------------------------------


class check:
    def __init__(self, args):
        self.args = args
        logger.info(
            f"tipo de argumento: {type(self.args)}, valores: {self.args}")
        self.functionsMultiProcess = {
            "fn0FA": facade.fn0FA,
            "fn0FB": facade.fn0FB,
            "fn1FA": facade.fn1FA,
            "fn1FB": facade.fn1FB,
            "fn1FC": facade.fn1FC,
            "fn2FA": facade.fn2FA,
            "fn2EA": facade.fn2EA,
            "fn2DA": facade.fn2DA,
            "fn2DB": facade.fn2DB,
            "fn2CA": facade.fn2CA,
            "fn2CB": facade.fn2CB,
            "fn2BA": facade.fn2BA,
            "fn2AA": facade.fn2AA,
            "fn29A": facade.fn29A,
            "fn29B": facade.fn29B,
            "fn29C": facade.fn29C,
            "fn28A": facade.fn28A,
            "fn28B": facade.fn28B,
            "fn3F0": facade.fn3F0,
            "fn3F1": facade.fn3F1,
            "fn3F2": facade.fn3F2,
            "fn3F3": facade.fn3F3,
            "fn3F4": facade.fn3F4,
            "fn3F5": facade.fn3F5,
            "fn3F6": facade.fn3F6,
            "fn3F7": facade.fn3F7,
            "fn3F8": facade.fn3F8,
            "fn3F9": facade.fn3F9,
            "fn3FA": facade.fn3FA,
            "fn3FB": facade.fn3FB,
            "fn3FC": facade.fn3FC,
            "fn3FD": facade.fn3FD,
            "fn3FE": facade.fn3FE,
            "fn3FF": facade.fn3FF,
            "fn3E0": facade.fn3E0,
            "fn3E1": facade.fn3E1,
            "fn3E2": facade.fn3E2,
            "fn3E3": facade.fn3E3,
            "fn3E4": facade.fn3E4,
            "fn3E5": facade.fn3E5,
            "fn3E6": facade.fn3E6,
            "fn3E7": facade.fn3E7,
            "fn3E8": facade.fn3E8,
            "fn3E9": facade.fn3E9,
            "fn3EA": facade.fn3EA,
            "fn3EB": facade.fn3EB,
            "fn3EC": facade.fn3EC,
            "fn3ED": facade.fn3ED,
            "fn3EE": facade.fn3EE,
            "fn3EF": facade.fn3EF,
            "fn3D0": facade.fn3D0,
            "fn3D1": facade.fn3D1,
            "fn3D2": facade.fn3D2,
            "fn3D3": facade.fn3D3,
            "fn3D4": "No/Verificado",
            "fn3D5": "No/Verificado",
            "fn3D6": "No/Verificado",
            "fn3D7": "No/Verificado",
            "fn3D8": "No/Verificado",
            "fn3D9": facade.fn3D9,
            "fn3DA": facade.fn3DA,
            "fn3DB": "No/Verificado",
            "fn3DC": "No/Verificado",
            "fn3DD": facade.fn3DD,
            "fn3DE": "No/Verificado",
            "fn3DF": "No/Verificado",
            "fn3C0": "No/Verificado",
            "fn3C1": "No/Verificado",
            "fn3C2": "No/Verificado",
            "fn3C3": facade.fn3C3,
            "fn3C4": facade.fn3C4,
            "fn3C5": facade.fn3C5,
            "fn3C6": "No/Verificado",
            "fn3C7": "No/Verificado",
            "fn3C8": "No/Verificado",
            "fn3C9": "No/Verificado",
            "fn3CA": facade.fn3CA,
            "fn4FA": facade.fn4FA,
            "fn5F0": facade.fn5F0,
            "fn5E0": facade.fn5E0,
            "fn5E1": facade.fn5E1,
            "fn5E2": facade.fn5E2,
            "fn5E3": facade.fn5E3,
            "fn5E4": facade.fn5E4,
            "fn5E5": facade.fn5E5,
            "fn5D0": facade.fn5D0,
            "fn6F0": facade.fn6F0,
            "fn6F1": facade.fn6F1,
            "fn6E0": facade.fn6E0,
            "fn6E1": facade.fn6E1,
            "fn6E2": facade.fn6E2,
            "fn6E3": facade.fn6E3,
            "fn6E4": facade.fn6E4,
            "fn6D0": facade.fn6D0,
            "fn6D1": facade.fn6D1,
            "fn6C0": facade.fn6C0,
            "fn6C1": "No/Verificado",
            "fn6C2": facade.fn6C2,
            "fn6B0": facade.fn6B0,
            "fn6B1": "No/Verificado",
            "fn6A0": "No/Verificado",
            "fn6A1": "No/Verificado",
            "fn6A2": "No/Verificado",
            "fn6A3": "No/Verificado",
            "fn690": "No/Verificado",
            "fn680": facade.fn680,
            "fn681": facade.fn681,
            "fn682": facade.fn682,
            "fn7F0": facade.fn7F0,
            "fn7F1": facade.fn7F1,
            "fn7F2": facade.fn7F2,
            "fn7F3": facade.fn7F3,
            "fn7F4": facade.fn7F4,
            "fn7F5": facade.fn7F5,
            "fn7F6": "No/Verificado",
            "fn8F0": facade.fn8F0,
            "fn8F1": facade.fn8F1,
            "fn8F2": facade.fn8F2,
            "fn8F3": facade.fn8F3,
            "fn9F0": facade.fn9F0,
            "fn9F1": facade.fn9F1,
            "fn9F2": facade.fn9F2,
            "fn9F3": facade.fn9F3
        }
        self.functions = {
            "fn0FA": "facade.fn0FA(conn, return_dict)",
            "fn0FB": "facade.fn0FB(conn, return_dict)",
            "fn1FA": "facade.fn1FA(conn, return_dict)",
            "fn1FB": "facade.fn1FB(conn, return_dict)",
            "fn1FC": "facade.fn1FC(conn, return_dict)",
            "fn2FA": "facade.fn2FA(conn, return_dict)",
            "fn2EA": "facade.fn2EA(conn, return_dict)",
            "fn2DA": "facade.fn2DA(conn, return_dict)",
            "fn2DB": "facade.fn2DB(conn, return_dict)",
            "fn2CA": "facade.fn2CA(conn, return_dict)",
            "fn2CB": "facade.fn2CB(conn, return_dict)",
            "fn2BA": "facade.fn2BA(conn, return_dict)",
            "fn2AA": "facade.fn2AA(conn, return_dict)",
            "fn29A": "facade.fn29A(conn, return_dict)",
            "fn29B": "facade.fn29B(conn, return_dict)",
            "fn29C": "facade.fn29C(conn, return_dict)",
            "fn28A": "facade.fn28A(conn, return_dict)",
            "fn28B": "facade.fn28B(conn, return_dict)",
            "fn3F0": "facade.fn3F0(conn, return_dict)",
            "fn3F1": "facade.fn3F1(conn, return_dict, self.args)",
            "fn3F2": "facade.fn3F2(conn, return_dict)",
            "fn3F3": "facade.fn3F3(conn, return_dict)",
            "fn3F4": "facade.fn3F4(conn, return_dict)",
            "fn3F5": "facade.fn3F5(conn, return_dict)",
            "fn3F6": "facade.fn3F6(conn, return_dict)",
            "fn3F7": "facade.fn3F7(conn, return_dict)",
            "fn3F8": "facade.fn3F8(conn, return_dict)",
            "fn3F9": "facade.fn3F9(conn, return_dict)",
            "fn3FA": "facade.fn3FA(conn, return_dict)",
            "fn3FB": "facade.fn3FB(conn, return_dict)",
            "fn3FC": "facade.fn3FC(conn, return_dict)",
            "fn3FD": "facade.fn3FD(conn, return_dict)",
            "fn3FE": "facade.fn3FE(conn, return_dict)",
            "fn3FF": "facade.fn3FF(conn, return_dict)",
            "fn3E0": "facade.fn3E0(conn, return_dict)",
            "fn3E1": "facade.fn3E1(conn, return_dict)",
            "fn3E2": "facade.fn3E2(conn, return_dict)",
            "fn3E3": "facade.fn3E3(conn, return_dict)",
            "fn3E4": "facade.fn3E4(conn, return_dict)",
            "fn3E5": "facade.fn3E5(conn, return_dict)",
            "fn3E6": "facade.fn3E6(conn, return_dict)",
            "fn3E7": "facade.fn3E7(conn, return_dict)",
            "fn3E8": "facade.fn3E8(conn, return_dict)",
            "fn3E9": "facade.fn3E9(conn, return_dict)",
            "fn3EA": "facade.fn3EA(conn, return_dict)",
            "fn3EB": "facade.fn3EB(conn, return_dict)",
            "fn3EC": "facade.fn3EC(conn, return_dict)",
            "fn3ED": "facade.fn3ED(conn, return_dict)",
            "fn3EE": "facade.fn3EE(conn, return_dict)",
            "fn3EF": "facade.fn3EF(conn, return_dict)",
            "fn3D0": "facade.fn3D0(conn, return_dict)",
            "fn3D1": "facade.fn3D1(conn, return_dict)",
            "fn3D2": "facade.fn3D2(conn, return_dict)",
            "fn3D3": "facade.fn3D3(conn, return_dict)",
            "fn3D4": "No/Verificado",
            "fn3D5": "No/Verificado",
            "fn3D6": "No/Verificado",
            "fn3D7": "No/Verificado",
            "fn3D8": "No/Verificado",
            "fn3D9": "facade.fn3D9(conn, return_dict)",
            "fn3DA": "facade.fn3DA(conn, return_dict)",
            "fn3DB": "No/Verificado",
            "fn3DC": "No/Verificado",
            "fn3DD": "facade.fn3DD(conn, return_dict)",
            "fn3DE": "No/Verificado",
            "fn3DF": "No/Verificado",
            "fn3C0": "No/Verificado",
            "fn3C1": "No/Verificado",
            "fn3C2": "No/Verificado",
            "fn3C3": "facade.fn3C3(conn, return_dict)",
            "fn3C4": "facade.fn3C4(conn, return_dict)",
            "fn3C5": "facade.fn3C5(conn, return_dict)",
            "fn3C6": "No/Verificado",
            "fn3C7": "No/Verificado",
            "fn3C8": "No/Verificado",
            "fn3C9": "No/Verificado",
            "fn3CA": "facade.fn3CA(conn, return_dict)",
            "fn4FA": "facade.fn4FA(conn, return_dict)",
            "fn5F0": "facade.fn5F0(conn, return_dict)",
            "fn5E0": "facade.fn5E0(conn, return_dict)",
            "fn5E1": "facade.fn5E1(conn, return_dict)",
            "fn5E2": "facade.fn5E2(conn, return_dict)",
            "fn5E3": "facade.fn5E3(conn, return_dict)",
            "fn5E4": "facade.fn5E4(conn, return_dict)",
            "fn5E5": "facade.fn5E5(conn, return_dict)",
            "fn5D0": "facade.fn5D0(conn, return_dict)",
            "fn6F0": "facade.fn6F0(conn, return_dict)",
            "fn6F1": "facade.fn6F1(conn, return_dict)",
            "fn6E0": "facade.fn6E0(conn, return_dict)",
            "fn6E1": "facade.fn6E1(conn, return_dict)",
            "fn6E2": "facade.fn6E2(conn, return_dict)",
            "fn6E3": "facade.fn6E3(conn, return_dict)",
            "fn6E4": "facade.fn6E4(conn, return_dict)",
            "fn6D0": "facade.fn6D0(conn, return_dict)",
            "fn6D1": "facade.fn6D1(conn, return_dict)",
            "fn6C0": "facade.fn6C0(conn, return_dict)",
            "fn6C1": "No/Verificado",
            "fn6C2": "facade.fn6C2(conn, return_dict)",
            "fn6B0": "facade.fn6B0(conn, return_dict)",
            "fn6B1": "No/Verificado",
            "fn6A0": "No/Verificado",
            "fn6A1": "No/Verificado",
            "fn6A2": "No/Verificado",
            "fn6A3": "No/Verificado",
            "fn690": "No/Verificado",
            "fn680": "facade.fn680(conn, return_dict)",
            "fn681": "facade.fn681(conn, return_dict)",
            "fn682": "facade.fn682(conn, return_dict)",
            "fn7F0": "facade.fn7F0(conn, return_dict)",
            "fn7F1": "facade.fn7F1(conn, return_dict)",
            "fn7F2": "facade.fn7F2(conn, return_dict)",
            "fn7F3": "facade.fn7F3(conn, return_dict)",
            "fn7F4": "facade.fn7F4(conn, return_dict)",
            "fn7F5": "facade.fn7F5(conn, return_dict)",
            "fn7F6": "No/Verificado",
            "fn8F0": "facade.fn8F0(conn, return_dict)",
            "fn8F1": "facade.fn8F1(conn, return_dict)",
            "fn8F2": "facade.fn8F2(conn, return_dict)",
            "fn8F3": "facade.fn8F3(conn, return_dict)",
            "fn9F0": "facade.fn9F0(conn, return_dict)",
            "fn9F1": "facade.fn9F1(conn, return_dict)",
            "fn9F2": "facade.fn9F2(conn, return_dict)",
            "fn9F3": "facade.fn9F3(conn, return_dict)"
        }
        if (self.args.function):
            __value = self.functions.get(self.args.function, None)
            if(__value):
                self.functions = {self.args.function: __value}

        self.args._FKErrorsFile = f'./{self.args.t_stamp}_ForenKeyErrors.csv'

    # ----------------------------------------------------------------------------
    # Transforma archivo JSON en un DataFrame de pandas con todas sus columnas.
    # Agrega las columnas que faltan.
    # ----------------------------------------------------------------------------

    def execute(self):
        _result = True
        sec = self.args.secPhase
        path = self.args.path_to_DB_file
        engine = create_engine(f"sqlite+pysqlcipher://:{sec}@/{path}?cipher=aes-256-cfb&kdf_iter=64000"
                               # ,connect_args={'timeout': 10000}
                               )
        try:
            conn = engine.connect()
        except Exception as e:
            logger.error(f"ERROR al realizar la conexión con la BD: {str(e)}")
            return False
        try:
            logger.info(
                f"Sistema ejecutandose con restrición de tiempo de {self.args.time} segundos...")
            
            if "sequential" in dir(self.args) and self.args.sequential:
                return_dict = dict()
                #return_dict = self.execute_sequentially(conn)
                self.execute_sequentially(conn, return_dict)
            else:
               
                return_dict = self.execute_parallel(conn)


            logger.info(return_dict)
            _result = all(list(return_dict.values()))
            if(not _result):
                logger.error(
                    "--------- EL ARCHIVO NO CUMPLE CON EL ESTÁNDAR DE DATOS PARA LA EDUCACIÓN ----------")

        except Exception as e:
            pass
        finally:
            conn.close()  # closind database connection
            return _result
    def execute_sequentially(self, conn, return_dict):
        try:
            for key, value in self.functions.items():
                if(value != "No/Verificado"):
                    start_time = datetime.now()
                    if self.args.time > 0 and timediff > self.args.time:
                        logger.error("TIMEOUT EJECUCION SECUENCIAL.............")
                        break
                    fnTarget = self.functionsMultiProcess[key]
                    logger.info(f"{fnTarget.__name__} iniciando...")
                    if key == "fn3F1":
                        fnTarget.__call__(conn, return_dict, self.args)
                    else:
                        fnTarget.__call__(conn, return_dict)
                    timediff = (datetime.now()-start_time).total_seconds()
                    logger.info(f"function: {key}, time elapsed: {timediff}")
            return return_dict
        except Exception as e:
            logger.error(f"Error general en ejecución secuencial: {e}")
            return False
    def execute_parallel(self, conn):
        try:
            manager = Manager()
            return_dict = manager.dict()
            jobs = []
            for key, value in self.functions.items():
                if(value != "No/Verificado"):
                    

                    fnTarget = self.functionsMultiProcess[key]
                    if key == "fn3F1":
                        p : Process = Process(target=fnTarget, name=fnTarget.__name__, args=(
                            conn, return_dict, self.args,))
                    else:
                        p : Process = Process(target=fnTarget, name=fnTarget.__name__, args=(
                            conn, return_dict,))
                    jobs.append(p)
            time = 0
            for p in jobs:
                logger.info(f"{p.name} iniciando...")
                p.start()

            while True:
                time += 1
                if any(p.is_alive() for p in jobs):
                    if(self.args.time > 0 and time >= self.args.time):
                        for p in jobs:
                            if p.is_alive():  # If thread is active
                                p.terminate()
                                logger.error(f"TIMEOUT: {p}")
                        break

                else:
                #    if not self.args.parallel:
                #        for p in jobs:
                #            p.join()
                    break
                sleep(1)
            return return_dict
        except Exception as e:
            logger.error(f"Error general en ejecución en paralelo {e}")
            

            
