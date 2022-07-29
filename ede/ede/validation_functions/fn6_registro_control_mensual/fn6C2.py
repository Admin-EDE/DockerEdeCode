from inspect import getframeinfo, currentframe
from multiprocessing import current_process
from datetime import datetime

from ede.ede._logger import logger


def fn6C2(conn, return_dict):
    """ Breve descripción de la función
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - A
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - A
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    arr = []
    arr2 = []
    arr3 = []
    dias_laborales = []
    dias_laborales2 = []
    numero = 0
    try:

        _S3 = """
          SELECT 
            pid.identifier
            ,pst.docnumber
            ,pst.filescanbase64
            ,pst.StatusStartDate 
          FROM PersonStatus pst 
            OUTER LEFT JOIN PersonIdentifier pid 
              ON pst.personid = pid.personId
              AND pid.RefPersonIdentificationSystemId  IN (
                SELECT RefPersonIdentificationSystemId 
                FROM RefPersonIdentificationSystem
                WHERE RefPersonIdentificationSystem.description IN ('ROL UNICO NACIONAL')
              )
            JOIN RefPersonStatusType rpst
              ON pst.RefPersonStatusTypeId = rpst.RefPersonStatusTypeId
              AND rpst.RefPersonStatusTypeId IN (
                SELECT RefPersonStatusTypeId
                FROM RefPersonStatusType
                WHERE RefPersonStatusType.description IN ('Excedente con derecho a subvención')
              )
            """

        now = datetime.now()
        _q1 = conn.execute(_S3)
        if(_q1.returns_rows == 0):
            logger.error(f"No hay informacion de estudiantes excedentes")
            logger.info(f"S/Datos")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True

        _q1 = _q1.fetchall()
        XX = 0
        
        if(len(_q1) == 0):
            logger.error(f"No hay informacion de estudiantes excedentes")
            logger.info(f"S/Datos")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
        else:
            for q1 in _q1:
                rut = str(q1[0])
                filescanbase64 = q1[2]
                docnumber = q1[1]
                dateF = q1[3]

                if ((filescanbase64 is None) or (docnumber is None) or (dateF is None)):
                    arr.append(rut)

            if(len(arr) != 0):
                logger.error(
                    f"Los siguientes alumnos no tienen Rex de aprobacion : {str(arr)} ")
                logger.error(f"Rechazado")
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False

            logger.info(f"Aprobado")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
