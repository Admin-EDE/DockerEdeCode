from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn2CA(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.4 De las bajas en el registro de matrícula
    Existe la fecha, motivo y declaración jurada del requirente o su verificador de identidad cargado en el sistema.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay alumnos retirados
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Todos los alumnos retirados del establecimiento cuentan con su fecha, motivo y declaración jurada
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _query = ejecutar_sql(conn, """--sql
          SELECT DISTINCT p.PersonId
          FROM OrganizationPersonRole OPR
          OUTER LEFT JOIN Person P on OPR.PersonId = P.PersonId
          OUTER LEFT JOIN PersonStatus PS on P.PersonId = PS.PersonId
          OUTER LEFT JOIN RefPersonStatusType on RefPersonStatusType.refPersonStatusTypeId = PS.refPersonStatusTypeId
          OUTER LEFT JOIN Document USING(fileScanBase64)
          WHERE RefPersonStatusType.Description IN ('Estudiante retirado definitivamente')
        """)
        if(len(_query)>0):
            _queryOK = ejecutar_sql(conn, """--sql
                SELECT DISTINCT p.PersonId
                FROM OrganizationPersonRole OPR
                OUTER LEFT JOIN Person P on OPR.PersonId = P.PersonId
                OUTER LEFT JOIN PersonStatus PS on P.PersonId = PS.PersonId
                OUTER LEFT JOIN RefPersonStatusType on RefPersonStatusType.refPersonStatusTypeId = PS.refPersonStatusTypeId
                OUTER LEFT JOIN Document USING(fileScanBase64)
                WHERE 
                  RefPersonStatusType.Description IN ('Estudiante retirado definitivamente')
                  and p.personId NOT IN (
                    SELECT DISTINCT p.PersonId
                    FROM OrganizationPersonRole OPR
                    JOIN Person P on OPR.PersonId = P.PersonId
                    JOIN PersonStatus PS on P.PersonId = PS.PersonId
                    JOIN RefPersonStatusType on RefPersonStatusType.refPersonStatusTypeId = PS.refPersonStatusTypeId
                    JOIN Document USING(fileScanBase64)
                    WHERE
                      OPR.RoleId = 6
                      and p.RecordEndDateTime IS NULL and PS.RecordEndDateTime IS NULL and OPR.RecordEndDateTime IS NULL
                      and PS.StatusStartDate IS NOT NULL and PS.StatusEndDate IS NOT NULL and PS.Description IS NOT NULL
                      and RefPersonStatusType.Description IN ('Estudiante retirado definitivamente')	
                      and documentId IS NOT NULL and length(Document.fileScanBase64) > 0
                  )
            """)
            _data =  list(set([m[0] for m in _queryOK if m[0] is not None]))
            if(len(_query)==len(_data)):
              logger.info(f'Todos los alumnos retirados del establecimiento cuentan con su fecha, motivo y declaración jurada.')
              logger.info(f'Aprobado')
              return_dict[getframeinfo(currentframe()).function] = True
              logger.info(f"{current_process().name} finalizando...")
              return True
            else:
              logger.error(f'Los siguientes alumnos retirados del establecimiento no cuentan su fecha, motivo o declaración jurada: {_data}')
              logger.error(f'Rechazado')
              return_dict[getframeinfo(currentframe()).function] = False
              logger.info(f"{current_process().name} finalizando...")
              return False
        else:
            logger.info(f'S/Datos')
            logger.info(f'No existen registros de alumnos retirados del establecimiento')
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
