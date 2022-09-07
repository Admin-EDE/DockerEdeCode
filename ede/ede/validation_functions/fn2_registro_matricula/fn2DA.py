from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn2DA(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.3 De las altas en el registro de matrícula.
    Validar que los alumnos nuevos tengan el certificado de promoción del estudiante
    y el certificado de traslado o baja de matrícula del establecimiento de origen.
    --------------------------------------------------
    Los alumnos nuevos se pueden identificar a través de 
    PersonStatus.RefPersonStatusTypeId == 27 (Estudiante nuevo con matrícula definitiva)
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No existen alumnos nuevos con matricula definitiva
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Todos los alumnos nuevos con matricula definitiva poseen documento
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _query = ejecutar_sql(conn, """--sql
        --Busca alumnos nuevos con matrícula definitiva
        SELECT DISTINCT PS.PersonId
        FROM OrganizationPersonRole OPR
                join Person P on OPR.PersonId = P.PersonId
                join PersonStatus PS on P.PersonId = PS.PersonId
        WHERE OPR.RoleId = 6
          and PS.RefPersonStatusTypeId = 27
        """)
        if(len(_query) > 0):
            _personStatusFile = ejecutar_sql(conn, """--sql
            --Busca el documento de cada alumno nuevo con matricula definitiva
          SELECT fileScanBase64
          FROM PersonStatus
          WHERE PersonId in (
              SELECT DISTINCT PS.PersonId
              FROM OrganizationPersonRole OPR
                      join Person P on OPR.PersonId = P.PersonId
                      join PersonStatus PS on P.PersonId = PS.PersonId
              WHERE OPR.RoleId = 6 --Estudiante
                and PS.RefPersonStatusTypeId = 27 --Estudiante con matrícula definitiva
          )
          AND fileScanBase64 is not null
          and RefPersonStatusTypeId = 27 --Estudiante con matrícula definitiva
          """)
            if (len(_query) == len(_personStatusFile)):
                _file = ejecutar_sql(conn, """--sql
                --Busca que el documento sea no nulo y  tenga su documentid
              SELECT documentId
              FROM Document
              WHERE fileScanBase64 IS NOT NULL
                  AND fileScanBase64 <> ''
                  AND documentId IN (
                      select fileScanBase64
                      from PersonStatus
                      where PersonId in (
                          select DISTINCT PS.PersonId
                          from OrganizationPersonRole OPR
                                  join Person P on OPR.PersonId = P.PersonId
                                  join PersonStatus PS on P.PersonId = PS.PersonId
                          where OPR.RoleId = 6 --Estudiante
                            and PS.RefPersonStatusTypeId = 27 --Estudiante con matrícula definitiva
                      )
                      and fileScanBase64 is not null
                      and RefPersonStatusTypeId = 27 --Estudiante con matrícula definitiva
                  );
              """)
                if(len(_file) == len(_query)):
                    logger.info(
                        f'Todos los alumnos nuevos con matricula definitiva poseen documento')
                    logger.info(f'Aprobado')
                    return_dict[getframeinfo(currentframe()).function] = True
                    logger.info(f"{current_process().name} finalizando...")
                    return True
                else:
                    logger.error(
                        f'los alumnos nuevos con matricula definitiva no poseen documento')
                    logger.error(f'Rechazado')
                    return_dict[getframeinfo(currentframe()).function] = False
                    logger.info(f"{current_process().name} finalizando...")
                    return False
            else:
                logger.error(
                    f'Los alumnos nuevos con matricula definitiva no poseen documento')
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
        else:
            logger.error(f'No existen alumnos nuevos con matricula definitiva')
            logger.error(f'S/Datos')
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
