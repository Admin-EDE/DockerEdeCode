from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn2BA(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.5 De los estudiantes excedentes
    Existe cargada en el sistema la resolución que autoriza al estudiante excedente.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No existen alumnos excedentes en el establecimiento
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Todos los alumnos excedentes cuentan con su documento correspondiente
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _query = ejecutar_sql(conn, """--sql
        --Comprobar si hay estudiantes con status 25, 24, 31
        SELECT DISTINCT P.PersonId, PS.RefPersonStatusTypeId
        from OrganizationPersonRole OPR
                join Person P on OPR.PersonId = P.PersonId
                join PersonStatus PS on P.PersonId = PS.PersonId
        where OPR.RoleId = 6 --Estudiante
          and PS.RefPersonStatusTypeId IN (25, 24, 31); --Intercambio, Excedente sin derecho a subvención, Excedente con derecho a subvención
        """)
        if (len(_query) > 0):
            _queryExcedentes = ejecutar_sql(conn, """--sql
            --Que aquellos estudiantes tengan su archivo subido
          SELECT fileScanBase64
          from PersonStatus
          where PersonId in (
              SELECT DISTINCT P.PersonId
              FROM OrganizationPersonRole OPR
                      join Person P on OPR.PersonId = P.PersonId
                      join PersonStatus PS on P.PersonId = PS.PersonId
              where OPR.RoleId = 6 --Estudiante
                and PS.RefPersonStatusTypeId IN (25, 24, 31)) --Intercambio, Excedente sin derecho a subvención, Excedente con derecho a subvención
            and fileScanBase64 is not null
            and RefPersonStatusTypeId IN (25, 24, 31); --Intercambio, Excedente sin derecho a subvención, Excedente con derecho a subvención
          """)
            if (len(_queryExcedentes) == len(_query)):
                _file = ejecutar_sql(conn, """--sql
                --Que aquellos estudiantes tengan su archivo subido, no nulo, y con documentid
            SELECT documentId
            FROM Document
            WHERE fileScanBase64 IS NOT NULL
              AND fileScanBase64 <> ''
              AND documentId in (select fileScanBase64
                                from PersonStatus
                                where PersonId in (
                                    select DISTINCT P.PersonId
                                    from OrganizationPersonRole OPR
                                              join Person P on OPR.PersonId = P.PersonId
                                              join PersonStatus PS on P.PersonId = PS.PersonId
                                    where OPR.RoleId = 6 --Estudiante
                                      and PS.RefPersonStatusTypeId IN (25, 24, 31) --Intercambio, Excedente sin derecho a subvención, Excedente con derecho a subvención
                                )
                                  and fileScanBase64 is not null
                                  and RefPersonStatusTypeId IN (25, 24, 31) --Intercambio, Excedente sin derecho a subvención, Excedente con derecho a subvención
            )
            """)
                if(len(_file) == len(_query)):
                    logger.info(
                        f'Todos los alumnos excedentes cuentan con su documento correspondiente')
                    logger.info(f'Aprobado')
                    return_dict[getframeinfo(currentframe()).function] = True
                    logger.info(f"{current_process().name} finalizando...")
                    return True
                else:
                    logger.error(
                        f'Los alumnos excedentes no cuentan con su documento correspondiente')
                    logger.error(f'Rechazado')
                    return_dict[getframeinfo(currentframe()).function] = False
                    logger.info(f"{current_process().name} finalizando...")
                    return False
            else:
                logger.error(
                    f'Los alumnos excedentes no cuentan con su documento correspondiente')
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
        else:
            logger.info(f'S/Datos')
            logger.info(f'No existen alumnos excedentes en el establecimiento')
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(f'NO se pudo ejecutar la verificación en la lista')
        logger.error(f'Rechazado')
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
