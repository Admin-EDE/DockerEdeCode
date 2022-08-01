from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede._logger import logger


def fn2BA(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.5 De los estudiantes excedentes
    Validar que exista cargada en el sistema la resolución que autoriza al estudiante.
    --------------------------------------------------
    NOTA: 
    - Una resolución es un documento público que emite el Ministerio de Educación en 
    ciertos casos. Cada resolución tiene un número y una fecha de total tramitación. 
    Para este caso se debe validar que exista el documento cargado en el sistema y 
    sus datos cargados en la tabla.
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
        _query = conn.execute("""--sql
        --Comprobar si hay estudiantes con status 25, 24, 31
        SELECT DISTINCT P.PersonId
        from OrganizationPersonRole OPR
                join Person P on OPR.PersonId = P.PersonId
                join PersonStatus PS on P.PersonId = PS.PersonId
        where OPR.RoleId = 6 --Estudiante
          and PS.RefPersonStatusTypeId IN (25, 24, 31); --Intercambio, Excedente sin derecho a subvención, Excedente con derecho a subvención
        """)
        if not _query.returns_rows:
          logger.info(
                f"No existen estudiantes excedentes en el establecimiento")
          logger.info(f"S/Datos")
          return_dict[getframeinfo(currentframe()).function] = True
          logger.info(f"{current_process().name} finalizando...")
          return True
        _query = _query.fetchall()
        if (len(_query) > 0):
            _queryExcedentes = conn.execute("""--sql
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
          """).fetchall()
            if (len(_queryExcedentes) == len(_query)):
                _file = conn.execute("""--sql
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
                                      and PS.RefPersonStatusTypeId IN (25, 24, 31); --Intercambio, Excedente sin derecho a subvención, Excedente con derecho a subvención
                                )
                                  and fileScanBase64 is not null
                                  and RefPersonStatusTypeId IN (25, 24, 31); --Intercambio, Excedente sin derecho a subvención, Excedente con derecho a subvención
            )
            """).fetchall()
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
