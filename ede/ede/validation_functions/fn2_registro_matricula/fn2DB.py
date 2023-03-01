from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn2DB(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.3 De las altas en el registro de matrícula.
    Todos los estudiantes matriculados bajo el decreto 152 artículo 60 tienen su documento escaneado.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay alumnos matriculados bajo el decreto 152 artículo 60
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Todos los estudiantes matriculados bajo el decreto 152 artículo 60 tienen su documento escaneado
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _query = ejecutar_sql(conn, """--sql
        SELECT DISTINCT P.PersonId
        FROM OrganizationPersonRole OPR
                join Person P on OPR.PersonId = P.PersonId
                join PersonStatus PS on P.PersonId = PS.PersonId
        where OPR.RoleId = 6
          and PS.RefPersonStatusTypeId = 33;
        """)
        if(len(_query)>0):
          _queryType = ejecutar_sql(conn, """--sql
          SELECT PS.fileScanBase64
          FROM PersonStatus PS
          WHERE PS.PersonId in (select DISTINCT P.PersonId
                                from OrganizationPersonRole OPR
                                        join Person P on OPR.PersonId = P.PersonId
                                        join PersonStatus PS on P.PersonId = PS.PersonId
                                where OPR.RoleId = 6
                                  and PS.RefPersonStatusTypeId = 33)
            and PS.fileScanBase64 is not null
            and PS.RefPersonStatusTypeId = 33
          """)
          if(len(_queryType) == len(_query)):
            _file = ejecutar_sql(conn, """--sql
            SELECT documentId
            FROM Document
            WHERE fileScanBase64 IS NOT NULL
              AND fileScanBase64 <> ''
              AND documentId in (SELECT PS.fileScanBase64
                                FROM PersonStatus PS
                                WHERE PS.PersonId in (select DISTINCT P.PersonId
                                                      from OrganizationPersonRole OPR
                                                                join Person P on OPR.PersonId = P.PersonId
                                                                join PersonStatus PS on P.PersonId = PS.PersonId
                                                      where OPR.RoleId = 6
                                                        and PS.RefPersonStatusTypeId = 33)
                                  and PS.fileScanBase64 is not null
                                  and PS.RefPersonStatusTypeId = 33);
            """)
            if(len(_file) == len(_query)):
              logger.info(f'Todos los alumnos matriculados bajo el decreto 152 poseen su documento correspondiente')
              logger.info(f'Aprobado')
              return_dict[getframeinfo(currentframe()).function] = True
              logger.info(f"{current_process().name} finalizando...")
              return True
            else:
              logger.error(f'Los alumnos matriculados bajo el decreto 152 no poseen su documento correspondiente')
              logger.error(f'Rechazado')
              return_dict[getframeinfo(currentframe()).function] = False
              logger.info(f"{current_process().name} finalizando...")
              return False
          else:
            logger.error(f'No existe documento para los alumnos matriculados bajo el decreto 152')
            logger.error(f'Rechazado')
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False
        else:
            logger.info(f"S/Datos")
            logger.info(f"No existen alumnos matriculados bajo el decreto 152, artículo 60")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False