from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn28A(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.8 De los estudiantes migrantes
    Los estudiantes migrantes tienen su IPE y documento de convalidación de estudios.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay estudiantes migrantes
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Hay estudiantes migrantes y tienen sus documentos subidos
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _query = ejecutar_sql(conn, """--sql
        SELECT DISTINCT P.PersonId
        FROM OrganizationPersonRole OPR
                JOIN Person P on OPR.PersonId = P.PersonId
                JOIN PersonIdentifier PI on P.PersonId = PI.PersonId
        where PI.RefPersonIdentificationSystemId = 52
          and OPR.RoleId = 6
          and PI.Identifier is not null;
        """)
        if(len(_query)>0):
          _personStatus = ejecutar_sql(conn, """--sql
          SELECT fileScanBase64
          FROM PersonStatus
          WHERE PersonId in (SELECT DISTINCT P.PersonId
                            FROM OrganizationPersonRole OPR
                                      join Person P on OPR.PersonId = P.PersonId
                                      join PersonIdentifier PI on P.PersonId = PI.PersonId
                            WHERE PI.RefPersonIdentificationSystemId = 52
                              and OPR.RoleId = 6
                              and PI.Identifier is not null)
            and RefPersonStatusTypeId = 34
            and fileScanBase64 is not null;
          """)
          if(len(_personStatus) == len(_query)):
            _file = ejecutar_sql(conn, """--sql
            SELECT
                  documentId
            FROM Document
            WHERE fileScanBase64 IS NOT NULL
              AND fileScanBase64 <> ''
              AND documentId in (
                select fileScanBase64
                from PersonStatus
                where PersonId in (select DISTINCT P.PersonId
                                  from OrganizationPersonRole OPR
                                            join Person P on OPR.PersonId = P.PersonId
                                            join PersonIdentifier PI on P.PersonId = PI.PersonId
                                  where PI.RefPersonIdentificationSystemId = 52
                                    and OPR.RoleId = 6
                                    and PI.Identifier is not null)
                  and RefPersonStatusTypeId = 34
                  and fileScanBase64 is not null);
            """)
            if(len(_query) == len(_file)):
              logger.info(f'Todos los alumnos extranjeros poseen documento de convalidacion de estudios')
              logger.info(f'Aprobado')
              return_dict[getframeinfo(currentframe()).function] = True
              logger.info(f"{current_process().name} finalizando...")
              return True
            else:
              logger.error(f'Existen documentos de convalidacion de ramos incompletos')
              logger.error(f'Rechazado')
              return_dict[getframeinfo(currentframe()).function] = False
              logger.info(f"{current_process().name} finalizando...")
              return False
          else:
            logger.error(f'No todos los alumnos extranjeros no poseen documento de convalidacion de estudios')
            logger.error(f'Rechazado')
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False
        else:
            logger.info(f"S/Datos")
            logger.info(f"No existen estudiantes migrantes registrados en el establecimiento")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False