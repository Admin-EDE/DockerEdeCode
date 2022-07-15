from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger

def fn2DA(conn, return_dict):
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
    try:
        _query = conn.execute("""
        SELECT DISTINCT PS.PersonId
        FROM OrganizationPersonRole OPR
                join Person P on OPR.PersonId = P.PersonId
                join PersonStatus PS on P.PersonId = PS.PersonId
        WHERE OPR.RoleId = 6
          and PS.RefPersonStatusTypeId = 27
        """).fetchall()
        if(len(_query)>0):
          _personStatusFile = conn.execute("""
          SELECT fileScanBase64
          FROM PersonStatus
          WHERE PersonId in (
              SELECT DISTINCT PS.PersonId
              FROM OrganizationPersonRole OPR
                      join Person P on OPR.PersonId = P.PersonId
                      join PersonStatus PS on P.PersonId = PS.PersonId
              WHERE OPR.RoleId = 6
                and PS.RefPersonStatusTypeId = 27
          )
          AND fileScanBase64 is not null
          and RefPersonStatusTypeId = 27
          """).fetchall()
          if (len(_query) == len(_personStatusFile)):
              _file = conn.execute("""
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
                          where OPR.RoleId = 6
                            and PS.RefPersonStatusTypeId = 27
                      )
                      and fileScanBase64 is not null
                      and RefPersonStatusTypeId = 27
                  );
              """).fetchall()
              if(len(_file) == len(_query)):
                logger.info(f'Todos los alumnos nuevos con matricula definitiva poseen documento')
                logger.info(f'Aprobado')
                return_dict[getframeinfo(currentframe()).function] = True
                return True
              else:
                logger.error(f'los alumnos nuevos con matricula definitiva no poseen documento')
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                return False
          else:
            logger.error(f'Los alumnos nuevos con matricula definitiva no poseen documento')
            logger.error(f'Rechazado')
            return_dict[getframeinfo(currentframe()).function] = False
            return False
        else:
            logger.error(f'No existen alumnos nuevos con matricula definitiva')
            logger.error(f'S/Datos')
            return_dict[getframeinfo(currentframe()).function] = False
            return False
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        return False
  ## Fin fn2DA WC ##