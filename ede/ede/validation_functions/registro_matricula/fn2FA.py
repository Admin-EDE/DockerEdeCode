from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


## WebClass INICIO ##
  ## Inicio fn2FA WC ##
def fn2FA(conn, return_dict):
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
    _r = False
    results = []
    try:
        results = conn.execute("""
        select count(distinct PersonId)-(select count(distinct PersonId) from OrganizationPersonRole
        where RoleId=6
        and ExitDate is not null)
        from OrganizationPersonRole
        where  EntryDate is not null and RoleId=6  ;
        """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {results} -> {str(e)}")

    resultsTwo = []
    try:
        resultsTwo = conn.execute("""
        SELECT count(distinct K12StudentEnrollment.OrganizationPersonRoleId)
        from K12StudentEnrollment
        where RefEnrollmentStatusId is not null
        AND FirstEntryDateIntoUSSchool IS NOT NULL;
        """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {resultsTwo} -> {str(e)}")

    try:
      if( len(results) > 0 and len(resultsTwo) > 0 ):
        lista = list(set([m[0] for m in results if m[0] is not None]))
        listaDos = list(set([m[0] for m in resultsTwo if m[0] is not None]))
        if lista == listaDos:
          logger.info(f"La cantidad de matriculados corresponder con los alumnos inscritos")
          logger.info(f"Aprobado")
          _r = True
        else :
          logger.error(f'La cantidad de alumnos matriculados no cocincide con los inscritos')
          logger.error(f'Rechazado')
      else:
        logger.info(f"S/Datos")
        logger.info(f'No hay registros de matriculas')
    except Exception as e:
      logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
      logger.error(f"Rechazado")
    finally:
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...") 
      return _r
  ## Fin fn2FA WC ##
