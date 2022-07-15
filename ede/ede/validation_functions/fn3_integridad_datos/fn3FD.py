from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede._logger import logger
### INICIO fn3FD ###
def fn3FD(conn, return_dict):
    """
    Integridad: Verifica que la cantidad de teléfonos corresponda con los tipos de teléfonos ingresados
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y "S/Datos" a través de logger si no encuentra información
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Verifica que cada teléfini tenga su asignación de tipo
            - Verifica que las comparaciones realizadas se cumplan.
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """       
    _r = False
    rows = []
    try:
      rows = conn.execute("""
        SELECT count(TelephoneNumber), count(RefPersonTelephoneNumberTypeId)
        from PersonTelephone
        UNION ALL
        SELECT count(TelephoneNumber), count(RefInstitutionTelephoneTypeId)
        FROM OrganizationTelephone
    """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {rows} -> {str(e)}")
    
    try:
      if(len(rows) > 0):
        personPhone= rows[0][0]
        personPhoneType = rows[0][1]
        orgPhone = rows[1][0]
        orgPhoneType = rows[1][1]
        _r1 = personPhone == personPhoneType and personPhone != 0
        _r2 = orgPhone == orgPhoneType and orgPhone != 0
        _r = _r1 and _r2
        _t = f"VERIFICA que la cantidad de teléfonos corresponda con los tipos de teléfonos registrados: {_r}. "
        logger.info(_t) if _r else logger.error(_t)
        _t = f"personPhone {personPhone}, personPhoneType: {personPhoneType}, orgPhone: {orgPhone}, orgPhoneType: {orgPhoneType}"
        logger.info(_t) if _r else logger.error(_t)       
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
        _r = True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
    finally:
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")
      return _r
  ### FIN fn3FD ###