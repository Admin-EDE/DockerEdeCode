from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede._logger import logger


def fn3FC(conn, return_dict):
    """
    Integridad: Verifica que la cantidad de emails corresponda con los tipos de emails ingresados
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y "S/Datos" a través de logger si no encuentra información
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Verifica que cada e-mail tenga su asignación de tipo
            - Verifica que las comparaciones realizadas se cumplan.
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = conn.execute("""--sql
        SELECT count(emailAddress), count(RefEmailTypeId)
        from PersonEmailAddress
        UNION ALL
        SELECT count(ElectronicMailAddress), count(RefEmailTypeId)
        FROM OrganizationEmail
    """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    try:
        if(len(rows) > 0):
            personEmails = rows[0][0]
            personEmailsType = rows[0][1]
            orgEmails = rows[1][0]
            orgEmailsType = rows[1][1]
            _r1 = personEmails == personEmailsType and personEmails != 0
            _r2 = orgEmails == orgEmailsType and orgEmails != 0
            _r = _r1 and _r2
            _t = f"VERIFICA que la cantidad de e-mails corresponda con los tipos de e-mails registrados: {_r}. "
            logger.info(_t) if _r else logger.error(_t)
            _t = f"personEmails: {personEmails}, personEmailsType: {personEmailsType}, orgEmails: {orgEmails}, orgEmailsType: {orgEmailsType}"
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
