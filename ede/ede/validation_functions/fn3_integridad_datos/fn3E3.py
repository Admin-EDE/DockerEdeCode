from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger


# VERIFICA QUE EL FORMATO DEL RBD CORRESPONDA
def fn3E3(conn, return_dict):
    """
    Verifica que el código identificador del establecimiento RBD sea correcto de acuerdo al formato
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay establecimientos registrados
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Los establecimientos cumplen el formato del RBD
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = conn.execute("""--sql
        SELECT Identifier 
        FROM k12schoolList 
          INNER JOIN organizationList 
            USING(OrganizationId);""").fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    try:
        logger.info(f"len(establecimientos): {len(rows)}")
        formatoRBD = check_utils.convertirArray2DToList(
            list(set([m[0] for m in rows if m[0] is not None])))
        if(len(formatoRBD) > 0):
            _err = set(
                [e for e in formatoRBD if not check_utils.validaFormatoRBD(e)])
            _r = False if len(_err) > 0 else True
            _t = f"VERIFICACION DEL FORMATO DEL RBD DEL ESTABLECIMIENTO: {_r}. {_err}"
            logger.info(_t) if _r else logger.error(_t)
            logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
        else:
            logger.info("S/Datos")
    except Exception as e:
        logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
