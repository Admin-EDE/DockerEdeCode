from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.validation_functions.check_utils as check_utils
from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3F6(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    La lista de teléfonos cumple con el formato.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y "S/Datos" a través de logger si no encuentra información
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Verificar que teléfonos ingresados cumplan con el formato E164
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = ejecutar_sql(conn, """--sql
        SELECT TelephoneNumber
        from PersonTelephone
        UNION ALL
        SELECT TelephoneNumber
        FROM OrganizationTelephone
    """)
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    try:
        datos = check_utils.convertirArray2DToList(list(
            [m[0] for m in rows if m[0] is not None]))  # Valida lista de rut ingresados a la BD
        if(len(rows) > 0 and len(datos) > 0):
            _err = set(
                [e for e in datos if not check_utils.validaFormatoE164Telefono(e)])
            _r = False if len(_err) > 0 else True
            _t = f"VERIFICACION DEL FORMATO DE LOS TELEFONOS DE LAS PERSONAS: {_r}. {_err}"
            logger.info(_t) if _r else logger.error(_t)
            logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
        else:
            logger.info("S/Datos")
        _r = True
    except Exception as e:
        logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
