from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger


def fn3F8(conn, return_dict):
    """
    Integridad: Verifica que el número de matrícula cumpla con el formato
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Verifica que el campo cumpla con la siguiente expresión regular: ^\d{0,4}$
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = conn.execute("""--sql
        SELECT identifier 
        FROM PersonIdentifier pi
        JOIN RefPersonIdentificationSystem rfi 
          ON  pi.RefPersonIdentificationSystemId=rfi.RefPersonIdentificationSystemId
          AND rfi.code IN ('SchoolNumber')
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    try:
        datos = check_utils.convertirArray2DToList(list(
            [m[0] for m in rows if m[0] is not None]))  # Valida lista de rut ingresados a la BD
        if(len(rows) > 0 and len(datos) > 0):
            _err = set(
                [e for e in datos if not check_utils.validaFormatoNumero(e)])
            _r = False if len(_err) > 0 else True
            _t = f"VERIFICACION DEL NUMERO DE MATRICULA DE LAS PERSONAS: {_r}. {_err}"
            logger.info(_t) if _r else logger.error(_t)
            logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
        else:
            logger.info("S/Datos")
    except Exception as e:
        logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
