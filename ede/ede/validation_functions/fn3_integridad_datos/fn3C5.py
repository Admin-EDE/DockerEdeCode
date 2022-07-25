from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger

# VERIFICA DATOS DE LAS ORGANIZACIONES


def fn3C5(conn, return_dict):
    """
    Verificador de identidad (OTP)
    Verifica que el campo cumpla con la siguiente expresión regular: ^[0-9]{6}+([-]{1}[0-9kK]{1})?$
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No existen OTP en la base de datos
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Todos los OTP (verificadores de identidad) cumplen el formato
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = conn.execute(
            "SELECT digitalRandomKey,firmaRatificador FROM RoleAttendanceEvent where digitalRandomKey not null;").fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    try:
        logger.info(f"len(digitalRandomKey): {len(rows)}")
        if(len(rows) > 0):
            # Valida los números de clave aleatoria de los docentes
            data = list(set([m[0] for m in rows if m[0] is not None])) + \
                list(set([m[1] for m in rows if m[1] is not None]))
            _err, _r = check_utils.imprimeErrores(
                data, check_utils.validaFormatoClaveAleatoria, "VERIFICA FORMATO Clave Aleatoria Docente")
            logger.info(f"Aprobado") if _r else logger.error(_err)
        else:
            logger.info("La BD no contiene clave aleatoria de los docentes")
            logger.info("S/Datos")
    except Exception as e:
        logger.error(
            f"No se pudieron validar los verificadores de indentidad: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
