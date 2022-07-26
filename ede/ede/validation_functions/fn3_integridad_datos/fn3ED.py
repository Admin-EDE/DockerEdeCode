from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger

# VERIFICA QUE EL GRADO ESTE DENTRO DE LA LISTA PERMITIDA


def fn3ED(conn, return_dict):
    """
    Verifica que el grado está dentro de la lista permitida
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay grados registrados
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Los grados están dentro de la lista permitida
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = conn.execute(
            "SELECT RBD,nombreEstablecimiento,modalidad,jornada,nivel,rama,sector,especialidad,tipoCurso,codigoEnseñanza,grado,letraCurso FROM jerarquiasList;").fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    try:
        gradoList = check_utils.convertirArray2DToList(
            list(set([m[10] for m in rows if m[10] is not None])))
        if(len(gradoList) > 0):
            _err = set(
                [e for e in gradoList if not check_utils.validaGrado(e)])
            _r = False if len(_err) > 0 else True
            _t = f"VERIFICA QUE EL GRADO ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}"
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
