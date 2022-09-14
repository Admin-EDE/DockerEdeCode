from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3D2(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    El campo de Asistencia no se encuentra vacío.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - Obtener el número de eventos de asistencia y éste es 0 (no hay)
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Obtener un número de eventos de asistencia mayor a 0 
            y todos tienen el campo si es clase virtual o no como no nulo.
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    _ExistData = []
    try:
        _ExistData = ejecutar_sql(conn, """--sql
          SELECT count(RoleAttendanceEventId) FROM RoleAttendanceEvent
      """)
    except Exception as e:
        logger.info(f"Resultado: {_ExistData} -> {str(e)}")

    if(_ExistData[0][0] == 0):
        logger.info(f"S/Datos")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    virtualIndicator = []
    try:
        virtualIndicator = ejecutar_sql(conn, """--sql
        /*
        * Selecciona los eventos que no tienen el campo VirtualIndicator
        * correctamente asignado
        */
        SELECT RoleAttendanceEventId 
        FROM RoleAttendanceEvent
        WHERE VirtualIndicator NOT IN (0,1);
      """)
    except Exception as e:
        logger.info(f"Resultado: {virtualIndicator} -> {str(e)}")

    if(len(virtualIndicator) == 0):
        logger.info(f"Aprobado")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    try:
        logger.info(f"virtualIndicator mal asignados: {len(virtualIndicator)}")
        if(len(virtualIndicator) > 0):
            data1 = list(
                set([m[0] for m in virtualIndicator if m[0] is not None]))
            _err1 = f"Los siguientes registros de la tabla RoleAttendanceEvent no tienen definidos el indicador de virtualidad del estudiante: {data1}"
            logger.error(_err1)
            logger.error(f"Rechazado")
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la verificación asignaturas sin curso asociado: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
