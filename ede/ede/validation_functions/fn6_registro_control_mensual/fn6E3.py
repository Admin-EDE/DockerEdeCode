from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn6E3(conn, return_dict):
    """
    REGISTRO CONTROL MENSUAL DE ASISTENCIA O CONTROL DE SUBVENCIONES
    6.2 Contenido mínimo, letra c.2
    En los casos de suspensión de clases, existe ingresado en el sistema la aprobación del calendario de recuperación de la secretaría ministerial.
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
    _flag1 = 0
    _flag2 = 0
    try:
        # OBTENGO LAS FECHAS CON SUSPENSION DE CLASES
        _s1 = """SELECT rexNumber, rexDate, fileScanBase64
                FROM OrganizationCalendarEvent
                WHERE indicadorSinClases = 1;"""

        _s2 = """SELECT numeroREX, fechaREX, fileScanBase64
              FROM OrganizationCalendarSession
              WHERE claseRecuperadaId != NULL;"""

        _q1 = ejecutar_sql(conn, _s1)
        if(len(_q1) != 0):
            for q1 in _q1:
                _rexNumber = q1[0]
                _rexDate = q1[1]
                _fsb = q1[2]
                if(_rexNumber is None):
                    _msg1 = f"No hay información de resolución ministerial para la suspensión de clases (numero de resolución)"
                    _flag1 = 1
                if(_rexDate is None):
                    _msg1 = f"No hay información de resolución ministerial para la suspensión de clases (fecha de resolución)"
                    _flag1 = 1
                if(_fsb is None):
                    _msg1 = f"No hay información de resolución ministerial para la suspensión de clases (documento digitalizado)"
                    _flag1 = 1

        else:
            logger.info(
                f"No hay información en el establecimiento de eventos que impliquen suspensión de clases.")
            logger.info(f"Aprobado")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True

        _q2 = ejecutar_sql(conn, _s2)
        if(len(_q2) != 0):
            for q2 in _q2:
                _rxn = q2[0]
                _rxd = q2[1]
                _rxfbs = q2[2]
                if(_rxn is None):
                    _msg2 = f"No hay información de resolución ministerial para recuperación de clases (numero de resolución)"
                    _flag2 = 1
                if(_rxd is None):
                    _msg2 = f"No hay información de resolución ministerial para  recuperación de clases (fecha de resolución)"
                    _flag2 = 1
                if(_rxfbs is None):
                    _msg2 = f"No hay información de resolución ministerial para  recuperación de clases (documento digitalizado)"
                    _flag1 = 1

        else:
            logger.info(
                f"No hay información en el establecimiento de clases recuperadas.")
            logger.info(f"Aprobado")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True

        if(_flag1 == 1):
            logger.error(_msg1)
            logger.error(f"S/Datos") #Rechazado, se cambia por ser el calendario opcional
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False
        elif(_flag2 == 1):
            logger.error(_msg2)
            logger.error(f"S/Datos") #Rechazado, se cambia por ser el calendario opcional
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False
        else:
            logger.info(f"Aprobado")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True

    except Exception as e:
        logger.error(f"NO se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"S/Datos") #Rechazado, se cambia por ser el calendario opcional
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
### fin fn6E3 ###
