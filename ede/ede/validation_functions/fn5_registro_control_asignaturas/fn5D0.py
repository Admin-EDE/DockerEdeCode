from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn5D0(conn, return_dict):
    """
    REGISTRO DE CONTROL DE ASIGNATURA
    6.2 Contenido mínimo, letra b.3
    No existen asistencias de Class/section duplicadas.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No existe asistencia por clase
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Existe asistencia por clase y no hay duplicados
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _oPR = ejecutar_sql(conn, """--sql
            SELECT DISTINCT count(RAE.Date), OPR.PersonId, RAE.Date, RAE.digitalRandomKey,RAE.VirtualIndicator
            FROM OrganizationPersonRole OPR
                    JOIN RoleAttendanceEvent RAE ON OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
            WHERE OPR.RoleId in(4,5)
            AND RAE.RefAttendanceEventTypeId = 2
            group by OPR.PersonId, OPR.OrganizationId, RAE.Date, RAE.digitalRandomKey, RAE.VirtualIndicator;
            """
            )
        if(len(_oPR)>0):
            _count = (list([m[0] for m in _oPR if m[0] is not None]))
            _contador = 0
            for x in _count:
                if(x > 1):
                    _contador += 1
            if(_contador > 0):
                logger.error('Duplicados')
                logger.error('Rechazado') #Rechazado, se cambia por ser el Registro de control de asignaturas opcional en Parvularia
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
            else:
                logger.info('No hay duplicados')
                logger.info('Aprobado')
                return_dict[getframeinfo(currentframe()).function] = True
                logger.info(f"{current_process().name} finalizando...")
                return True
        else:
            logger.error(f'No existen Firmas')
            logger.error(f'S/Datos')
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado") #Rechazado, se cambia por ser el Registro de control de asignaturas opcional en Parvularia
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False