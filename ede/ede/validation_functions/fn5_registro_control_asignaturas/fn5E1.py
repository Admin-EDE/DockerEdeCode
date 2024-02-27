from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn5E1(conn, return_dict):
    """
    REGISTRO DE CONTROL DE ASIGNATURA
    6.2 Contenido mínimo, letra b.2
    Al final de la jornada existe el registro de alumnos matriculados en el curso y el total de la asistencia diaria.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay alumnos matriculados
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Existe registro de matriculados y hay asistencia diaria
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _query = ejecutar_sql(conn, """--sql
        SELECT OPR.OrganizationPersonRoleId,
            (SELECT count(OPR.PersonId)
                from OrganizationPersonRole OPR
                        join Organization O on OPR.OrganizationId = O.OrganizationId
                        join Course C on O.OrganizationId = C.OrganizationId
                where OPR.RoleId = 6
                and O.RefOrganizationTypeId = 21) as MatriculasTotales
        FROM OrganizationPersonRole OPR
                join Organization O on OPR.OrganizationId = O.OrganizationId
                join Course C on O.OrganizationId = C.OrganizationId
        WHERE OPR.RoleId = 6
        AND O.RefOrganizationTypeId = 21
        GROUP by OPR.OrganizationPersonRoleId;
        """)
        if(len(_query)>0):
            _alumnos = (list([m[0] for m in _query if m[0] is not None]))
            if not _alumnos :
                logger.error(f"Sin alumnos registrados")
                logger.error(f'Rechazado') #Rechazado, se cambia por ser el Registro de control de asignaturas opcional en Parvularia
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
            _matriculasTotales = (list([m[1] for m in _query if m[1] is not None]))
            if not _matriculasTotales :
                logger.error(f"Sin matriculas registradas")
                logger.error(f'Rechazado') #Rechazado, se cambia por ser el Registro de control de asignaturas opcional en Parvularia
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
            _totalAlumnos = int(len(_alumnos))
            if int(_matriculasTotales[0]) == _totalAlumnos:
                    _queryRegistroAsistencia = ejecutar_sql(conn, """--sql
                    SELECT DISTINCT RoleAttendanceEventId,
                                    Date,
                                    RefAttendanceEventTypeId
                    FROM RoleAttendanceEvent
                    WHERE RefAttendanceEventTypeId = 1 and Date is not null
                    GROUP by date;
                    """)
                    if(len(_queryRegistroAsistencia)>0):
                        logger.info(f'Matriculas registradas y asistencia diaria realizada')
                        logger.info(f'Aprobado')
                        return_dict[getframeinfo(currentframe()).function] = True
                        logger.info(f"{current_process().name} finalizando...")
                        return True
                    else:
                        logger.error(f'Asistencia diaria no realizada por el establecimiento')
                        logger.error(f'Rechazado') #Rechazado, se cambia por ser el Registro de control de asignaturas opcional en Parvularia
                        return_dict[getframeinfo(currentframe()).function] = False
                        logger.info(f"{current_process().name} finalizando...")
                        return False
            else:
                logger.error(f"Sin matriculas no coinciden con total de alumnos registrados")
                logger.error(f'Rechazado') #Rechazado, se cambia por ser el Registro de control de asignaturas opcional en Parvularia
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
        else:
            logger.error(f'S/Datos')
            logger.error(f'No existen alumnos matriculados en el establecimiento')
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado") #Rechazado, se cambia por ser el Registro de control de asignaturas opcional en Parvularia
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False