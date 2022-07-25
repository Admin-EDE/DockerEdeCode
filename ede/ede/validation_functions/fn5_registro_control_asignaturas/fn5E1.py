from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


def fn5E1(conn, return_dict):
    """ Breve descripción de la función
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
    try:
        _query = conn.execute("""
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
        """).fetchall()
        if(len(_query) > 0):
            _alumnos = (list([m[0] for m in _query if m[0] is not None]))
            if not _alumnos:
                logger.error(f"Sin alumnos registrados")
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                return False
            _matriculasTotales = (
                list([m[1] for m in _query if m[1] is not None]))
            if not _matriculasTotales:
                logger.error(f"Sin matriculas registradas")
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                return False
            _totalAlumnos = int(len(_alumnos))
            if int(_matriculasTotales[0]) == _totalAlumnos:
                _queryRegistroAsistencia = conn.execute("""
                    SELECT DISTINCT RoleAttendanceEventId,
                                    Date,
                                    RefAttendanceEventTypeId
                    FROM RoleAttendanceEvent
                    WHERE RefAttendanceEventTypeId = 1 and Date is not null
                    GROUP by date;
                    """).fetchall()
                if(len(_queryRegistroAsistencia) > 0):
                    logger.info(
                        f'Matriculas registradas y asistencia diaria realizada')
                    logger.info(f'Aprobado')
                    return_dict[getframeinfo(currentframe()).function] = True
                    return True
                else:
                    logger.error(
                        f'Asistencia diaria no realizada por el establecimiento')
                    logger.error(f'Rechazado')
                    return_dict[getframeinfo(currentframe()).function] = False
                    return False
            else:
                logger.error(
                    f"Sin matriculas no coinciden con total de alumnos registrados")
                logger.error(f'Rechazado')
                return_dict[getframeinfo(currentframe()).function] = False
                return False
        else:
            logger.error(f'S/Datos')
            logger.error(
                f'No existen alumnos matriculados en el establecimiento')
            return_dict[getframeinfo(currentframe()).function] = False
            return False
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        return False
