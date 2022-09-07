from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn6C0(conn, return_dict):
    """
    REGISTRO CONTROL MENSUAL DE ASISTENCIA O CONTROL DE SUBVENCIONES
    6.2 Contenido mínimo, letra c.4
    verificar que los estudiantes excedentes estén registrados 
    en el control de asistencia solo para efectos pedagógicos
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
        _l1 = []
        _s1 = """SELECT c.RUN, A.OrganizationPersonRoleId
                FROM OrganizationPersonRole A
                JOIN Organization B
                ON A.OrganizationId = B.OrganizationId
                JOIN PersonList c
                ON a.personId = c.personId
                JOIN PersonStatus D
                ON A.personId = D.personId
                WHERE A.RoleId = 6
                AND B.RefOrganizationTypeId = 21
                AND D.RefPersonStatusTypeId = 24;"""

        _s2 = """SELECT Date 
                FROM RoleAttendanceEvent
                WHERE OrganizationPersonRoleId = ?;"""

        _q1 = ejecutar_sql(conn, _s1)
        if(len(_q1) != 0):
            for q1 in _q1:
                _r = str(q1[0])
                _op = q1[1]
                _q2 = ejecutar_sql(conn, _s2, _op)
                if(len(_q2) != 0):
                    for q2 in _q2:
                        _d = str(q2[0])
                        _l1.append(f"Alumno: {str(_r)} - fecha: {str(_d)}")

            if(len(_l1) != 0):
                logger.error(
                    f"Los siguientes alumnos excedentes sin derecho a subvencion tienen registro de asistencia a nivel de curso: {str(_l1)}")
                logger.error(f"Rechazado")
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
            else:
                logger.info(f"Aprobado")
                return_dict[getframeinfo(currentframe()).function] = True
                logger.info(f"{current_process().name} finalizando...")
                return True

        else:
            logger.info(
                f"No hay registros de alumnos excedentes sin derecho a subvencion en el establecimiento.")
            logger.info(f"Aprobado")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True

    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
