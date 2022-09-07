from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.validation_functions.check_utils as check_utils
from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3CA(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    Verifica que existan campos relacionados a la asistencia
    --------------------------------------------------
    Verificar que el evento “Daily attendance” sea solo asignado a  organizationId de tipo curso
    Verificar que el evento “Class/section attendance” sea solo asignado a  organizationId de tipo asignatura
    Verificar que el estado “Reingreso autorizado” sea solo asignado al organizationId del establecimiento
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay eventos de asistencia
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Hay asistencias y están bien asignadas a las organizaciones
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = ejecutar_sql(conn, """--sql
      SELECT DISTINCT ast.Description as 'RefAttendanceStatus',aet.Description as 'AttendanceEventType', orgt.Description as 'OrganizationType'
      FROM RoleAttendanceEvent rae
      INNER JOIN OrganizationPersonRole opr on opr.OrganizationPersonRoleId = rae.OrganizationPersonRoleId
      INNER JOIN Organization org on org.OrganizationId = opr.OrganizationId
      INNER JOIN RefAttendanceEventType aet on aet.RefAttendanceEventTypeId = rae.RefAttendanceEventTypeId
      INNER JOIN RefOrganizationType orgt on orgt.RefOrganizationTypeId = org.RefOrganizationTypeId
      INNER JOIN RefAttendanceStatus ast on ast.RefAttendanceStatusId = rae.RefAttendanceStatusId;
      """)
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")
    try:
        logger.info(f"len(OrganizationType): {len(rows)}")
        if(len(rows) > 0):
            # Siempre deberían existir elementos de asistencia
            data = list(set([(m[0], m[1], m[2])
                        for m in rows if m[0] is not None]))
            _err, _r = check_utils.imprimeErrores(data, check_utils.validaEventosDeAsistencia,
                                                  "VERIFICA que los eventos de asistencia se encuentren correctamente asignados")
            logger.info(f"Aprobado") if _r else logger.error(_err)
        else:
            logger.info("La BD no contiene información de asistencia cargada")
            logger.info("S/Datos")
    except Exception as e:
        logger.error(
            f"No se pudo verificar que los eventos de asistencia esten bien asignados a las Organizaciones: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
