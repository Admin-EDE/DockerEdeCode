from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn5E4(conn, return_dict):
    """
    REGISTRO DE CONTROL DE ASIGNATURA
    6.2 Contenido mínimo, letra b.2
    La asistencia se encuentra tomada, es decir, cada estudiante tiene alguno de los siguientes estados: Presente, ausente o atrasado.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay asistencias de estudiantes
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Hay asistencias de estudiantes, con fecha y estado (ausente, presente, atrasado)
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _query = ejecutar_sql(conn, """--sql
        SELECT RAE.DATE,
              RAE.RefAttendanceStatusId
        FROM OrganizationPersonRole OPR
                join RoleAttendanceEvent RAE on OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
        where OPR.RoleId = 6
        and RAE.Date is not null;
        """)
        if(len(_query)>0):
          _date = (list(set([m[0] for m in _query if m[0] is not None])))
          if not _date:
            logger.error(f"Sin fecha de asistencia ingresada")
            logger.error(f'Rechazado')
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False
          _status = (list(set([m[1] for m in _query if m[1] is not None])))
          if not _status:
            logger.error(f"Sin estado de asistencia asignado")
            logger.error(f'Rechazado')
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False
          logger.info(f'Aprobado')
          logger.info(f'Todos los registros de asistencia cuentan con un estado asignado')
          return_dict[getframeinfo(currentframe()).function] = True
          logger.info(f"{current_process().name} finalizando...")
          return True
        else:
            logger.info(f"S/Datos")
            logger.info(f"Sin datos de asistencia")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
          logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
          logger.error(f"Rechazado")
          return_dict[getframeinfo(currentframe()).function] = False
          logger.info(f"{current_process().name} finalizando...")
          return False