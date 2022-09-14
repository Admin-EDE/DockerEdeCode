from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3C4(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    El campo MaximumCapacity cumple con la siguiente expresión regular: '^[1-9]{1}\d{1,3}$' y todas las organizaciones de la tabla CourseSection son de tipo ASIGNATURA.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Verifica que el campo MaximumCapacity cumpla con la siguiente expresión regular: '^[1-9]{1}\d{1,3}$'
            y que todas las organizaciones de la tabla CourseSection sean de tipo ASIGNATURA
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    RoleAttendanceEvent = []
    try:
        RoleAttendanceEvent = ejecutar_sql(conn, """--sql
        -- Lista todos los IDs que no cumplan con la expresión regular.
        SELECT RoleAttendanceEventId, Date
        FROM RoleAttendanceEvent
        WHERE 
        -- Agrega a la lista todos los registros que no cumplan con la expresión regular
        Date NOT REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
		    AND
		    Date NOT NUll        
      """)
    except Exception as e:
        logger.info(f"Resultado: {RoleAttendanceEvent} -> {str(e)}")
    OrganizationPersonRole = []
    try:
        OrganizationPersonRole = ejecutar_sql(conn, """--sql
        -- Lista todos los IDs que no cumplan con la empresión regular.
        SELECT OrganizationPersonRoleId, EntryDate, ExitDate
        FROM OrganizationPersonRole
        WHERE 
        -- Agrega a la lista todos los registros que no cumplan con la expresión regular
        (
          EntryDate NOT REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$' AND EntryDate NOT NULL
        OR
        ExitDate NOT REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$' AND ExitDate NOT NULL
      )
      AND
      ( EntryDate NOT NULL OR ExitDate NOT NULL )
      """)
    except Exception as e:
        logger.info(f"Resultado: {OrganizationPersonRole} -> {str(e)}")
    try:
        if(len(RoleAttendanceEvent) > 0 or len(OrganizationPersonRole) > 0):
            data1 = list(
                set([m[0] for m in RoleAttendanceEvent if m[0] is not None]))
            data2 = list(
                set([m[0] for m in OrganizationPersonRole if m[0] is not None]))
            _c1 = len(set(data1))
            _c2 = len(set(data2))
            _err1 = f"Las siguientes registros tiene mal formateado el campo Date: {data1}"
            _err2 = f"Las siguientes registros tienen mal formateado el campo EntryDate o ExitDate: {data2}"
            if (_c1 > 0):
                logger.error(_err1)
            if (_c2 > 0):
                logger.error(_err2)
            if (_c1 > 0 or _c2 > 0):
                logger.error(f"Rechazado")
        else:
            logger.info(f"Aprobado")
            _r = True
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la verificación asignaturas sin curso asociado: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
