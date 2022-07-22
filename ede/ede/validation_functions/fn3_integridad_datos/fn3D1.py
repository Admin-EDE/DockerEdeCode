from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger

# Verifica que el campo MaximumCapacity cumpla con la siguiente expresión regular: '^[1-9]{1}\d{1,3}$'
#  y que todas las organizaciones de la tabla CourseSection sean de tipo ASIGNATURA


def fn3D1(conn, return_dict):
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
    _r = False
    MaximumCapacityErrors = []
    try:
        MaximumCapacityErrors = conn.execute("""
        -- Selecciona los Organizaciones de tipo ASIGNATURA que no cumplen con el criterio de la expresión regular
        SELECT OrganizationId, MaximumCapacity
        FROM CourseSection
        OUTER LEFT JOIN Organization USING(OrganizationId)
        WHERE 
          -- Agrega a la lista todos los registros que no cumplan con la expresión regular
          MaximumCapacity NOT REGEXP "^[1-9]{1}\d{1,3}$"
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {MaximumCapacityErrors} -> {str(e)}")

    organizationMalAsignadas = []
    try:
        organizationMalAsignadas = conn.execute("""
          -- Selecciona las Organizaciones que no son de tipo ASIGNATURA 
          SELECT OrganizationId
          FROM CourseSection
          OUTER LEFT JOIN Organization USING(OrganizationId)
          WHERE 
                  -- Agrega a la lista todas las organizaciones que no sean de tipo ASIGNATURA
                  RefOrganizationTypeid NOT IN (
                          -- Rescata desde la tabla de referencia el ID de las organizaciones de tipo ASIGNATURA
                          SELECT RefOrganizationTypeid 
                          FROM RefOrganizationType 
                          WHERE Description LIKE 'Course Section'
                  )
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {organizationMalAsignadas} -> {str(e)}")

    try:
        logger.info(
            f"MaximunCapacity mal asignados: {len(MaximumCapacityErrors)}, Tabla CourseSection con organizacion mal asignadas: {len(organizationMalAsignadas)}")
        if(len(MaximumCapacityErrors) > 0 or len(organizationMalAsignadas) > 0):
            data1 = list(
                set([m[0] for m in MaximumCapacityErrors if m[0] is not None]))
            data2 = list(
                set([m[0] for m in organizationMalAsignadas if m[0] is not None]))
            _c1 = len(set(data1))
            _c2 = len(set(data2))
            _err1 = f"Las siguientes asignaturas no tiene el campo MaximumCapacity declarado correctamente: {data1}"
            _err2 = f"Las siguientes organizaciones no son de tipo asignaturas: {data2}"
            if (_c1 > 0):
                logger.error(_err1)
            if (_c2 > 0):
                logger.error(_err2)
            if (_c1 > 0 or _c2 > 0):
                logger.error(f"Rechazado")
                return_dict[getframeinfo(currentframe()).function] = False
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
