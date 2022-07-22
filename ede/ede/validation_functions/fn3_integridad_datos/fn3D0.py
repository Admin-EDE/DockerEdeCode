from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede._logger import logger

# Verifica que cada asignatura se encuentre asociada a un curso.
# Entrega los organizationID de las asignaturas
# que no están asociadas a ningún curso


def fn3D0(conn, return_dict):
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
    _ExistData = []
    try:
        _ExistData = conn.execute("""
        SELECT count(OrganizationId)
        FROM OrganizationRelationship
        INNER JOIN Organization USING(OrganizationId)
        WHERE 
          -- PERMITE solo las organizaciones de tipo ASIGNATURA
          RefOrganizationTypeid in (
            SELECT RefOrganizationTypeid
            FROM RefOrganizationType 
            WHERE Description LIKE 'Course Section'
          )
                                """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {_ExistData} -> {str(e)}")

    if(_ExistData[0][0] == 0):
        logger.info(f"S/Datos")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    asignaturas = []
    try:
        asignaturas = conn.execute("""
        /* 
        * Selecciona de la tabla Organization los ID's de todas las asignaturas
        * que no tengan un curso asociado 
        */ 
        WITH refOrganizationTypeAsignatura AS (SELECT RefOrganizationTypeid FROM RefOrganizationType WHERE Description LIKE 'Course Section')
                SELECT o.Organizationid 
                FROM Organization o
                WHERE 
                        -- Selecciona de la lista solo las organizaciones de tipo ASIGNATURA
                        RefOrganizationTypeid in refOrganizationTypeAsignatura AND 
                        -- Con el fin de encontrar las ASIGNATURAS que no se encuentren asociadas a ningún curso, 
                        -- se excluye de la lista las organizaciones que se encuentran correctamente asignadas
                        o.OrganizationId NOT IN (
                                -- Esta consulta obtiene la lista de ASIGNATURAS correctamente asignadas a un CURSO
                                SELECT OrganizationId
                                FROM OrganizationRelationship
                                INNER JOIN Organization USING(OrganizationId)
                                WHERE 
                                        -- PERMITE solo las organizaciones de tipo ASIGNATURA
                                        RefOrganizationTypeid in refOrganizationTypeAsignatura
                                        AND
                                        -- PERMITE solo las asignaciones que tengan como padre un CURSO
                                        Parent_OrganizationId IN (
                                                -- Obtiene la lista de Organizaciones de tipo CURSO
                                                SELECT OrganizationId 
                                                FROM Organization
                                                WHERE RefOrganizationTypeId IN (
                                                        -- Recupera el ID de referencia de las organizaciones tipo CURSO
                                                        SELECT RefOrganizationTypeid FROM RefOrganizationType WHERE Description LIKE 'Course'
                                                )
                                        )
                );
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {asignaturas} -> {str(e)}")

    if(len(asignaturas) == 0):
        logger.info(f"Aprobado")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    try:
        logger.info(
            f"Organizaciones no asociadas a ningún curso: {len(asignaturas)}")
        if(len(asignaturas) > 0):
            asignaturasList = list(
                set([m[0] for m in asignaturas if m[0] is not None]))
            _c = len(set(asignaturasList))
            _err = f"Las siguientes asignaturas no tienen ningún curso asociado: {asignaturasList}"
            logger.error(_err)
            logger.error(f"Rechazado")
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la verificación asignaturas sin curso asociado: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
