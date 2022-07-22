from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger

# Revisar que la organización del establecimiento, asignaturas y cursos
# tengan asignada una localidad dentro del establecimiento.


def fn3C3(conn, return_dict):
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
        FROM Organization
        OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
        WHERE RefOrganizationType.Description IN ('Course','Course Section')
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {_ExistData} -> {str(e)}")

    if(_ExistData[0][0] == 0):
        logger.info(f"S/Datos")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    org_loc_id = []
    try:
        org_loc_id = conn.execute("""--sql
      SELECT O.OrganizationId, OL.OrganizationId, OL.Locationid 
      FROM Organization O
 JOIN OrganizationLocation OL ON O.OrganizationId=OL.OrganizationId
WHERE OL.LocationId != (
SELECT CSL.Locationid FROM Organization O
 JOIN CourseSectionLocation CSL ON O.OrganizationId=CSL.OrganizationId)
ORDER BY O.OrganizationId
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {org_loc_id} -> {str(e)}")
    if len(org_loc_id) > 0:
        logger.error(
            f"Error de consistencia, LocationId debe ser consistente en diferentes tablas: {org_loc_id}")
        logger.error(f"Rechazado")
        _r = False
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
    locations = []
    try:
        locations = conn.execute("""--sql
        /*
        * Entrega la lista de organizaciones que no contiene bien definida su ubicación dentro del establecimiento.
        * Los campos obligatorios son: 
        *     RefOrganizationLocationType.Description == 'Physical'
        *     región NOT NULL AND País NOT NULL AND  ApartmentRoomOrSuiteNumber NOT NULL AND BuildingSiteNumber NOT NULL AND
                StreetNumberAndName NOT NULL AND City NOT NULL
        */
        SELECT OrganizationId
        FROM Organization
        OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
        WHERE 
        RefOrganizationType.Description IN ('Course','Course Section')
        AND
        OrganizationId NOT IN (SELECT OrganizationId FROM (SELECT OrganizationId, RefOrganizationType.Description as 'organizationType' , LocationAddress.StreetNumberAndName, LocationAddress.ApartmentRoomOrSuiteNumber, LocationAddress.BuildingSiteNumber, LocationAddress.City, RefState.Description as 'Región', RefCountry.Description as 'País', RefOrganizationLocationType.RefOrganizationLocationTypeId, RefOrganizationLocationType.Description as 'TipoLocalidad'
                FROM Organization
                OUTER LEFT JOIN OrganizationWebsite USING(OrganizationId)
                OUTER LEFT JOIN OrganizationEmail USING(OrganizationId)
                OUTER LEFT JOIN OrganizationTelephone USING(OrganizationId)
                OUTER LEFT JOIN OrganizationLocation USING(OrganizationId)
                OUTER LEFT JOIN RefEmailType USING(RefEmailTypeId)
                OUTER LEFT JOIN RefInstitutionTelephoneType USING(RefInstitutionTelephoneTypeId)
                OUTER LEFT JOIN RefOrganizationLocationType USING(RefOrganizationLocationTypeId)
                OUTER LEFT JOIN LocationAddress USING(LocationId)
                OUTER LEFT JOIN RefState USING(RefStateId)
                OUTER LEFT JOIN RefCountry USING(RefCountryId)
                OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
                WHERE 
                OrganizationType in ('Course','Course Section')
                AND
                tipoLocalidad in ('Physical')
                AND
                región NOT NULL
                AND
                País NOT NULL
                AND 
                ApartmentRoomOrSuiteNumber NOT NULL
                AND
                BuildingSiteNumber NOT NULL
                AND
                StreetNumberAndName NOT NULL
                AND
                City NOT NULL
        ));
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {locations} -> {str(e)}")

    if(len(locations) == 0):
        logger.info(f"Aprobado")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    try:
        logger.info(f"Localidades mal asignadas: {len(locations)}")
        if(len(locations) > 0):
            data1 = list(set([m[0] for m in locations if m[0] is not None]))
            _c1 = len(set(data1))
            _err1 = f"Los siguientes organizaciones no tienen sus ubicaciones bien asignadas: {data1}"
            if (_c1 > 0):
                logger.error(_err1)
                logger.error(f"Rechazado")
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la verificación: {str(e)}")
        logger.error(f"Rechazado")
        _r = False
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
