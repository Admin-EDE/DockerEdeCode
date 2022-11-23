from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3DD(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    El establecimiento tiene su información mínima ingresada.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - El campo OrganizationWebsite.Website debe estar definido para la organización del establecimiento
            - El campo Organizationemail.addressElectronicMailAddress debe estar definido para la organización 
            del establecimiento
            - El campo Organizationemail.RefEmailTypeId debe estar definido para la organización del establecimiento, 
            al menos, el tipo Organizational (school) address [3]
            - Debe estar definido el número del establecimiento OrganizationTelephone.TelephoneNumber. 
            Para la organización del establecimiento OrganizationTelephone.RefInstitutionTelephoneTypeId debe 
            estar definido, al menos, los códigos Main phone number (2) y Administrative phone number (3), 
            si son iguales se repite. 
            - El primer código es para comunicarse directamente con La Dirección del establecimiento, el otro es para 
            los llamados administrativos.
            - Para la organización del establecimiento OrganizationLocation.RefOrganizationLocationTypeId debe estar 
            definido Mailing [1], Physical [2] y Shipping [3], si es la misma para todos los casos, se debe repetir.
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    school = []
    try:
        school = ejecutar_sql(conn, """--sql
        -- Revisa que la organización tipo Establecimiento tenga registrada su página web
        SELECT OrganizationId, RefOrganizationType.Description as 'organizationType',Website
        FROM Organization
        JOIN OrganizationWebsite USING(OrganizationId)
        JOIN RefOrganizationType USING(RefOrganizationTypeId)
        WHERE 
        RefOrganizationType.Description IN ('K12 School')
      """)
    except Exception as e:
        logger.info(f"Resultado: {school} -> {str(e)}")

    if(len(school) == 0):
        logger.error(f"S/Datos")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    webSite = []
    try:
        webSite = ejecutar_sql(conn, """--sql
        -- Revisa que la organización tipo Establecimiento tenga registrada su página web
        SELECT OrganizationId, RefOrganizationType.Description as 'organizationType',Website
        FROM Organization
        OUTER LEFT JOIN OrganizationWebsite USING(OrganizationId)
        OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
        WHERE 
        RefOrganizationType.Description IN ('K12 School')
        AND
        Website NOT REGEXP '^(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})$'
      """)
    except Exception as e:
        logger.info(f"Resultado: {webSite} -> {str(e)}")

    ElectronicMailAddress = []
    try:
        ElectronicMailAddress = ejecutar_sql(conn, """--sql
        -- Revisa que la organización tipo Establecimiento tenga registrado su email de contacto
        SELECT OrganizationId, ElectronicMailAddress
        FROM Organization
        OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
        OUTER LEFT JOIN OrganizationEmail USING(OrganizationId)
        OUTER LEFT JOIN RefEmailType USING(RefEmailTypeId)
        WHERE 
        RefOrganizationType.Description IN ('K12 School')
        AND
        ElectronicMailAddress NOT REGEXP '^(?:[a-z0-9!#$%&''*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&''*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])$'
        AND
        RefEmailType.Description IN ('Organizational (school) address')
      """)
    except Exception as e:
        logger.info(f"Resultado: {ElectronicMailAddress} -> {str(e)}")

    phoneNumbers = []
    try:
        phoneNumbers = ejecutar_sql(conn, """--sql
        -- Revisa que la organización tipo Establecimiento tenga registrados sus teléfonos de contacto
        SELECT DISTINCT OrganizationId, RefOrganizationType.Description as 'organizationType', TelephoneNumber, RefInstitutionTelephoneType.Description as 'phoneType'--, LocationAddress.StreetNumberAndName, LocationAddress.ApartmentRoomOrSuiteNumber, LocationAddress.BuildingSiteNumber, LocationAddress.City, RefState.Description as 'Región', RefCountry.Description as 'País', LocationAddress.PostalCode, LocationAddress.Latitude, LocationAddress.Longitude, RefOrganizationLocationType.Description as 'TipoLocalidad'
        FROM Organization
        OUTER LEFT JOIN OrganizationTelephone USING(OrganizationId)
        OUTER LEFT JOIN RefInstitutionTelephoneType USING(RefInstitutionTelephoneTypeId)
        OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
        WHERE 
        OrganizationType in ('K12 School')
        AND
        TelephoneNumber NOT REGEXP '^\+56\d{9,15}$'
        AND 
        phoneType IN ('Main phone number','Administrative phone number')
      """)
    except Exception as e:
        logger.info(f"Resultado: {phoneNumbers} -> {str(e)}")

    locations = []
    try:
        locations = ejecutar_sql(conn, """--sql
        -- Revisa que las ubicaciones del establecimiento se encuentren bien definidas.
        SELECT DISTINCT OrganizationId, RefOrganizationType.Description as 'organizationType', LocationAddress.StreetNumberAndName, LocationAddress.ApartmentRoomOrSuiteNumber, LocationAddress.BuildingSiteNumber, LocationAddress.City, RefState.Description as 'Región', RefCountry.Description as 'País', LocationAddress.PostalCode, LocationAddress.Latitude, LocationAddress.Longitude, RefOrganizationLocationType.Description as 'TipoLocalidad'
        FROM Organization
        OUTER LEFT JOIN OrganizationLocation USING(OrganizationId)
        OUTER LEFT JOIN RefOrganizationLocationType USING(RefOrganizationLocationTypeId)
        OUTER LEFT JOIN LocationAddress USING(LocationId)
        OUTER LEFT JOIN RefState USING(RefStateId)
        OUTER LEFT JOIN RefCountry USING(RefCountryId)
        OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
        WHERE 
        OrganizationType in ('K12 School')
        AND
        tipoLocalidad IN ('Physical', 'Mailing', 'Shipping')
        AND
        (ApartmentRoomOrSuiteNumber IS NULL
        OR
        BuildingSiteNumber IS NULL
        OR
        LocationAddress.City IS NULL
        OR
        RefState.Description IS NULL
        OR
        RefCountry.Description IS NULL
        OR 
        LocationAddress.PostalCode IS NULL
        OR
        LocationAddress.Latitude IS NULL
        OR
        LocationAddress.Longitude IS NULL
        )
      """)
    except Exception as e:
        logger.info(f"Resultado: {locations} -> {str(e)}")

    try:
        _err = False
        if(len(webSite) > 0 or len(ElectronicMailAddress) > 0 or len(phoneNumbers) > 0 or len(locations) > 0):
            data = list(set([m[0] for m in webSite if m[0] is not None]))
            if (len(set(data)) > 0):
                logger.error(f"Website con formato erroneo: {data}")
                _err = True

            data = list(
                set([m[0] for m in ElectronicMailAddress if m[0] is not None]))
            if (len(set(data)) > 0):
                logger.error(
                    f"ElectronicMailAddress con formato erroneo: {data}")
                _err = True

            data = list(set([m[0] for m in phoneNumbers if m[0] is not None]))
            if (len(set(data)) > 0):
                logger.error(f"phoneNumbers con formato erroneo: {data}")
                _err = True

            data = list(set([m[0] for m in locations if m[0] is not None]))
            if (len(set(data)) > 0):
                logger.error(f"locations con formato erroneo: {data}")
                _err = True

        if (not _err):
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.error(f"Rechazado")
            _r = False
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la verificación: {str(e)}")
        logger.error(f"Rechazado")
        _r = False
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
