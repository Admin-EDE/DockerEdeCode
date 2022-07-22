from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


def fn2EA(conn, return_dict):
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
    results = []
    try:
        results = conn.execute("""
        SELECT 
          (
            SELECT identifier 
            FROM PersonIdentifier pi
            JOIN RefPersonIdentificationSystem rfi 
              ON  pi.RefPersonIdentificationSystemId=rfi.RefPersonIdentificationSystemId
              AND rfi.code IN ('SchoolNumber')
            WHERE pi.PersonId = p.PersonId
          ) as "matricula" -- 0
          ,(
            SELECT identifier 
            FROM PersonIdentifier pi
            JOIN RefPersonIdentificationSystem rfi 
              ON  pi.RefPersonIdentificationSystemId=rfi.RefPersonIdentificationSystemId
              AND rfi.code IN ('RUN', 'IPE')
            WHERE pi.PersonId = p.PersonId
          ) as "cedula" -- 1
          , p.FirstName as "primer nombre" -- 2
          , p.MiddleName as "otros nombres" -- 3
          , p.LastName as "apellidoPaterno" -- 4
          , p.SecondLastName as "apellidoMaterno" -- 5
          , case when RTA.Description is null then 'ninguna' else RTA.Description end as "tribalAffiationDescription" -- 6
          , Role.Name as rol -- 7
          , rf.Description as sexo -- 8
          , p.Birthdate as "fechaCumpleaños" -- 9
          , opr.EntryDate as "fecha de incorporacion" -- 10
          , RefCountry.Description as pais -- 11
          , rfs.Description as region -- 12
          , pa.City -- 13
          , rfc.Description as comuna -- 14
          , pa.AddressCountyName -- 15
          , pa.StreetNumberAndName as direccion -- 16
          , pa.ApartmentRoomOrSuiteNumber -- 17
          , pa.PostalCode -- 18
          , p2.FirstName as "Nombre Apoderado" -- 19
          , p2.MiddleName as "segundo nombre apoderado" -- 20
          , p2.LastName as "apellidoPaterno apoderado" -- 21
          , p2.SecondLastName as "apellidoMaterno apoderado" -- 22
          , RefCountry2.Description as paisApoderado -- 23
          , rfs2.Description as regionApoderado -- 24
          , pa2.City as ciudadapoderado -- 25
          , rfc2.Description as comunaApoderado -- 26
          , pa2.AddressCountyName -- 27
          , pa2.StreetNumberAndName as direccionApoderado -- 28
          , pa2.ApartmentRoomOrSuiteNumber -- 29
          , pa2.PostalCode as codigoPostalApoderado -- 30
          , rfpiv.Description -- 31
          , pt2.TelephoneNumber as numeroTelefonicoApoderado -- 32
          , rfptnt.Description as tipoNumeroApoderado -- 33
          , pt2.PrimaryTelephoneNumberIndicator -- 34
          , pea2.EmailAddress as emailApoderado -- 35
          , rfet.Description as tipoEmail -- 36
          , opr.ExitDate as fechaRetiro -- 37
          , opr.OrganizationId -- 38
          , p.personId --39
          , rprs.description -- 40
        FROM Person p 
          JOIN Organization o on o.OrganizationId=opr.OrganizationId			  
  -- Información del estudiante
          OUTER LEFT JOIN RefSex rf on p.RefSexId = rf.RefSexId
          OUTER LEFT JOIN OrganizationPersonRole opr on opr.PersonId=p.PersonId
          OUTER LEFT JOIN RefTribalAffiliation RTA on p.RefTribalAffiliationId = RTA.RefTribalAffiliationId
          OUTER LEFT JOIN Role on Role.RoleId=opr.RoleId
          OUTER LEFT JOIN PersonAddress pa on pa.PersonId=p.PersonId
          OUTER LEFT JOIN RefCountry on pa.RefCountryId = RefCountry.RefCountryId
          OUTER LEFT JOIN RefState rfs on pa.RefStateId= rfs.RefStateId
          OUTER LEFT JOIN RefCounty rfc on pa.RefCountyId = rfc.RefCountyId
          OUTER LEFT JOIN PersonRelationship prs on p.PersonId=prs.RelatedPersonId				
  -- Información del Apoderado
          OUTER LEFT JOIN RefPersonRelationship rprs on prs.RefPersonRelationshipId=rprs.RefPersonRelationshipId
          OUTER LEFT JOIN Person p2 on p2.PersonId=prs.personId 
          OUTER LEFT JOIN PersonAddress pa2 on pa2.PersonId=p2.PersonId
          OUTER LEFT JOIN RefCountry RefCountry2 on pa2.RefCountryId = RefCountry2.RefCountryId
          OUTER LEFT JOIN RefState rfs2 on pa2.RefStateId= rfs2.RefStateId
          OUTER LEFT JOIN RefCounty rfc2 on pa2.RefCountyId = rfc2.RefCountyId
          OUTER LEFT JOIN RefPersonalInformationVerification rfpiv on pa2.RefPersonalInformationVerificationId = rfpiv.RefPersonalInformationVerificationId
          OUTER LEFT JOIN PersonTelephone pt2 on pt2.PersonId = p2.PersonId
          OUTER LEFT JOIN RefPersonTelephoneNumberType rfptnt on pt2.RefPersonTelephoneNumberTypeId = rfptnt.RefPersonTelephoneNumberTypeId
          OUTER LEFT JOIN PersonEmailAddress pea2 on p2.PersonId=pea2.PersonId
          OUTER LEFT JOIN RefEmailType rfet on rfet.RefEmailTypeId = pea2.RefEmailTypeId
        WHERE 
          opr.RoleId IN (
            SELECT RoleId
            FROM Role
            WHERE Name IN ('Estudiante')
          ) 
          AND
          o.RefOrganizationTypeId IN (
            SELECT RefOrganizationTypeId
            FROM RefOrganizationType
            WHERE Description IN ('Course')
          )
        GROUP by p.PersonId
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {results} -> {str(e)}")

    if(len(results) == 0):
        logger.error(f"S/Datos")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    try:
        _response = True
        _err = {}
        for fila in results:
            _errList = []
            if (fila[0] is None):
                _errList.append("alumno sin matricula")
                _response = False
            if (fila[1] is None):
                _errList.append("alumno sin RUN")
                _response = False
            if (fila[2] is None):
                _errList.append("alumno sin nombre")
                _response = False
            if (fila[4] is None):
                _errList.append("alumno sin apellido paterno")
                _response = False
            if (fila[5] is None):
                _errList.append("alumno sin apellido materno")
                _response = False
            if (fila[6] is None):
                _errList.append("alumno sin tribalAffillation")
                _response = False
            if (fila[7] is None):
                _errList.append("alumno sin rol")
                _response = False
            if (fila[8] is None):
                _errList.append("alumno sin sexo")
                _response = False
            if (fila[9] is None):
                _errList.append("alumno sin fecha cumpleaños")
                _response = False
            if (fila[10] is None):
                _errList.append("alumno sin fecha de entrada")
                _response = False
            if (fila[11] is None):
                _errList.append("alumno sin pais")
                _response = False
            if (fila[12] is None):
                _errList.append("alumno sin region")
                _response = False
            if (fila[13] is None):
                _errList.append("alumno sin ciudad")
                _response = False
            if (fila[14] is None):
                _errList.append("alumno sin comuna")
                _response = False
            if (fila[15] is None or fila[16] is None or fila[17] is None):
                _errList.append("alumno sin dirección")
                _response = False
            if (fila[18] is None):
                _errList.append("alumno sin codigo postal")
                _response = False
            # Verifica si existe un apoderado asignado al estudiante
            if(fila[40] == 'Apoderado(a)/Tutor(a)'):
                if (fila[19] is None):
                    _errList.append("apoderado alumno sin nombre")
                    _response = False
                if (fila[21] is None):
                    _errList.append("apoderado alumno sin apellido paterno")
                    _response = False
                if (fila[22] is None):
                    _errList.append("apoderado alumno sin apellido materno")
                    _response = False
                if (fila[23] is None):
                    _errList.append("apoderado alumno sin pais")
                    _response = False
                if (fila[24] is None):
                    _errList.append("apoderado alumno sin region")
                    _response = False
                if (fila[25] is None):
                    _errList.append("apoderado alumno sin ciudad")
                    _response = False
                if (fila[26] is None):
                    _errList.append("apoderado alumno sin comuma")
                    _response = False
                if (fila[27] is None or fila[28] is None or fila[29] is None):
                    _errList.append("apoderado alumno sin direccion")
                    _response = False
                if (fila[30] is None):
                    _errList.append("apoderado alumno sin codigo postal")
                    _response = False
                if (fila[32] is None):
                    _errList.append("apoderado alumno sin numero telefonico")
                    _response = False
                if (fila[33] is None):
                    _errList.append(
                        "apoderado alumno sin tipo de numero telefonico")
                    _response = False
                if (fila[34] is None):
                    _errList.append(
                        "apoderado alumno sin verificador de numero primario")
                    _response = False
                if (fila[35] is None):
                    _errList.append("apoderado alumno sin email")
                    _response = False
                if (fila[36] is None):
                    _errList.append("apoderado alumno sin tipo de email")
                    _response = False
            else:
                _errList.append("El estudiante no tiene un apoderdo asignado")
                _response = False

            if(len(_errList) > 0):
                _err[fila[39]] = _errList

        if(_response):
            logger.info(f"datos de alumnos validados")
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.error(f"Rechazado")
            logger.error(f"errores encontrados: {_err}")
    except Exception as e:
        logger.info(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
