from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


def fn1FC(conn, return_dict):
    """ 
    Validar que exista cargado en la base de datos el documento digital que 
    acredite la entrega al apoderado de los documentos necesarios para la 
    continuidad del estudiante dentro del sistema educativo.
    -------------------------------------------------------------------------
    Revisar que la entrega de documentos se encuentre cargada en las 
    incidencias como un tipo de reunión con el apoderado.

    En tabla Indicent.RefIncidentBehaviorId == 36 
    (Entrega de información en formato digital) y 
    IncidentPerson.digitalRandomKey OR fileScanBase64 según sea el caso
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay alumnos retirados
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Los alumnos retirados tienen entregado a su apoderado los documentos para continuidad de estudios
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = conn.execute("""--sql
SELECT DISTINCT
	 pst.personId as 'estudianteId'
	, pid.Identifier as 'RUN'
	, oprAlumno.RoleId
	, prshApoderado.personId as 'ApoderadoId'
	, prshApoderado.RefPersonRelationshipId
	, oprApoderado.RoleId
	, incP.IncidentId
	, incP.RefIncidentPersonTypeId
	, incP.Date	
	, incP.digitalRandomKey
	, incP.fileScanBase64
	, doc.fileScanBase64 

FROM PersonStatus pst

-- RESCATA INFORMACIÓN DEL ESTUDIANTE
OUTER LEFT JOIN PersonIdentifier pid
	ON pst.personId = pid.personId
	AND pid.RefPersonIdentificationSystemId IN (SELECT RefPersonIdentificationSystemId FROM RefPersonIdentificationSystem WHERE CODE IN ('RUN'))
OUTER LEFT JOIN OrganizationPersonRole oprAlumno
	ON pst.personId = oprAlumno.personId
	AND oprAlumno.RoleId IN (SELECT RoleId FROM Role WHERE Name IN ('Estudiante'))

-- RESCATA INFORMACIÓN DEL APODERADO O TUTOR ENCARGADO DEL ESTUDIANTE
OUTER LEFT JOIN PersonRelationship prshApoderado
	ON prshApoderado.RelatedPersonId = pst.personId
	AND prshApoderado.RefPersonRelationshipId IN (SELECT RefPersonRelationshipId FROM RefPersonRelationship WHERE Code IN ('Apoderado(a)/Tutor(a)'))

OUTER LEFT JOIN OrganizationPersonRole oprApoderado
	ON oprApoderado.personId = prshApoderado.personId
	AND oprApoderado.RoleId IN (SELECT RoleId FROM Role WHERE Name IN ('Padre, madre o apoderado'))
	
-- RESCATA INFORMACIÓN RELACIONADA CON LA ENTREGA DE INFORMACIÓN
OUTER LEFT JOIN IncidentPerson incP
	ON incP.personId = prshApoderado.personId
	AND incP.RefIncidentPersonTypeId IN (SELECT RefIncidentPersonTypeId FROM RefIncidentPersonType WHERE Description IN ('Apoderado','Parent/guardian'))
 
OUTER LEFT JOIN Document doc
	ON incP.fileScanBase64 = documentId

OUTER LEFT JOIN Incident Inc
	ON inc.IncidentId = incP.IncidentId
	AND inc.RefIncidentBehaviorId IN (SELECT RefIncidentBehaviorId FROM RefIncidentBehavior WHERE Description IN ('Entrega de información para continuidad de estudios') )
	
WHERE 
	pst.RefPersonStatusTypeId IN (SELECT RefPersonStatusTypeId FROM RefPersonStatusType WHERE Description IN ('Estudiante retirado definitivamente')) 
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    if(len(rows) <= 0):
        _r = True
        logger.info(f"S/Datos")
        return_dict[getframeinfo(currentframe()).function] = _r
        return _r

    _resp = []
    try:
        for row in rows:
            estudianteId = row[0]
            estudianteRUN = row[1]
            estudianteRole = row[2]
            apoderadoId = row[3]
            apoderadoRefPersonRelationShip = row[4]
            apoderadoRole = row[5]
            incidentId = row[6]
            incidentType = row[7]
            incidentDate = row[8]
            incidentKey = row[9]
            incidentFile = row[10]
            fileScanBase64 = row[11]

            if(not (estudianteId
               and
               estudianteRUN
               and
               estudianteRole
               and
               apoderadoId
               and
               apoderadoId
               and
               apoderadoRefPersonRelationShip
               and
               apoderadoRole
               and
               incidentId
               and
               incidentType
               and
               incidentDate
               and
               (incidentKey or incidentFile)
               and
               fileScanBase64)
               ):
                _resp.append(row)

        if(len(_resp) <= 0):
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.error(f"Rechazado")
            logger.info(f"{_resp}")
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
