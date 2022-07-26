from inspect import getframeinfo, currentframe
from multiprocessing import current_process
import sys


from ede.ede._logger import logger


def fn2CB(conn, return_dict):
    """ 
    5.4 De las bajas en el registro de matrícula
    Validar que exista el registro de entrega de documentos al apoderado.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay estudiantes retirados definitivamente
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Todos los estudiantes retirados definitivamente tienen documento e
            indicente relacionado a la entrega de estos
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _query = []
    _r = False
    try:
        _query = conn.execute("""--sql
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
	AND inc.RefIncidentBehaviorId IN (SELECT RefIncidentBehaviorId FROM RefIncidentBehavior WHERE Description IN ('Entrega de documentos retiro de un estudiante') )
	
WHERE 
	pst.RefPersonStatusTypeId IN (SELECT RefPersonStatusTypeId FROM RefPersonStatusType WHERE Description IN ('Estudiante retirado definitivamente')) 
        """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {str(e)}")

    _erroresDetalle = []
    try:
        if(not _query):
            logger.info(f"S/Datos")
            _r = True
            raise Exception(f"Sin informacion para verificar")

        for row in _query:
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
                _erroresDetalle.append(row)

        if(len(_erroresDetalle) > 0):
            logger.error(
                f"A los siguientes estudiantes no se les entregó correctamente sus documentos de retiro: {_erroresDetalle}")
        else:
            _r = True
    except Exception as e:
        logger.info(
            f"Error on line {sys.exc_info()[-1].tb_lineno}, {type(e).__name__},{e}")
        logger.error(f"{str(e)}")
    finally:
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
