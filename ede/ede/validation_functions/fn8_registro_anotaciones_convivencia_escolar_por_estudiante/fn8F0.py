from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.validation_functions.check_utils as check_utils
from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn8F0(conn, return_dict):
    """
    REGISTRO DE ANOTACIONES DE CONVIVENCIA ESCOLAR POR ESTUDIANTE
    6.2 Contenido mínimo, letra e
    Verificar que exista registro de la siguiente información
      - Anotaciones negativas de su comportamiento
      - Citaciones a los apoderados sobre temas relativos a sus pupilos.
      - Medidas disciplinarias que sean aplicadas al estudiante.
      - Reconocimientos por destacado cumplimiento del reglamento interno (positivas).      
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y "S/Datos" a través de logger si no encuentra información.
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - encontrar un JSON en el campo RegulationViolatedDescription con este formato
              {
              "ArtículoProtocolo":"Titulo II, articulo 5",
              "Severidad":"Leve",
              "Procedimiento":"",
              }
            - Las anotaciones negativas debería clasificarse según la tabla 
            refIncidentBehavior, para el caso de anotaciones positivas usar 
            Incident.RefIncidentBehaviorId == 34 (Anotación Positiva).
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    allIncidents = []
    try:
        allIncidents = ejecutar_sql(conn, """--sql
                  SELECT 
                    I.IncidentId,
                    I.IncidentIdentifier,
                    I.IncidentDate,
                    I.IncidentTime,
                    I.IncidentDescription,
                    I.RefIncidentBehaviorId,
                    I.RegulationViolatedDescription,
                    rInBh.Description,
                    IncPer.personId,
                    IncPer.RefIncidentPersonTypeId,
                    IncPer.Date,
                    rdat.RefDisciplinaryActionTakenId
                  FROM Incident I
                    OUTER LEFT JOIN K12StudentDiscipline K12SD 
                      ON K12SD.IncidentId = I.IncidentId
                    OUTER LEFT JOIN OrganizationPersonRole OPR
                      ON K12SD.OrganizationPersonRoleId = OPR.OrganizationPersonRoleId
                    OUTER LEFT JOIN Role rol
                      ON rol.RoleId = OPR.RoleId
                    OUTER LEFT JOIN K12StudentAcademicHonor K12SA
                      ON K12SA.OrganizationPersonRoleId = OPR.OrganizationPersonRoleId
                    OUTER LEFT JOIN RefIncidentBehavior rInBh
                      ON rInBh.RefIncidentBehaviorId = I.RefIncidentBehaviorId                      
                    OUTER LEFT JOIN IncidentPerson IncPer
                      ON IncPer.IncidentId = I.IncidentId
                    OUTER LEFT JOIN RefDisciplinaryActionTaken rdat					
                      ON K12SD.RefDisciplinaryActionTakenId = rdat.RefDisciplinaryActionTakenId
                  GROUP BY I.IncidentId    
      """)
    except Exception as e:
        logger.info(f"Resultado: {allIncidents} -> {str(e)}")

    if(len(allIncidents) == 0):
        logger.info(f"S/Datos")
        _r = True

    FineRows = []
    try:
        if(len(allIncidents) > 0):
            for incident in allIncidents:
                incidentId = incident[0]
                incidentIdentifier = incident[1]
                incidentDate = incident[2]
                incidentTime = incident[3]
                incidentDesc = incident[4]
                RefIncidentBehaviorId = incident[5]
                isJsonValidRegulationViolatedDesc = check_utils.validateJSON(
                    incident[6])
                refIncidentBehaviorDesc = incident[7]
                PersonId = incident[8]
                refIncidentPersonId = incident[9]
                incidentPersonDate = incident[10]
                refDisciplinaryActionTaken = incident[11]
                # print(incidentId,RefIncidentBehaviorDescription,isJsonValid)

                if(incidentId is None
                   or incidentIdentifier is None
                   or incidentTime is None
                   or incidentDate is None
                   or incidentDesc is None
                   or RefIncidentBehaviorId is None
                   or isJsonValidRegulationViolatedDesc is None
                   or PersonId is None
                   or refIncidentPersonId is None
                   or incidentPersonDate is None
                   or isJsonValidRegulationViolatedDesc == False):
                    logger.error("Rechazado")
                    logger.error("Los campos obligatorios no pueden ser nulos")
                    return_dict[getframeinfo(currentframe()).function] = False
                    logger.info(f"{current_process().name} finalizando...")
                    return False

                if(refIncidentBehaviorDesc not in (
                    'Entrevista', 'Reunión con apoderados', 'Entrega de documentos retiro de un estudiante', 'Anotación positiva', 'Entrega de documentos de interés general', 'Entrega de información para continuidad de estudios')
                   and refDisciplinaryActionTaken is None):
                    logger.error("Rechazado")
                    logger.error(
                        "Las anotaciones negativas deben tener acciones asociadas")
                    return_dict[getframeinfo(currentframe()).function] = False
                    logger.info(f"{current_process().name} finalizando...")
                    return False

            #resultList  = [item[0] for item in allIncidents if item not in FineRows]

            # if( len(resultList) > 0):
            #  logger.info(f"Rechazado")
                #logger.info(f"Los incidentId con problemas son: {resultList}")
            # else:
        logger.info(f"Aprobado")
        _r = True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
