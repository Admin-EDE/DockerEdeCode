from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn7F1(conn, return_dict):
    """
    REGISTRO DE EVALUACIONES Y SECTORES EDUCATIVOS
    6.2 Contenido mínimo, letra d
    verificar que las calificaciones de las evaluaciones sumativas 
    sean representadas en una escala de 1 a 7 hasta con un decimal.
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
    _query = []
    try:
        _query = ejecutar_sql(conn, """--sql
        SELECT round(R.ScoreValue, 1) AS value,
              R.ScoreValue           AS fullValue
        FROM AssessmentResult R
                JOIN AssessmentRegistration AR ON AR.AssessmentRegistrationId = R.AssessmentRegistrationId
                JOIN AssessmentAdministration AA ON AR.AssessmentAdministrationId = AA.AssessmentAdministrationId
                JOIN Assessment A ON AA.AssessmentId = A.AssessmentId
                JOIN AssessmentSession ASN ON ASN.AssessmentAdministrationId = AA.AssessmentAdministrationId
                JOIN AssessmentSessionStaffRole ASSR ON ASN.AssessmentSessionId = ASSR.AssessmentSessionId
                JOIN OrganizationPersonRole OPR ON OPR.PersonId = ASSR.PersonId
                
                JOIN Organization ORG 
                  ON ORG.OrganizationId = OPR.OrganizationId
                JOIN RefOrganizationType ROT 
                  ON ROT.RefOrganizationTypeId = ORG.RefOrganizationTypeId
                  AND ROT.Description IN ('Course')

                JOIN OrganizationRelationship orsh 
                  ON orsh.OrganizationId = ORG.OrganizationId
                JOIN Organization orgGrado 
                  ON orgGrado.OrganizationId = orsh.Parent_OrganizationId

              JOIN OrganizationRelationship orsh2
                  ON orsh2.OrganizationId = orgGrado.OrganizationId
                JOIN Organization orgCodEns
                  ON orgCodEns.OrganizationId = orsh2.Parent_OrganizationId
                  AND orgCodEns.name IN ('110:Enseñanza Básica','310:Enseñanza Media H-C niños y jóvenes','410:Enseñanza Media T-P Comercial Niños y Jóvenes','510:Enseñanza Media T-P Industrial Niños y Jóvenes','610:Enseñanza Media T-P Técnica Niños y Jóvenes','710:Enseñanza Media T-P Agrícola Niños y Jóvenes','810:Enseñanza Media T-P Marítima Niños y Jóvenes','910:Enseñanza Media Artística Niños y Jóvenes')	
                
        WHERE A.RefAssessmentTypeId = 29
          AND R.RefScoreMetricTypeId IN (1, 2)
          AND ASSR.RefAssessmentSessionStaffRoleTypeId = 6
          AND OPR.RoleId = 6
        GROUP BY ASN.AssessmentAdministrationId, ASN.AssessmentSessionId, ASSR.AssessmentSessionStaffRoleId;
        """)
    except Exception as e:
      logger.info(f"Resultado: {_query} -> {str(e)}")
    
    try:
        if(len(_query)>0):
          _contador = 0
          _assessment = int(len(_query))
          _assessmentScoreValue = (list([m[0] for m in _query if m[0] is not None]))
          _assessmentScoreFullValue = (list([m[1] for m in _query if m[1] is not None]))
          for y in _assessmentScoreFullValue:
            if (len(y)>3):
              logger.error(f'Se han ingresado calificaciones sumativas con mas de un decimal')
              logger.error(f'Rechazado')
              _r = False
          for x in _assessmentScoreValue:
            if (x >= 1.0 and x <= 7.0):
              _contador += 1
          if _contador == _assessment:
            logger.info(f'Todas las evaluaciones sumativas estan ingresadas correctamente')
            logger.info(f'Aprobado')
            _r = True
          else:
            logger.error(f'No todas las evaluaciones estan entre el rango permitido de 1.0 - 7.0')
            logger.error(f'Rechazado')
            _r = False
        else:
          logger.error(f'S/Datos')
          _r = True
          logger.error(f'No se encuentran evaluaciones sumativas registradas en el establecimiento')
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        _r = False
    finally:
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")      
      return _r