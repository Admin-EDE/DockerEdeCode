from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede._logger import logger


def fn7F2(conn, return_dict):
    """
    REGISTRO DE EVALUACIONES Y SECTORES EDUCATIVOS
    6.2 Contenido mínimo, letra d
    verificar que la calificación final mínima de aprobación del estudiante sea un 4.0.
    -------------------
    Verificar que si en la tabla PersonStatus el estudiante tiene el estado promovido, 
    su calificación final sea, al menos, de un cuatro (4,00)
    Tablas PersonStatus y Assessment Result
    RefPersonStatusType = 28 (Estudiante promovido)

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
        _query = conn.execute("""
        SELECT 
			  DISTINCT PS.PersonId
			,orgCodEns.name as 'cod_enseñanza'			  
			,orgGrado.name as 'grado'
			, ORG.name			
        FROM OrganizationPersonRole OPR

        JOIN Person P 
					ON OPR.PersonId = P.PersonId
        JOIN PersonStatus PS 
					ON P.PersonId = PS.PersonId
				JOIN Organization ORG 
					ON ORG.OrganizationId = OPR.OrganizationId
				JOIN RefOrganizationType ROT 
					ON ROT.RefOrganizationTypeId = ORG.RefOrganizationTypeId
					AND ROT.Description IN ('Course')

				JOIN OrganizationRelationship orsh 
					ON orsh.OrganizationId = ORG.OrganizationId
				JOIN Organization orgGrado 
					ON orgGrado.OrganizationId = orsh.Parent_OrganizationId
					AND orgGrado.name NOT IN ('110.01:1º Básico')

			JOIN OrganizationRelationship orsh2
					ON orsh2.OrganizationId = orgGrado.OrganizationId
				JOIN Organization orgCodEns
					ON orgCodEns.OrganizationId = orsh2.Parent_OrganizationId
					AND orgCodEns.name IN ('110:Enseñanza Básica','310:Enseñanza Media H-C niños y jóvenes','410:Enseñanza Media T-P Comercial Niños y Jóvenes','510:Enseñanza Media T-P Industrial Niños y Jóvenes','610:Enseñanza Media T-P Técnica Niños y Jóvenes','710:Enseñanza Media T-P Agrícola Niños y Jóvenes','810:Enseñanza Media T-P Marítima Niños y Jóvenes','910:Enseñanza Media Artística Niños y Jóvenes')	

	
        WHERE OPR.RoleId = 6
          AND PS.RefPersonStatusTypeId = 28;
        """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {_query} -> {str(e)}")

    if(len(_query) == 0):
        logger.info(f'No existen estudiantes promovidos en el establecimiento')
        logger.info(f'S/Datos')
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    _scoreQuery = []
    try:
        _scoreQuery = conn.execute("""
      SELECT round((sum(replace(R.ScoreValue, ',', '')) / count(R.ScoreValue)), 0), R.RefScoreMetricTypeId as 'tipo'
      FROM AssessmentResult R
              JOIN AssessmentRegistration AR ON AR.AssessmentRegistrationId = R.AssessmentRegistrationId
              JOIN AssessmentAdministration AA ON AR.AssessmentAdministrationId = AA.AssessmentAdministrationId
              JOIN Assessment A ON AA.AssessmentId = A.AssessmentId
              JOIN AssessmentSession ASN ON ASN.AssessmentAdministrationId = AA.AssessmentAdministrationId
              JOIN AssessmentSessionStaffRole ASSR ON ASN.AssessmentSessionId = ASSR.AssessmentSessionId
      WHERE A.RefAssessmentTypeId IN (28, 29)
        AND R.RefScoreMetricTypeId IN (1, 2)
        AND ASSR.RefAssessmentSessionStaffRoleTypeId = 6
        AND ASSR.PersonId IN (SELECT DISTINCT PS.PersonId
                              FROM OrganizationPersonRole OPR
                                      JOIN Person P ON OPR.PersonId = P.PersonId
                                      JOIN PersonStatus PS ON P.PersonId = PS.PersonId
                                      
                                      JOIN Organization ORG 
                                        ON ORG.OrganizationId = OPR.OrganizationId
                                      JOIN RefOrganizationType ROT 
                                        ON ROT.RefOrganizationTypeId = ORG.RefOrganizationTypeId
                                        AND ROT.Description IN ('Course')

                                      JOIN OrganizationRelationship orsh 
                                        ON orsh.OrganizationId = ORG.OrganizationId
                                      JOIN Organization orgGrado 
                                        ON orgGrado.OrganizationId = orsh.Parent_OrganizationId
                                        AND orgGrado.name NOT IN ('110.01:1º Básico')

                                    JOIN OrganizationRelationship orsh2
                                        ON orsh2.OrganizationId = orgGrado.OrganizationId
                                      JOIN Organization orgCodEns
                                        ON orgCodEns.OrganizationId = orsh2.Parent_OrganizationId
                                        AND orgCodEns.name IN ('110:Enseñanza Básica','310:Enseñanza Media H-C niños y jóvenes','410:Enseñanza Media T-P Comercial Niños y Jóvenes','510:Enseñanza Media T-P Industrial Niños y Jóvenes','610:Enseñanza Media T-P Técnica Niños y Jóvenes','710:Enseñanza Media T-P Agrícola Niños y Jóvenes','810:Enseñanza Media T-P Marítima Niños y Jóvenes','910:Enseñanza Media Artística Niños y Jóvenes')	
                                                                           
                              WHERE OPR.RoleId = 6
                                AND PS.RefPersonStatusTypeId = 28
      )
      GROUP BY ASN.AssessmentAdministrationId, ASN.AssessmentSessionId, ASSR.AssessmentSessionStaffRoleId, ASSR.PersonId
      ORDER BY ASSR.PersonId ASC;
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {_scoreQuery} -> {str(e)}")

    try:
        if(len(_scoreQuery) > 0):
            _score = (list([m[0] for m in _scoreQuery if m[0] is not None]))
            _tipo = (list([m[1] for m in _scoreQuery if m[1] is not None]))
            for x in _score:
                if(str(_tipo) in ('1', '2') and x < 4):
                    logger.error(
                        f'Existen alumnos promovidos con calificacion final inferior a 4,0')
                    logger.error(f'Rechazado')
                    _r = False
            logger.info(
                f'Todos los alumnos aprobados cuentan con promedio final sobre 4,0')
            logger.info(f'Aprobado')
            _r = True
        else:
            logger.error(
                f'Los alumnos ingresados como promovidos no cuentan con un registro de calificaciones en el establecimiento')
            logger.error(f'Rechazado')
            _r = False
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        _r = False
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
