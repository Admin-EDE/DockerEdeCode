from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


def fn7F5(conn, return_dict):
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
    _query = []
    try:
        _query = conn.execute("""
        SELECT LA.LearnerActivityId
        FROM Assessment A
                JOIN AssessmentAdministration AA ON A.AssessmentId = AA.AssessmentId
                JOIN AssessmentRegistration AR ON AA.AssessmentAdministrationId = AR.AssessmentAdministrationId
                JOIN LearnerActivity LA ON LA.AssessmentRegistrationId = AR.AssessmentRegistrationId
        ORDER BY LA.LearnerActivityId;
        """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {_query} -> {str(e)}")

    if(len(_query) == 0):
        logger.error(f'No evaluaciones registradas en el establecimiento ')
        logger.error(f'S/Datos')
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    _organizationCalendarSession = []
    try:
        _organizationCalendarSession = conn.execute("""
      SELECT OrganizationCalendarSessionId
      FROM LearnerActivity
      WHERE LearnerActivityId IN (
          SELECT LA.LearnerActivityId
          FROM Assessment A
                  JOIN AssessmentAdministration AA ON A.AssessmentId = AA.AssessmentId
                  JOIN AssessmentRegistration AR ON AA.AssessmentAdministrationId = AR.AssessmentAdministrationId
                  JOIN LearnerActivity LA ON LA.AssessmentRegistrationId = AR.AssessmentRegistrationId
          ORDER BY LA.LearnerActivityId)
        AND OrganizationCalendarSessionId IS NOT NULL
      GROUP BY OrganizationCalendarSessionId;
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {_organizationCalendarSession} -> {str(e)}")

    if(len(_organizationCalendarSession) == 0):
        logger.error(
            f'Las evaluaciones registradas no poseen registro en los calendarios')
        logger.error(f'Rechazado')
        _r = False
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    _calendar = []
    try:
        _calendar = conn.execute("""
          SELECT 'Descripcion' as Descrip
          FROM OrganizationCalendarSession
          WHERE Description IS NOT NULL
            AND Description <> ''
            AND OrganizationCalendarSessionId in (
              SELECT OrganizationCalendarSessionId
              FROM LearnerActivity
              WHERE LearnerActivityId IN (
                  SELECT LA.LearnerActivityId
                  FROM Assessment A
                          JOIN AssessmentAdministration AA ON A.AssessmentId = AA.AssessmentId
                          JOIN AssessmentRegistration AR ON AA.AssessmentAdministrationId = AR.AssessmentAdministrationId
                          JOIN LearnerActivity LA ON LA.AssessmentRegistrationId = AR.AssessmentRegistrationId
                  ORDER BY LA.LearnerActivityId)
                AND OrganizationCalendarSessionId IS NOT NULL
              GROUP BY OrganizationCalendarSessionId)
          """).fetchall()

    except Exception as e:
        logger.info(f"Resultado: {_calendar} -> {str(e)}")

    try:
        if(len(_calendar) == len(_organizationCalendarSession)):
            logger.info(
                f'Todas las evaluaciones registradas en el establecimiento poseen registro de contenidos en los calendarios')
            logger.info(f'Aprobado')
            _r = True
        else:
            logger.error(
                f'No se han ingresado en los calendarios la descripcion del contenido impartido')
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
