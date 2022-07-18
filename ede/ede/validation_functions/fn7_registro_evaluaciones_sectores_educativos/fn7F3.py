from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


  ## Inicio fn7F3 WC ##
def fn7F3(conn, return_dict):
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
      SELECT LA.LearnerActivityId,
          LA.PersonId,
          LA.Weight,
          R.ScoreValue
      FROM LearnerActivity LA
              JOIN AssessmentRegistration AR ON LA.AssessmentRegistrationId = AR.AssessmentRegistrationId
              JOIN AssessmentAdministration AA ON AR.AssessmentAdministrationId = AA.AssessmentAdministrationId
              JOIN Assessment A ON AA.AssessmentId = A.AssessmentId
              JOIN AssessmentResult R ON AR.AssessmentRegistrationId = R.AssessmentRegistrationId
      WHERE A.RefAssessmentTypeId IN (28, 29)
      AND R.RefScoreMetricTypeId IN (1, 2, 3);
      """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {_query} -> {str(e)}")
    
    if( len(_query) == 0 ):
      logger.error(f'S/Datos')
      logger.error(f'No se encuentran evaluaciones registradas en el establecimiento')
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")
      return _r

    try:
      _err = True
      _weight = (list([m[2] for m in _query if m[2] is not None]))
      for x in _weight:
          if (x is None or x > 100 or x <= 0):
              logger.error(f'El peso de la/s calificacion/es esta mal ingresado')
              logger.error(f'Rechazado')
              _err = False

      _scoreValue = (list([m[3] for m in _query if m[3] is not None]))
      for y in _scoreValue:
          if (y is None):
              logger.error(f'Existen Calificaciones mal ingresadas en el establecimiento')
              logger.error(f'Rechazado')
              _err = False
      
      if(_err):
        logger.info(f'Calificaciones con su ponderacion ingresadas correctamente')
        logger.info(f'Aprobado')
        _r = True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
    finally:
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")
      return _r

  ## Fin fn7F3 WC ##
