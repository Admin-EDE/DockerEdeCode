from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn7F0(conn, return_dict):
    """
    REGISTRO DE EVALUACIONES Y SECTORES EDUCATIVOS
    6.2 Contenido mínimo, letra d
    Las evaluaciones de los estudiantes estan todas clasificadas en formativas o sumativas.
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
      SELECT A.AssessmentId,
            ASSR.PersonId,
            A.RefAssessmentTypeId
        FROM AssessmentResult R
              JOIN AssessmentRegistration AR ON AR.AssessmentRegistrationId = R.AssessmentRegistrationId
              JOIN AssessmentAdministration AA ON AR.AssessmentAdministrationId = AA.AssessmentAdministrationId
              JOIN Assessment A ON AA.AssessmentId = A.AssessmentId
              JOIN AssessmentSession ASN ON ASN.AssessmentAdministrationId = AA.AssessmentAdministrationId
              JOIN AssessmentSessionStaffRole ASSR ON ASN.AssessmentSessionId = ASSR.AssessmentSessionId
              JOIN OrganizationPersonRole OPR ON OPR.PersonId = ASSR.PersonId
                WHERE ASSR.RefAssessmentSessionStaffRoleTypeId = 6
                  AND OPR.RoleId = 6
            GROUP BY ASN.AssessmentAdministrationId, ASN.AssessmentSessionId, ASSR.AssessmentSessionStaffRoleId;
      """)
    except Exception as e:
        logger.info(f"Resultado: {_query} -> {str(e)}")

    try:
        if(len(_query) > 0):
            _contador = 0
            _assessment = int(len(_query))
            _assessmentType = (
                list([m[2] for m in _query if m[2] is not None]))
            for x in _assessmentType:
                if (x == 28 or x == 29):
                    _contador += 1
            if _contador == _assessment:
                logger.info(
                    f'Todas las evaluaciones estan ingresadas como sumativas o formativas')
                logger.info(f'Aprobado')
                _r = True
            else:
                logger.error(
                    f'No todas las evaluaciones estan ingresadas como sumativas o formativas')
                logger.error(f'Rechazado')
                _r = False
        else:
            logger.error(f'S/Datos')
            logger.error(
                f'No se encuentran evaluaciones registradas en el establecimiento')
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
