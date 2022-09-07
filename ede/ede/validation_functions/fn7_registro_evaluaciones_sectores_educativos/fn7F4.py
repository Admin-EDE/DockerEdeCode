from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn7F4(conn, return_dict):
    """
    REGISTRO DE EVALUACIONES Y SECTORES EDUCATIVOS
    6.2 Contenido mínimo, letra d
    Verificar que los cambios realizados a las escalas o 
    ponderaciones tengan el verificador de identidad del docente 
    y del jefe técnico-pedagógico.
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
        SELECT LearnerActivityId
        FROM LearnerActivity
        WHERE digitalRandomKey IS NOT NULL
        """)
    except Exception as e:
        logger.info(f"Resultado: {_query} -> {str(e)}")

    if(len(_query) == 0):
        logger.error(f'No existen cambios realizados a las ponderaciones ')
        logger.error(f'S/Datos')
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    _digitalRandom = []
    try:
        _digitalRandom = ejecutar_sql(conn, """--sql
        SELECT digitalRandomKey,
              DateDigitalRandomKey,
              personIDDigitalRandomKey
        FROM LearnerActivity
        WHERE LearnerActivityId IN (SELECT LearnerActivityId
                                    FROM LearnerActivity
                                    WHERE digitalRandomKey IS NOT NULL)
        AND DateDigitalRandomKey IS NOT NULL
        AND personIDDigitalRandomKey IS NOT NULL
        """)
    except Exception as e:
        logger.info(f"Resultado: {_digitalRandom} -> {str(e)}")

    if(len(_digitalRandom) != len(_query)):
        logger.error(
            f'Se han ingresado datos incompletos para las modificaciones de ponderaciones')
        logger.error(f'Rechazado')
        _r = False
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    _digitalRandomKeyPerson = []
    try:
        _digitalRandomKeyPerson = ejecutar_sql(conn, """--sql
      SELECT personIDDigitalRandomKey
      FROM LearnerActivity
      WHERE LearnerActivityId IN (SELECT LearnerActivityId
                                  FROM LearnerActivity
                                  WHERE digitalRandomKey IS NOT NULL)
        AND DateDigitalRandomKey IS NOT NULL
        AND personIDDigitalRandomKey IS NOT NULL
        AND personIDDigitalRandomKey IN (SELECT P.PersonId
                                        FROM OrganizationPersonRole OPR
                                                  JOIN Person P ON OPR.PersonId = P.PersonId
                                        WHERE OPR.RoleId IN (2, 4, 5));
      """)
    except Exception as e:
        logger.info(f"Resultado: {_digitalRandomKeyPerson} -> {str(e)}")

    try:
        if(len(_digitalRandom) == len(_digitalRandomKeyPerson)):
            logger.info(
                f'Las modificaciones a las ponderaciones cuentan con firma del Docente/UTP')
            logger.info(f'Aprobado')
            _r = True
        else:
            logger.error(
                f'Las firmas ingresadas no corresponden a las del Docente/UTP')
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
