from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn2FA(conn, return_dict):
    """
    REGISTRO DE MATRÍCULA
    5.1 Estructura del registro de matrícula.
    El total de alumnos matriculados menos las bajas, es igual a la suma de los estudiantes inscritos en los libros de clases.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay alumnos matriculados o inscritos
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - La cantidad de alumnos matriculados menos las bajas es igual a los alumnos inscritos
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    results = []
    try:
        results = ejecutar_sql(conn, """--sql
        select count(distinct PersonId)
        from OrganizationPersonRole
        where RoleId=6
        and EntryDate is not null
        and ExitDate is null   ;
        """)
    except Exception as e:
        logger.info(f"Resultado: {results} -> {str(e)}")

    resultsTwo = []
    try:
        resultsTwo = ejecutar_sql(conn, """--sql
        SELECT count(distinct K12StudentEnrollment.OrganizationPersonRoleId)
        from K12StudentEnrollment
        where RefEnrollmentStatusId is not null
        AND FirstEntryDateIntoUSSchool IS NOT NULL;
        """)
    except Exception as e:
        logger.info(f"Resultado: {resultsTwo} -> {str(e)}")

    try:
        if(len(results) > 0 and len(resultsTwo) > 0):
            lista = list(set([m[0] for m in results if m[0] is not None]))
            listaDos = list(
                set([m[0] for m in resultsTwo if m[0] is not None]))
            if lista == listaDos:
                logger.info(
                    f"La cantidad de matriculados corresponder con los alumnos inscritos")
                logger.info(f"Aprobado")
                _r = True
            else:
                logger.error(
                    f'La cantidad de alumnos matriculados no cocincide con los inscritos')
                logger.error(f'Rechazado')
                _r = False
        else:
            logger.info(f"S/Datos")
            _r = True
            logger.info(f'No hay registros de matriculas')
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        _r = False
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
