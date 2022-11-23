from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3FE(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    Los estudiantes tienen sus datos de nacimiento.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Encontrar informacion en la consulta
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = ejecutar_sql(conn, """--sql
SELECT
	  p.personId
	, pbp.ciudadNacimiento
	, pbp.regionNacimiento
	, pbp.paisNacimiento
	, count(rpst.description)
FROM Person p

JOIN (
	SELECT 
		  PersonBirthplace.PersonId, 
		  PersonBirthplace.City as 'ciudadNacimiento'
		, RefState.Code as 'regionNacimiento'
		, RefCountry.Code as 'paisNacimiento'
	FROM PersonBirthplace
	JOIN RefCountry 
		ON RefCountry.RefCountryId = PersonBirthplace.RefCountryId
	OUTER LEFT JOIN RefState 
		ON RefState.RefStateId = PersonBirthplace.RefStateId
	) as pbp 
	ON p.PersonId = pbp.PersonId
JOIN PersonStatus pst
	ON pst.personId = p.personId
	
JOIN RefPersonStatusType rpst
	ON pst.RefPersonStatusTypeId = rpst.RefPersonStatusTypeId
	AND 
	rpst.description IN ('Estudiante con matrícula definitiva','Estudiante asignado a un curso, se crea número de lista')
	AND 
	rpst.description NOT IN ('Estudiante retirado definitivamente')

GROUP BY p.personId
    """)
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")
    try:
        if(len(rows) > 0):
            logger.info(f"len(estudiantes): {len(rows)}")
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.info(f"S/Datos")
            _r = True
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la vista personList filtrada por estudiantes: {str(e)}")
        logger.error(f"Rechazado")
        _r = False
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
