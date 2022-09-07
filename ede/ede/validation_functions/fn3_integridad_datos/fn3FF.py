from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.validation_functions.check_utils as check_utils
from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3FF(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    Verifica que todos los estudiantes tengan país, región y ciudad de nacimiento
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y “Aprobado” a través de logger, solo si: 
            - #estudiantes == #cuidadNac == #regionNac == #paisNac
            - Verifica que los estudiantes chilenos tengan la información de país, región y ciudad
            - y que los extranjeros tengan la información de su ciudad de origen y país.
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
        logger.info(f"len(estudiantes): {len(rows)}")
        personIdCL = check_utils.convertirArray2DToList(
            list([m[0] for m in rows if (m[0] is not None and m[3] == 'CL')]))
        personIdEX = check_utils.convertirArray2DToList(list([m[0] for m in rows if (
            m[0] is not None and m[3] is not None and m[3] != 'CL' and m[1] is not None)]))
        cuidadNacCl = check_utils.convertirArray2DToList(list(
            [m[1] for m in rows if m[1] is not None and m[0] is not None and m[3] == 'CL']))
        regionNacCL = check_utils.convertirArray2DToList(list(
            [m[2] for m in rows if m[2] is not None and m[0] is not None and m[3] == 'CL']))
        paisNacCL = check_utils.convertirArray2DToList(list(
            [m[3] for m in rows if m[3] is not None and m[0] is not None and m[3] == 'CL']))
        statusCL = check_utils.convertirArray2DToList(list([m[3] for m in rows if (
            m[4] is not None and m[4] >= 2 and m[0] is not None and m[3] == 'CL')]))
        _lCL = [len(personIdCL) == len(cuidadNacCl) == len(
            regionNacCL) == len(paisNacCL) == len(statusCL)]
    except Exception as e:
        logger.info(f"Resultado: {_lCL} y {personIdEX} -> {str(e)}")
    try:
        if(len(_lCL) > 0 or len(personIdEX) > 0):
            _r = True
            studentNumber = len(personIdEX) + len(personIdCL)
            _t = f"Se encontraron {studentNumber} estudiantes con información de Pais, Región y cuidad de nacimiento: {_r}."
            logger.info(_t) if _lCL else logger.error(_t)
            logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
        else:
            logger.info("S/Datos")
    except Exception as e:
        logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
