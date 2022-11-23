from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3FB(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
	La cantidad de #Matricula == #lista == #FechasIncorporaciones.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - verifica que la cantidad de números de matrícula, números de lista y fechas de incorporación sean iguales.
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]          
    """
    _r = False
    rows = []
    try:
        rows = ejecutar_sql(conn, """--sql
SELECT 
(
	SELECT count(p.personId)
	FROM person p
	JOIN PersonIdentifier numLista
		ON p.personid = numLista.personid
		AND numLista.RecordEndDateTime IS NULL
	JOIN RefPersonIdentificationSystem rfiLista 
	  ON  numLista.RefPersonIdentificationSystemId=rfiLista.RefPersonIdentificationSystemId
	  AND rfiLista.code IN ('listNumber')
	WHERE
	p.RecordEndDateTime IS NULL
) as 'cantidadNumeroLista'
,(
	SELECT count(p.personId)
	FROM person p
	JOIN PersonIdentifier numMatri
		ON p.personid = numMatri.personid
		AND numMatri.RecordEndDateTime IS NULL
	JOIN RefPersonIdentificationSystem rfiMatri
	  ON  numMatri.RefPersonIdentificationSystemId=rfiMatri.RefPersonIdentificationSystemId
	  AND rfiMatri.code IN ('SchoolNumber')
	  WHERE
	p.RecordEndDateTime IS NULL
) as 'cantidadNumeroMatricula'
,(
	SELECT count(p.personId)
	FROM person p
	JOIN PersonStatus ps
		ON ps.personId = p.personId
	JOIN RefPersonStatusType rpst
	  ON  rpst.RefPersonStatusTypeId=ps.RefPersonStatusTypeId
	  AND rpst.Description IN ('Estudiante con matrícula definitiva', 'Estudiante promovido', 'Estudiante con matrícula provisoria', 'Estudiante Matriculado a través de Decreto 152, artículo 60')
	  WHERE
	p.RecordEndDateTime IS NULL
) as 'cantidadMatriDefinitiva'
,(
	SELECT count(p.personId)
	FROM person p
	JOIN PersonStatus ps
		ON ps.personId = p.personId
		AND ps.StatusEndDate IS NULL
	JOIN RefPersonStatusType rpst
	  ON  rpst.RefPersonStatusTypeId=ps.RefPersonStatusTypeId
	  AND rpst.Description IN ('Estudiante asignado a un curso, se crea número de lista')
	  WHERE
	p.RecordEndDateTime IS NULL
) as 'cantidadNumerosListaAsignados'
, (
	SELECT group_concat(p.personId)
	FROM person p
	JOIN PersonIdentifier numLista
		ON p.personid = numLista.personid
		AND numLista.RecordEndDateTime IS NULL
	JOIN RefPersonIdentificationSystem rfiLista 
	  ON  numLista.RefPersonIdentificationSystemId=rfiLista.RefPersonIdentificationSystemId
	  AND rfiLista.code IN ('listNumber')
	WHERE
	p.RecordEndDateTime IS NULL
    AND	p.personId NOT IN (
		SELECT p.personId
		FROM person p
		JOIN PersonStatus ps
			ON ps.personId = p.personId
			AND ps.StatusEndDate IS NULL
		JOIN RefPersonStatusType rpst
		  ON  rpst.RefPersonStatusTypeId=ps.RefPersonStatusTypeId
		  AND rpst.Description IN ('Estudiante asignado a un curso, se crea número de lista')	
	)
	
) as 'personIdsNumListWithProblems'
, (
	SELECT group_concat(p.personId)
	FROM person p
	JOIN PersonIdentifier numMatri
		ON p.personid = numMatri.personid
	JOIN RefPersonIdentificationSystem rfiMatri
	  ON  numMatri.RefPersonIdentificationSystemId=rfiMatri.RefPersonIdentificationSystemId
	  AND rfiMatri.code IN ('SchoolNumber')
	WHERE
	p.RecordEndDateTime IS NULL
	AND	p.personId NOT IN (
		SELECT p.personId
	FROM person p
	JOIN PersonStatus ps
		ON ps.personId = p.personId
	JOIN RefPersonStatusType rpst
	  ON  rpst.RefPersonStatusTypeId=ps.RefPersonStatusTypeId
	  AND rpst.Description IN ('Estudiante con matrícula definitiva')
	)	
) as 'personIdsNumMatriculaWithProblems'
      """)
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    try:
        if(len(rows) > 0 and rows[0][0] != 0):
            cantidadNumeroLista = rows[0][0]
            cantidadNumeroMatricula = rows[0][1]
            cantidadMatriDefinitiva = rows[0][2]
            cantidadNumerosListaAsignados = rows[0][3]
            listNumerosListaAsignados = rows[0][4]
            listNumerosMatAsignados = rows[0][5]
            _r = cantidadNumeroLista == cantidadNumeroMatricula == cantidadMatriDefinitiva == cantidadNumerosListaAsignados
            _t1 = f"Verifica: {_r}. PERSON_IDENTIFIER -> NumLista:{cantidadNumeroLista}, NumMat:{cantidadNumeroMatricula}. personIds: {listNumerosListaAsignados}"
            _t2 = f"Verifica: {_r}. PERSON_STATUS     ->  NumLista:{cantidadNumerosListaAsignados}, NumMat:{cantidadMatriDefinitiva}. personids: {listNumerosMatAsignados}"
            logger.info(_t1) if _r else logger.error(_t1)
            logger.info(_t2) if _r else logger.error(_t2)
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
