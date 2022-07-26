from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


def fn29A(conn, return_dict):
    """ 
    5.7 De los estudiantes en práctica
    Validar que los estudiantes en práctica hayan terminado al menos el primer semestre del tercer año.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay alumnos en práctica
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Los alumnos en práctica terminaron el primer semestre de tercer año
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = conn.execute("""--sql
        SELECT est.personid
        FROM person est
        JOIN PersonStatus ps
          ON ps.personId = est.personId
          AND ps.RefPersonStatusTypeId IN (SELECT RefPersonStatusTypeId FROM RefPersonStatusType WHERE Description = 'En práctica')
    """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    if(len(rows) == 0):
        logger.info(f"S/Datos")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    results = []
    try:
        results = conn.execute("""--sql
SELECT est.personid
FROM person est
JOIN PersonStatus ps
	ON ps.personId = est.personId
	AND ps.RefPersonStatusTypeId IN (SELECT RefPersonStatusTypeId FROM RefPersonStatusType WHERE Description = 'En práctica')

JOIN (
	SELECT opr.personId, o.OrganizationId
	FROM OrganizationPersonRole opr
	JOIN Organization o
		ON o.OrganizationId = opr.OrganizationId
		AND o.RefOrganizationTypeId IN (SELECT RefOrganizationTypeId FROM RefOrganizationType WHERE Code = 'Course')
) orgCurso	
ON orgCurso.personid = est.personId

JOIN (
SELECT  DISTINCT OrganizationIdDelCurso
FROM jerarquiasList
WHERE grado like '%3º medio%'
) jer 
ON jer.OrganizationIdDelCurso = orgCurso.OrganizationId

OUTER LEFT JOIN (
	SELECT opr.personId
	FROM OrganizationPersonRole opr
	JOIN Organization o
		ON o.OrganizationId = opr.OrganizationId
		AND o.RefOrganizationTypeId IN (SELECT RefOrganizationTypeId FROM RefOrganizationType WHERE Code = 'practicaProfesional')
) orgPractica	
ON orgPractica.personid = est.personId
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {results} -> {str(e)}")

    if(len(results) == 0):
        logger.info(f"Rechazado")
        logger.info(
            f"Alumnos mal asignados en su practica profesional: {rows}")
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    try:
        listaDeEstudiantesEnPractica = list(
            set([m[0] for m in results if m[0] is not None]))
        faltantes = []
        for row in rows:
            personIdEstudiante = row[0]
            if(personIdEstudiante not in listaDeEstudiantesEnPractica):
                faltantes.append(personIdEstudiante)
        if(len(faltantes) == 0):
            logger.info(
                f"todos los alumnos de practica cumplen con los requisitos")
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.info(f"Rechazado")
            logger.info(
                f"Alumnos mal asignados en su practica profesional: {faltantes}")
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
