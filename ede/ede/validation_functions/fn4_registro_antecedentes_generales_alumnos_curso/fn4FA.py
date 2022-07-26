from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


def fn4FA(conn, return_dict):
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
    rows = []
    try:
        rows = conn.execute("""--sql
SELECT 
	  est.personId
	, orgCurso.OrganizationId as cursoId
	, orgCurso.name as cursoName
	, (est.FirstName || ' ' || est.MiddleName || ' ' || est.LastName || ' ' || est.SecondLastName)     as "nombre_Estudiante"
	, group_concat(DISTINCT run_ipe_Est.Identifier) as 'run_ipe_estudiante'
	, group_concat(DISTINCT numListaEst.Identifier) as 'numero_Lista'
	, group_concat(DISTINCT numMatriculaEst.Identifier) as 'numero_matricula_estudiante'
	, est.Birthdate as 'fecha_nacimiento_estudiante'
	, CASE 
		WHEN sexo_est.Definition = 'Male' THEN 'M'
		WHEN sexo_est.Definition = 'female' THEN 'F'
		ELSE NULL
	  END as 'sexo_estudiante'
	, pa_est.StreetNumberAndName as 'direccion_estudiante'
	, group_concat(DISTINCT rpst_est.Description) as 'PersonStatus_Estudiante'
	, group_concat(DISTINCT profJefe.name) as 'profesor_jefe_curso'
	, count(DISTINCT asignaturas.Organizationid) as 'asignaturasId'
	, group_concat(DISTINCT asignaturas.name) as 'asignaturas_nombre'
	, count(prof_educ.name) as 'nombre_profesionales_educación'
	, group_concat(DISTINCT prof_educ.RUN) as 'run_profesionales_educación'
-------------- información del ESTUDIANTE -------------------
FROM Person est

JOIN OrganizationPersonRole opr
	on opr.personId = est.personId
	and opr.RoleId IN (SELECT RoleId from Role WHERE role.Name IN ('Estudiante'))

JOIN Organization orgCurso
	ON opr.Organizationid = orgCurso.OrganizationId
	AND orgCurso.RefOrganizationTypeId IN (SELECT RefOrganizationTypeId FROM RefOrganizationType rotcurso WHERE rotcurso.Code IN ('Course'))
	
OUTER LEFT JOIN PersonIdentifier run_ipe_Est 
	on est.PersonId = run_ipe_Est.PersonId
	and run_ipe_Est.RefPersonIdentificationSystemId IN (Select RefPersonIdentificationSystemId from RefPersonIdentificationSystem rpi where rpi.code IN ('RUN','IPE'))

OUTER LEFT JOIN PersonIdentifier numListaEst 
	on est.PersonId = numListaEst.PersonId
	and numListaEst.RefPersonIdentificationSystemId IN (Select RefPersonIdentificationSystemId from RefPersonIdentificationSystem rpi where rpi.code IN ('listNumber'))

OUTER LEFT JOIN PersonIdentifier numMatriculaEst 
	on est.PersonId = numMatriculaEst.PersonId
	and numMatriculaEst.RefPersonIdentificationSystemId IN (Select RefPersonIdentificationSystemId from RefPersonIdentificationSystem rpi where rpi.code IN ('SchoolNumber'))

OUTER LEFT JOIN RefSex sexo_est 
	on est.RefSexId = sexo_est.RefSexId	
	
OUTER LEFT JOIN PersonAddress pa_est
	on est.PersonId = pa_est.PersonId

OUTER LEFT JOIN PersonStatus ps_estudiante
	ON est.personId = ps_estudiante.personId

OUTER LEFT JOIN RefPersonStatusType rpst_est 
	ON ps_estudiante.RefPersonStatusTypeId = rpst_est.RefPersonStatusTypeId
	
-------------- información del profesor jefe  -------------------
OUTER LEFT JOIN (
			SELECT 
				(p.FirstName || ' ' || p.MiddleName || ' ' || p.LastName || ' ' || p.SecondLastName) as name
				,orgCurso.Organizationid as OrganizationId
			FROM Person p
			JOIN OrganizationPersonRole op 
				ON op.PersonId = p.PersonId
				AND op.roleid IN (SELECT RoleId FROM Role WHERE name = 'Profesor(a) Jefe')
			JOIN Organization orgCurso
				ON op.Organizationid = orgCurso.OrganizationId
				AND orgCurso.RefOrganizationTypeId IN ( SELECT RefOrganizationTypeId FROM RefOrganizationType WHERE Code IN ('Course')	)
			) profJefe
			ON orgCurso.Organizationid = profJefe.OrganizationId

-------------- información del asignaturas -------------------
OUTER LEFT JOIN (
	SELECT Parent_OrganizationId, orgAsignatura.OrganizationId, orgAsignatura.name as name
	from OrganizationRelationship orgRelAsig
	JOIN Organization orgAsignatura
		ON orgAsignatura.Organizationid = orgRelAsig.OrganizationId
		AND orgAsignatura.RefOrganizationTypeId IN (SELECT RefOrganizationTypeId FROM RefOrganizationType rotAsig WHERE rotAsig.Code IN ('CourseSection'))

) as asignaturas 
ON asignaturas.Parent_OrganizationId = orgCurso.Organizationid

-------------- profesionales de la educación que interactúan con el estudiante -------------------
OUTER LEFT JOIN (
	SELECT opr.OrganizationId, (p.FirstName || ' ' || p.MiddleName || ' ' || p.LastName || ' ' || p.SecondLastName) as name, pi.Identifier as RUN
	from person p
	JOIN OrganizationPersonRole opr
		ON opr.personId = p.personId
		AND opr.roleid IN (SELECT RoleId FROM Role WHERE name IN ('Docente','Asistente de la Educación','Técnica(o) de párvulo','Paradocente','Tutor(a) práctica profesional','Profesor(a) de reemplazo'))
	JOIN PersonIdentifier pi
		on p.PersonId = pi.PersonId
		and pi.RefPersonIdentificationSystemId IN (Select RefPersonIdentificationSystemId from RefPersonIdentificationSystem rpi where rpi.code IN ('RUN'))
) as prof_educ
ON asignaturas.organizationId = prof_educ.Organizationid

GROUP BY est.personId
                          """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    if(len(rows) == 0):
        logger.info(f"S/Datos")
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    try:
        _err = {}
        for row in rows:
            # print(row)
            errorList = []
            if(row[2] is None):
                errorList.append('EL curso no tiene letra asignada')
            if(row[3] is None):
                errorList.append('Estudiante sin nombre')
            if(row[4] is None):
                errorList.append('estudiante sin RUT o IPE')
            if(row[5] is None):
                errorList.append('estudiante sin número de lista')
            if(row[6] is None):
                errorList.append('estudiante sin número de matrícula')
            if(row[7] is None):
                errorList.append('estudiante sin fecha de nacimiento')
            if(row[8] is None):
                errorList.append('estudiante sin sexo asignado')
            if(row[9] is None):
                errorList.append('estudiante sin dirección')
            if(row[10] is not None
               and ('Estudiante asignado a un curso, se crea número de lista' not in row[10]
               or 'Estudiante con matrícula definitiva' not in row[10])):
                errorList.append(
                    'estudiante sin los estatus minimos asignados')
            if(row[11] is None):
                errorList.append('estudiante sin profesor jefe asignado')
            if(row[12] > row[14]):
                errorList.append(
                    'Los profesionales que trabajan en las asignaturas debería ser >= que las asignaturas registradas')

            if(len(errorList) > 0):
                _err[row[0]] = errorList

        if(len(_err) == 0):
            logger.info("Se validaron todos los datos")
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.error(f"Rechazado")
            logger.error(f"personId con errores: {_err}")

    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
