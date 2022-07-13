from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger

### MIAULA INICIO ###

### INICIO FN0FA ###
def fn0FA(conn, return_dict):
    """
    SalidasNoHabituales: 7.0 Registro de salidas o retiros (NO Habituales)
      Verifica que cada estudiante tenga registrado un listado de personas
    autorizadas para retirarlo.
      Se considera excepción de estudiantes registrados en educación de adultos.
      Se agregó el campo RetirarEstudianteIndicador a la tabla PersonRelationship
      para identificar a las personas autorizadas para retirar estudiantes 
      desde el establecimiento.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y "Aprobado" a través de logger, si:
            - Cada estudiante tiene al menos una persona autorizada para 
          retirarlo del establecimiento
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []    
    try:
      rows = conn.execute("""
SELECT DISTINCT 
	  pid.Identifier -- Muestra el RUN o IPE del estudiante con problemas
	, count(prsh.RetirarEstudianteIndicador) as 'cantidadPersonasAutorizadas'
FROM Person p
	JOIN PersonIdentifier pid
		ON p.personId = pid.PersonId
		AND pid.RecordEndDateTime IS NULL
	JOIN RefPersonIdentificationSystem rpid
		ON pid.RefPersonIdentificationSystemId = rpid.RefPersonIdentificationSystemId
		AND rpid.Code IN ('RUN','IPE')
	JOIN OrganizationPersonRole opr
		ON p.personId = opr.personId
		AND opr.RecordEndDateTime IS NULL
	-- Esto relación filtra por estudiante
	JOIN Role r
		ON r.RoleId = opr.RoleId
		AND r.name IN ('Estudiante')
	-- Esta relación obliga al estudiante a estar asignado a un curso
	JOIN Organization curso
		ON curso.OrganizationId = opr.OrganizationId
		AND curso.RecordEndDateTime IS NULL
		AND curso.RefOrganizationTypeId = (
			SELECT RefOrganizationTypeId
			FROM RefOrganizationType
			WHERE RefOrganizationType.code IN ('Course')
		)
	-- La vista jerarquiasList mantiene la relación entre el curso y el nivel
	JOIN jerarquiasList jer 
		ON curso.OrganizationId = jer.OrganizationIdDelCurso
		AND jer.nivel NOT IN ('03:Educación Básica Adultos'
                      ,'06:Educación Media Humanístico Científica Adultos'
                      ,'08:Educación Media Técnico Profesional y Artística, Adultos')
	--En PersonRelationship el campo personId identifica al apoderado y el campo RelatedPersonId al estudiante
	OUTER LEFT JOIN PersonRelationship prsh 
		ON p.personId = prsh.RelatedPersonId
		AND prsh.RecordEndDateTime IS NULL
		AND prsh.RetirarEstudianteIndicador = 1 --Indica que se encuentra habilitado
GROUP BY pid.Identifier
            """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {rows} -> {str(e)}")
    try:
      c_ = 0
      rutConProblemas = []      
      if( len(rows) > 0 ):
        rutList = check_utils.convertirArray2DToList(list([m[0] for m in rows if m[0] is not None])) 
        cantidadList = check_utils.convertirArray2DToList(list([m[1] for m in rows if m[1] is not None]))        

        for i,cantidad in enumerate(cantidadList):
          if( int(cantidad) > 0 ): 
            c_ += 1
          else: 
            rutConProblemas.append(rutList[i])

        logger.info(f"Total Alumnos                                     : {len(rows)}")
        logger.info(f"Total Personas asociadas y autorizadas para retiro: {c_}")

        if( c_ >= len(rows) ):
          logger.info(f"TODOS los alumnos tienen informacion de personas asociadas y/o autorizadas para retiro.")
          logger.info(f"Aprobado")
          _r = True
        else:
          logger.error(f"Los siguientes estudiantes no tienen personas autorizadas para retirarlos. {rutConProblemas}")
          logger.error(f"Rechazado")
      else:
        logger.info(f"No se encontraron estudiantes y es obligación tenerlos. Se rechaza la función.")
        logger.error(f"Rechazado")
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista personList filtrada por alumnos: {str(e)}")
      logger.error(f"Rechazado")
    finally:
      return_dict[getframeinfo(currentframe()).function] = _r
      return _r
### FIN FN0FA ###