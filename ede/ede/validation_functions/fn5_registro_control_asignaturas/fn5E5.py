from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger

def fn5E5(conn, return_dict):
    """
    REGISTRO DE CONTROL DE ASIGNATURA
      6.2 Contenido mínimo, letra b.2
      Validar que la hora del registro de control de subvenciones corresponda 
      con la segunda hora del registro de control de asignatura.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - La Tabla RoleAttendanceEvent mantiene el registro de las asistencias. 
            Se debe filtrar las asistencias de las organizaciones que son de tipo curso y 
            verificar que existan las firmas de los docentes.
            - Hecho lo anterior, se debe verificar que la asistencia corresponda a la ingresada 
            en la segunda hora y en caso de haber discrepancias informar a través del LOG.
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """      
    _r = False
    return_dict[getframeinfo(currentframe()).function] = _r
    _query = []
    try:
      _query = conn.execute("""
SELECT
  O.OrganizationId as 'Curso',
  strftime('%Y-%m-%d', rae_.Date) as 'fechaAsistencia',
  count(rae_.refattendancestatusid) as 'totalEstudiantesCurso' -- Cantidad total de estudiantes [idx 0]
,
  sum(
    CASE
      WHEN rae_.refattendancestatusid IN (1) THEN 1
      ELSE 0
    END
  ) as 'estudiantesPresentesCurso' -- [idx 1]
,
  sum(
    CASE
      WHEN rae_.refattendancestatusid IN (2, 3) THEN 1
      ELSE 0
    END
  ) as 'estudiantesAusentesv' -- [idx 2]
,
  sum(
    CASE
      WHEN rae_.refattendancestatusid IN (4) THEN 1
      ELSE 0
    END
  ) as 'estudiantesRetrasadosCurso' -- [idx 3]
,
  a.Date -- [idx 4]
,
  a.Parent_OrganizationID -- [idx 5]
,
  a.OrganizationId -- [idx 6]
,
  a.fechaAsistenciaAsignatura -- [idx 7]
,
  a.diaSemana -- [idx 8]
,
  ifnull(a.totalEstudiantes, 0) totalEstudiantes -- [idx 9]
,
  ifnull(a.estudiantesPresentesAsignatura, 0) estudiantesPresentesAsignatura -- [idx 10]
,
  ifnull(a.estudiantesAusentesAsignatura, 0) estudiantesAusentesAsignatura -- [idx 11]
,
  ifnull(a.estudiantesRetrasadosAsignatura, 0) estudiantesRetrasadosAsignatura -- [idx 12]
FROM
  Organization O
  JOIN RefOrganizationType rot ON O.RefOrganizationTypeId = rot.RefOrganizationTypeId
  AND O.RefOrganizationTypeId IN (
    SELECT
      RefOrganizationTypeId
    FROM
      RefOrganizationType
    WHERE
      Description IN ('Course')
  ) OUTER
  LEFT JOIN OrganizationPersonRole opr_ ON O.OrganizationId = opr_.OrganizationId
  JOIN Role rol ON opr_.RoleId = rol.roleId
  AND opr_.roleId in (
    SELECT
      roleId
    from
      role
    where
      Name in ("Estudiante")
  ) OUTER
  LEFT JOIN RoleAttendanceEvent rae_ ON opr_.OrganizationPersonRoleId = rae_.OrganizationPersonRoleId OUTER
  LEFT JOIN (
    SELECT
      rae.Date,
      -- fecha completa de la clase [idx 0]
      ors.Parent_OrganizationID,
      O.OrganizationId,
      strftime('%Y-%m-%d', rae.Date) as 'fechaAsistenciaAsignatura',
      -- rescata solo la fecha desde rae.Date [idx 1]
      CASE
        WHEN strftime('%w', rae.Date) = '0' THEN 'Domingo'
        WHEN strftime('%w', rae.Date) = '1' THEN 'Lunes'
        WHEN strftime('%w', rae.Date) = '2' THEN 'Martes'
        WHEN strftime('%w', rae.Date) = '3' THEN 'Miércoles'
        WHEN strftime('%w', rae.Date) = '4' THEN 'Jueves'
        WHEN strftime('%w', rae.Date) = '5' THEN 'Viernes'
        WHEN strftime('%w', rae.Date) = '6' THEN 'Sabado'
      END as 'diaSemana',
      -- rescata solo el dia de la semana desde rae.Date [idx 3]
      count(refattendancestatusid) as 'totalEstudiantes',
      -- Cantidad total de estudiantes [idx 4]
      sum(
        CASE
          WHEN refattendancestatusid IN (1) THEN 1
          ELSE 0
        END
      ) as 'estudiantesPresentesAsignatura',
      -- [idx 5]
      sum(
        CASE
          WHEN refattendancestatusid IN (2, 3) THEN 1
          ELSE 0
        END
      ) as 'estudiantesAusentesAsignatura',
      -- [idx 6]
      sum(
        CASE
          WHEN refattendancestatusid IN (4) THEN 1
          ELSE 0
        END
      ) as 'estudiantesRetrasadosAsignatura',
      -- [idx 7]
      strftime(
        '%H:%M',
        rae.Date,
        substr(rae.Date, length(rae.Date) -5, 6)
      ) as 'hora' -- rescata solo la hora [idc 8]
      --,*
    FROM
      Organization O
      JOIN RefOrganizationType rot ON O.RefOrganizationTypeId = rot.RefOrganizationTypeId
      AND O.RefOrganizationTypeId IN (
        SELECT
          RefOrganizationTypeId
        FROM
          RefOrganizationType
        WHERE
          Description IN ('Course Section')
      )
      JOIN OrganizationRelationship ors ON O.OrganizationId = ors.OrganizationId
      AND ors.RefOrganizationRelationShipId IN (
        SELECT
          RefOrganizationRelationShipId
        FROM
          RefOrganizationRelationShip
        WHERE
          Code IN ('InternalOrganization')
      )
      JOIN OrganizationPersonRole opr ON O.OrganizationId = opr.OrganizationId
      AND opr.RecordEndDateTime IS NULL
      JOIN RoleAttendanceEvent rae ON opr.OrganizationPersonRoleId = rae.OrganizationPersonRoleId
      AND rae.RecordEndDateTime IS NULL
      JOIN Role rol ON opr.RoleId = rol.RoleId
      AND opr.RoleId IN (
        SELECT
          RoleId
        FROM
          Role
        WHERE
          Name IN ('Estudiante')
      )
      JOIN OrganizationCalendar oc ON O.OrganizationId = oc.OrganizationId
      AND oc.RecordEndDateTime IS NULL
      JOIN CourseSectionSchedule css ON O.OrganizationId = css.OrganizationId
      AND css.RecordEndDateTime IS NULL
      AND hora between time(
        ifnull(css.ClassBeginningTime, '00:00'),
        '-5 minutes'
      )
      and time(ifnull(css.ClassEndingTime, '00:00'), '+5 minutes')
      JOIN OrganizationCalendarSession ocs ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId
      AND ocs.RecordEndDateTime IS NULL
      AND ocs.BeginDate = fechaAsistenciaAsignatura --AND hora between ifnull(ocs.SessionStartTime,'00:00') and ifnull(ocs.SessionEndTime,'00:00')
      AND ocs.SessionStartTime between time(
        ifnull(css.ClassBeginningTime, '00:00'),
        '-5 minutes'
      )
      and time(ifnull(css.ClassEndingTime, '00:00'), '+5 minutes') --GROUP BY rae.Date
    WHERE
      -- Verifica que se encuentre cargado el leccionario
      rae.RefAttendanceEventTypeId = 2
      AND -- Verifica que se encuentre cargado el leccionario
      ocs.Description NOT NULL
      AND -- Verifica que el indicador sea True, ya que en estos casos corresponde la relación	
      ocs.AttendanceTermIndicator IN (1)
      AND -- Verifica que la firma se encuentre cargada en el sistema
      rae.digitalRandomKey NOT NULL
      AND -- Verifica que se haya especificado si es estudiante asiste presencialmente o no.
      rae.VirtualIndicator NOT NULL
      AND -- Verifica que día y horario de firma corresponda con calendario de la asignatura
      css.ClassMeetingDays like '%' || diaSemana || '%'
      AND -- Agrega a la lista todos los registros que no cumplan con la expresión regular
      rae.Date REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
      AND -- Agrega a la lista todos los registros que no cumplan con la expresión regular
      rae.digitalRandomKeyDate REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
      AND -- Agrega a la lista todos los registros que no cumplan con la expresión regular
      rae.digitalRandomKey REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$'
      AND ClassPeriod in ('Bloque02')
    GROUP BY
      rae.Date,
      O.OrganizationId --ORDER BY O.OrganizationId
  ) a ON O.OrganizationId = a.Parent_OrganizationID
WHERE
  -- Verifica que el tipo de asistencia sea diaria
  rae_.RefAttendanceEventTypeId = 1
  AND opr_.RecordEndDateTime IS NULL
  AND rae_.RecordEndDateTime IS NULL
  AND fechaAsistencia = a.fechaAsistenciaAsignatura
GROUP BY
  O.OrganizationId,
  fechaAsistencia
ORDER BY
  a.OrganizationId
    """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {_query} -> {str(e)}")
      logger.error(f"Rechazado")
      logger.info(f"No hay información para evaluar, pero debería por eso no aplica S/Datos")
    try:
      _result = []      
      if( len(_query) > 0 ):
        #print(_query)
        _totalCurso = check_utils.convertirArray2DToList(list([m[2] for m in _query if m[2] is not None]))
        #print(_totalCurso)
        _totalAsign = check_utils.convertirArray2DToList(list([m[11] for m in _query if m[11] is not None]))
        #print(_totalAsign)
        
        _estPresentesCurso = check_utils.convertirArray2DToList(list([m[3] for m in _query if m[3] is not None]))
        #print(_estPresentesCurso)
        _estPresentesAsign = check_utils.convertirArray2DToList(list([m[12] for m in _query if m[12] is not None]))
        #print(_estPresentesAsign)
        _estAtradadosAsign = check_utils.convertirArray2DToList(list([m[14] for m in _query if m[14] is not None]))                
        #print(_estAtradadosAsign)

        _estAusentesCurso = check_utils.convertirArray2DToList(list([m[4] for m in _query if m[4] is not None]))
        #print(_estAusentesCurso)
        _estAusentesAsign = check_utils.convertirArray2DToList(list([m[13] for m in _query if m[13] is not None]))
        #print(_estAusentesAsign)

        for idx_,el_ in enumerate(_totalCurso):
          #logger.info(idx_)
          #logger.info(el_)
          if el_ != _totalAsign[idx_]:
              logger.error(f'Rechazado')
              logger.error(f'Totales de estudiantes no coinciden')
              _result.append(False)
          else:
              _result.append(True)
          
          if _estPresentesCurso[idx_] != (_estPresentesAsign[idx_]+_estAtradadosAsign[idx_]):
              logger.error(f'Rechazado')
              logger.error(f'Total de estudiantes presentes no coinciden')
              _result.append(False)
          else:
              _result.append(True)              

          if _estAusentesCurso[idx_] != _estAusentesAsign[idx_]:
              logger.error(f'Rechazado')
              logger.error(f'Total de estudiantes ausentes no coinciden')
              _result.append(False)
          else:
              _result.append(True)              
      _r = all(_result)
      if(_r and len(_result) > 0):
        logger.info('Aprobado')
      else:
        logger.error('Rechazado')
        _r = False
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        return _r
   ## FIN fn5E5 WC ##