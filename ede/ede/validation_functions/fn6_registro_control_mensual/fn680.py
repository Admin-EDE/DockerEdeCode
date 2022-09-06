from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


def fn680(conn, return_dict):
    """
    REGISTRO CONTROL MENSUAL DE ASISTENCIA O CONTROL DE SUBVENCIONES
    6.2 Contenido mínimo, letra c.8
    verificar que los registros reportados semanalmente por la empresa 
    se encuentren cargados en el sistema
    --------------------------------------------------
    El medio de verificación de la asistencia debería ser un documento 
    reportado por la empresa, el cual debe estar cargado en el sistema 
    en los campos fileScanBase64 y observaciones.

    [revisar si es necesario cargar el documento o solo dejar la observación]

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
    # arr=[]
    # arr2=[]
    # arr3=[]
    # arr4=[]
    # dias_laborales=[]
    # dias_laborales2=[]
    # dias_laborales3=[]
    # dias_laborales4=[]
    # numero=0
    try:
        _queryText = """--sql
          /*
            6.2 Contenido mínimo, letra c.8 (Alumnos de formación Dual)
            verificar que los registros reportados semanalmente por la empresa se encuentren cargados en el sistema
          */

          SELECT 
            pid.Identifier --idx 0
            ,strftime('%Y-%m-%d', rae.Date) as 'fecha' -- rescata solo la fecha desde rae.Date [idx 1]
            ,strftime('%H:%M', rae.Date, substr(rae.Date,length(rae.Date)-5,6)) as 'hora' -- rescata solo la hora desde rae.Date [idx 2]
            ,CASE 
              WHEN strftime('%w', rae.Date) = '0' THEN 'Domingo'
              WHEN strftime('%w', rae.Date) = '1' THEN 'Lunes'
              WHEN strftime('%w', rae.Date) = '2' THEN 'Martes'
              WHEN strftime('%w', rae.Date) = '3' THEN 'Miércoles'
              WHEN strftime('%w', rae.Date) = '4' THEN 'Jueves'
              WHEN strftime('%w', rae.Date) = '5' THEN 'Viernes'
              WHEN strftime('%w', rae.Date) = '6' THEN 'Sabado'
            END as 'diaSemana' -- rescata solo el dpia de la semana desde rae.Date [idx 3]
            ,rae.fileScanBase64 as 'documentId' --idx 4
            ,rae.observaciones as 'observaciones' --idx 5
          FROM person a 
            JOIN PersonStatus pst
              ON a.personId = pst.personId
              AND pst.RecordEndDateTime IS NULL
              AND pst.RefPersonStatusTypeId IN (
                SELECT RefPersonStatusTypeId
                FROM RefPersonStatusType
                WHERE RefPersonStatusType.Description IN ('Estudiante con formación DUAL')
              )
            JOIN PersonIdentifier pid
              ON a.personId = pid.personId
              AND pid.RecordEndDateTime IS NULL
              AND pid.RefPersonIdentificationSystemId IN (
                SELECT RefPersonIdentificationSystemId
                FROM RefPersonIdentificationSystem
                WHERE RefPersonIdentificationSystem.Code IN ('RUN')
              )		
            JOIN organizationpersonrole opr
              on a.personId = opr.personId 
              AND opr.RecordEndDateTime IS NULL
              AND opr.RoleId IN (
                SELECT RoleId
                FROM Role
                WHERE Role.Name IN ('Estudiante')	
              )			
            JOIN Organization O 
              ON opr.OrganizationId = O.OrganizationId
              AND O.RecordEndDateTime IS NULL
              AND O.RefOrganizationTypeId IN (
                SELECT RefOrganizationTypeId
                FROM RefOrganizationType
                WHERE RefOrganizationType.Description IN ('Asignatura de Practica profesional')				
              )		
            JOIN RoleAttendanceEvent rae
              on opr.OrganizationPersonRoleId = rae.OrganizationPersonRoleId 
              AND rae.RecordEndDateTime IS NULL
            JOIN OrganizationCalendar oc 
              ON O.OrganizationId = oc.OrganizationId
              AND oc.RecordEndDateTime IS NULL
            JOIN OrganizationCalendarSession ocs
              ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId
              AND ocs.RecordEndDateTime IS NULL
            JOIN CourseSectionSchedule css
              ON O.OrganizationId = css.OrganizationId
              AND css.RecordEndDateTime IS NULL		
            JOIN Document doc
              ON rae.fileScanBase64 = doc.documentId
              AND doc.fileScanBase64 IS NOT NULL		
          WHERE 
            -- Verifica que se encuentre cargado el leccionario
            rae.RefAttendanceEventTypeId IN (
              SELECT RefAttendanceEventTypeId
              FROM RefAttendanceEventType
              WHERE RefAttendanceEventType.Code IN ('ClassSectionAttendance')
            )
            AND
            -- Verifica que se encuentre cargado el leccionario
            ocs.Description NOT NULL
            AND
            -- Verifica que el indicador sea True, ya que en estos casos corresponde la relación	
            ocs.AttendanceTermIndicator IN (1)
            AND
            -- Verifica que la firma se encuentre cargada en el sistema
            rae.digitalRandomKey NOT NULL
            AND
            -- Verifica que se haya especificado si es estudiante asiste presencialmente o no.
            rae.VirtualIndicator NOT NULL
            AND
            -- Verifica que día y horario de firma corresponda con calendario de la asignatura
            css.ClassMeetingDays like '%'||diaSemana||'%'
            AND
            hora between css.ClassBeginningTime and css.ClassEndingTime
            AND
            -- Agrega a la lista todos los registros que no cumplan con la expresión regular
            rae.Date REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
            AND
            -- Agrega a la lista todos los registros que no cumplan con la expresión regular
            rae.digitalRandomKeyDate REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
            AND
            -- Agrega a la lista todos los registros que no cumplan con la expresión regular
            rae.digitalRandomKey REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$'
            AND 
            documentId NOT NULL
      """

        # _S1="""select a.personId,strftime('%Y-%m-%d', b.entrydate) as entrydate,strftime('%Y-%m-%d',b.ExitDate) as ExitDate
        # from person a join organizationpersonrole b on a.personId=b.personId where b.roleid=6 """

        # _S6=""" select strftime('%Y-%m-%d',BeginDate) as BeginDate ,strftime('%Y-%m-%d',EndDate) as EndDate
        #        from OrganizationCalendarSession Where organizationcalendarsessionid=1"""

        # _S7=""" select c.identifier,a.fileScanBase64 from RoleAttendanceEvent a join organizationpersonrole b on a.OrganizationPersonRoleId=b.OrganizationPersonRoleId
    # join PersonIdentifier c on b.personid=c.personId where strftime('%Y-%m-%d',a.Date)=? and  a.RefAttendanceEventTypeId=1 and b.personId=?;"""

        # now=datetime.now()
        #_q1 = conn.execute(_S1).fetchall()
        _q1 = conn.execute(_queryText)  # .fetchall()
        if(_q1.returns_rows == 0):
            logger.info(
                f"El establecimientos no tiene alumnos de formación DUAL para revisar")
            logger.info(f"Aprobado")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True

        _q1 = _q1.fetchall()
        XX = 0
        if(len(_q1) != 0):
            # for q1 in _q1:
            #   personid=str(q1[0])
            #   fecha_entrada= str(q1[1])
            #   fecha_fin=str(q1[2])
            #   _q6 = conn.execute(_S6).fetchall()
            #   if(len(_q6)!=0):
            #     for q6 in _q6:
            #       fecha_inicio=str(q6[0])
            #       fecha_termino=str(q6[1])

            #       if fecha_fin<fecha_termino:
            #         fecha_ter_x=fecha_fin
            #       else:
            #         fecha_ter_x=fecha_termino

            #       if (fecha_entrada>fecha_inicio):
            #         arr4=self.ListaFechasRango(fecha_inicio,fecha_ter_x,conn)
            #       else:
            #         arr4=self.ListaFechasRango(fecha_entrada,fecha_ter_x,conn)

            #       for xx2 in arr4:
            #         fecha=str(xx2)
            #         fechaxx1=fecha.replace(',','')
            #         fechaxx2=fechaxx1.replace('(','')
            #         fechaxx3=datetime.strptime(fechaxx2[2:12],'%Y-%m-%d')
            #         _q8 = conn.execute(_S7,fechaxx3,personid).fetchall()
            #         if(len(_q8)!=0):
            #           for dd in _q8:
            #             rut=str(dd[0])
            #             obser=str(dd[1])
            #             if obser=="None":
            #               arr.append(rut)
            logger.info(f"Aprobado")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
