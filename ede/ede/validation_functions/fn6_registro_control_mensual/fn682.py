from inspect import getframeinfo, currentframe
from multiprocessing import current_process
from datetime import datetime
import numpy as np

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def ListaFechasRango(fecha_ini, fecha_ter, conn):
    arr = []
    arr2 = []
    arr3 = []
    arr4 = []
    arr5 = []
    arr6 = []
    arr7 = []
    _Trae_fechas = """     WITH RECURSIVE dates(date) AS (
                        VALUES (?)
                        UNION ALL
                        SELECT date(date, '+1 day')
                        FROM dates
                        WHERE date < ?
                        )
                        SELECT strftime('%Y-%m-%d',date) as date  FROM dates;"""

    _S3x = """SELECT strftime('%Y-%m-%d',StartDate) as StartDate , strftime('%Y-%m-%d',EndDate) as EndDate from OrganizationCalendarCrisis;"""

    _S3x2 = """SELECT strftime('%Y-%m-%d',EventDate) as EventDate from OrganizationCalendarevent where strftime('%Y-%m-%d',EventDate)>= ?;"""

    fecha_in = datetime.strptime(fecha_ini, '%Y-%m-%d')
    fecha_te = datetime.strptime(fecha_ter, '%Y-%m-%d')
    logger.info(fecha_in)
    logger.info(fecha_te)
    _q1 = ejecutar_sql(conn, _Trae_fechas, fecha_in, fecha_te)
    if(len(_q1) != 0):
        for q1 in _q1:
            fecha = str(q1)
            fechaxx1 = fecha.replace(',', '')
            fechaxx2 = fechaxx1.replace('(', '')
            fechaxx3 = datetime.strptime(fechaxx2[1:11], '%Y-%m-%d')
            if int(fechaxx3.isoweekday()) != 6:  # sabado
                if int(fechaxx3.isoweekday()) != 7:  # domingo
                    arr.append(str(datetime.strftime(fechaxx3, '%Y-%m-%d')))

        arr3 = np.array(arr)
        arr2.append(np.unique(arr3))

    _q2 = ejecutar_sql(conn, _S3x)
    if(len(_q2) != 0):
        for q2 in _q2:
            fecha_com = datetime.strptime(q2[0], '%Y-%m-%d')
            fecha_fin = datetime.strptime(q2[1], '%Y-%m-%d')
            _q1 = ejecutar_sql(conn, _Trae_fechas, fecha_com, fecha_fin)
            if(len(_q1) != 0):
                for q1 in _q1:
                    fecha = str(q1)
                    fechaxx1 = fecha.replace(',', '')
                    fechaxx2 = fechaxx1.replace('(', '')
                    fechaxx3 = datetime.strptime(fechaxx2[1:11], '%Y-%m-%d')
                    if int(fechaxx3.isoweekday()) != 6:  # sabado
                        if int(fechaxx3.isoweekday()) != 7:  # domingo
                            arr.append(
                                str(datetime.strftime(fechaxx3, '%Y-%m-%d')))

    for ar in arr2:
        dia = datetime.strptime(str(ar[0]), '%Y-%m-%d')
        for ar2 in arr:
            dia2 = datetime.strptime(str(ar2), '%Y-%m-%d')
            if dia != dia2:
                arr4.append(str(datetime.strftime(dia, '%Y-%m-%d')))

    _q3 = ejecutar_sql(conn, _S3x2, fecha_in)
    if(len(_q3) != 0):
        for q3 in _q3:
            fecha = str(q3)
            fechaxx1 = fecha.replace(',', '')
            fechaxx2 = fechaxx1.replace('(', '')
            fechaxx3 = datetime.strptime(fechaxx2[1:11], '%Y-%m-%d')
            dia = fechaxx3
            for ar in arr4:
                fecha = str(ar)
                fechaxx1 = fecha.replace(',', '')
                fechaxx2 = fechaxx1.replace('(', '')
                fechaxx3 = datetime.strptime(fechaxx2[0:11], '%Y-%m-%d')
                if dia != fechaxx3:
                    arr5.append(str(datetime.strftime(dia, '%Y-%m-%d')))

    arr6 = np.array(arr5)
    arr7.append(np.unique(arr6))
    return arr7


def fn682(conn, return_dict):
    """
    REGISTRO CONTROL MENSUAL DE ASISTENCIA O CONTROL DE SUBVENCIONES
    6.2 Contenido mínimo, letra c.8
    Los estudiantes de formación dual se encuentran identificados en el registro de control de asistencia y asignatura.
    -----
    Verificar que la asistencia de práctica profesional se encuentre cargada en 
    roleAttendanceEvent y que en ella, todos los estudiantes tengan cargada su asistencia.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y "S/Datos" a través de logger si no encuentra información
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - A
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    arr = []
    arr4 = []
    try:
        _S1 = """SELECT OrganizationId
                FROM Organization
                WHERE RefOrganizationTypeId = 47;"""

        _S2 = """SELECT Parent_OrganizationId
                FROM OrganizationRelationship
                WHERE OrganizationId = ?;"""

        _S3 = """SELECT OrganizationId
                FROM K12Course
                WHERE OrganizationId = ? and RefWorkbasedLearningOpportunityTypeId=1 ;"""

        _S4 = """SELECT personid from OrganizationPersonRole
        where OrganizationId=? and RoleId = 6 ;"""

        _S5 = """SELECT b.personid,strftime('%Y-%m-%d',c.EntryDate) as EntryDate,strftime('%Y-%m-%d',c.ExitDate) as ExitDate
      from PersonStatus a join personidentifier b on  a.personid = b.personId  join organizationpersonrole c on b.personid=c.personId  where a.RefPersonStatusTypeId=35 and a.personid=? ;"""

        _S6 = """SELECT strftime('%Y-%m-%d',BeginDate) as BeginDate ,strftime('%Y-%m-%d',EndDate) as EndDate 
              from OrganizationCalendarSession Where organizationcalendarsessionid=1;"""

        _S7 = """SELECT * from RoleAttendanceEvent a join organizationpersonrole b on a.OrganizationPersonRoleId=b.OrganizationPersonRoleId 
            where strftime('%Y-%m-%d',a.Date)=? and  a.RefAttendanceEventTypeId=1 and b.personId=? ;"""

        _q1 = ejecutar_sql(conn, _S1)
        if(len(_q1) != 0):
            for q1 in _q1:
                parent = str(q1)
                _q2 = ejecutar_sql(conn, _S2, parent)
                if(len(_q2) != 0):
                    for q2 in _q2:
                        parent2 = str(q2)
                        _q3 = ejecutar_sql(conn, _S3, parent2)
                        if(len(_q3) != 0):
                            for q3 in _q3:
                                parent3 = str(q3)
                                _q4 = ejecutar_sql(conn, _S4, parent3)
                                if(len(_q4) != 0):
                                    for q4 in _q4:
                                        peronid = str(q4)
                                        _q5 = ejecutar_sql(conn, 
                                            _S5, peronid)
                                        if(len(_q5) != 0):
                                            for xx in _q5:
                                                personid2 = str(xx[0])
                                                fecha_entrada = str(xx[1])
                                                fecha_fin = str(xx[2])
                                                _q6 = ejecutar_sql(conn, 
                                                    _S6)
                                                if(len(_q6) != 0):
                                                    for xx in _q6:
                                                        fecha_inicio = str(
                                                            xx[0])
                                                        fecha_termino = str(
                                                            xx[1])
                                                        if fecha_fin < fecha_termino:
                                                            fecha_ter_x = fecha_fin
                                                        else:
                                                            fecha_ter_x = fecha_termino
                                                        if (fecha_entrada > fecha_inicio):
                                                            arr = ListaFechasRango(
                                                                fecha_inicio, fecha_ter_x, conn)
                                                        else:
                                                            arr = ListaFechasRango(
                                                                fecha_entrada, fecha_ter_x, conn)

                                                for xx2 in arr:
                                                    fecha = str(xx2)
                                                    fechaxx1 = fecha.replace(
                                                        ',', '')
                                                    fechaxx2 = fechaxx1.replace(
                                                        '(', '')
                                                    fechaxx3 = datetime.strptime(
                                                        fechaxx2[1:11], '%Y-%m-%d')
                                                    _q8 = ejecutar_sql(conn, 
                                                        _S7, fechaxx3, personid2)
                                                    if(len(_q8) == 0):
                                                        arr4.append(personid2)

                                                if(len(arr4) != 0):
                                                    logger.error(
                                                        f"Los siguientes alumnos no tienen asistencia:{str(arr4)}")
                                                    logger.error(f"Rechazado")
                                                else:
                                                    logger.info(f"Aprobado")
                                                    _r = True
                                else:
                                    logger.error(
                                        f"No tiene alumnos en la asignatura ")
                                    logger.error(f"Rechazado")
                        else:
                            logger.error(
                                f"La asignatura no esta enlazada para que sea de partica profesional")
                            logger.error(f"Rechazado")
        else:
            logger.info(
                f"En el colegio no hay asignaturas de pratica profesional.")
            logger.info(f"S/Datos")
            _r = True
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
