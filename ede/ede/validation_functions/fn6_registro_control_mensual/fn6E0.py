from inspect import getframeinfo, currentframe
from multiprocessing import current_process
from datetime import datetime


from ede.ede._logger import logger


def fn6E0(conn, return_dict):
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
    arr = []
    diaSemana = []
    numero = 0
    try:

        _S3 = """ select organizationid from Organization where reforganizationtypeid=22  ;"""

        _S4 = """ select ClassMeetingDays,strftime('%H:%M',ClassBeginningTime) as ClassBeginningTime,strftime('%H:%M',ClassBeginningTime) as ClassBeginningTime,  
            ClassPeriod from coursesectionschedule where organizationid=?;"""

        _S5 = """ select a.OrganizationPersonRoleId,a.OrganizationId,a.PersonId,a.roleid,strftime('%Y-%m-%d %H:%M',a.EntryDate) as EntryDate, strftime('%Y-%m-%d %H:%M',a.ExitDate) as ExitDate,a.RecordStartDateTime,a.RecordEndDateTime,b.Identifier
                    from  OrganizationPersonRole a 
                    join PersonIdentifier b on a.personId=b.personId  
                    where roleid=6;"""

        _S6 = """ select a.OrganizationPersonRoleId,strftime('%Y-%m-%d',b.Date) as Date,b.fileScanbase64,b.observaciones 
                    from OrganizationPersonRole a join RoleAttendanceEvent b on a.OrganizationPersonRoleId= b.OrganizationPersonRoleId
                    where a.OrganizationPersonRoleId= ? and b.Date= ?;"""

        now = datetime.now()
        _q1 = conn.execute(_S3).fetchall()
        XX = 0
        if(len(_q1) != 0):
            for q1 in _q1:
                organizationid = str(q1)
                _q2 = conn.execute(_S4, organizationid).fetchall()
                if(len(_q2) != 0):
                    for q2 in _q2:
                        diaSemana = str(q2[2]).split(",")
                        hora_comi = str(q2[3])
                        hora_final = str(q2[4])
                        periodo = str(q2[5])
                        cantidad_letras = int(len(periodo))-1
                        periodo2 = (periodo[-2:])
                        _q3 = conn.execute(_S5).fetchall()
                        if(int(periodo2.strip()) == 2):
                            for q3 in _q3:
                                id_alu = str(q3[8])
                                orgaId = str(q3[0])
                                hora1 = str(q3[4])
                                hora2 = str(q3[5])
                                dfs = datetime.strptime(hora1[:10], '%Y-%m-%d')
                                nombresemana2 = dfs.isoweekday()
                                for aa in diaSemana:
                                    if str(aa.lower()) == 'lunes':
                                        numero = 0
                                    elif str(aa.lower()) == 'martes':
                                        numero = 1
                                    elif str(aa.lower()) == 'miércoles':
                                        numero = 2
                                    elif str(aa.lower()) == 'jueves':
                                        numero = 3
                                    elif str(aa.lower()) == 'viernes':
                                        numero = 4
                                    if int(nombresemana2) == int(numero):
                                        if datetime.strptime(hora1[11:len(hora1)], '%H:%M') > datetime.strptime(hora_comi[:5], '%H:%M'):
                                            _q4 = conn.execute(
                                                _S6, orgaId, dfs).fetchall()
                                            if(len(_q4) != 0):
                                                for q4 in _q4:
                                                    justi = str(q4[2])
                                                    obv = str(q4[3])
                                                    if justi == "None" or obv == "None":
                                                        arr.append(id_alu)
                                            else:
                                                arr.append(id_alu)

            if(len(arr) != 0):
                logger.error(
                    f"Los siguientes alumnos llegaron tarde o : {str(arr)} ")
                logger.error(f"Rechazado")
                return_dict[getframeinfo(currentframe()).function] = False
                return False
            else:
                logger.info("Aprobado")
                return_dict[getframeinfo(currentframe()).function] = True
                return True

        else:
            logger.error(f"No hay registro Numero de lista asociados .")
            logger.error(f"Rechazado")
            return_dict[getframeinfo(currentframe()).function] = False
            return False

    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        return False
